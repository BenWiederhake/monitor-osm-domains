from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction
from django.db.models import F
from django.db.models import Max
from monosmdom_server import common
import brotli
import crawl
import datetime
import json
import logging
import pycurl
import storage
import traceback


CRAWL_DOMAIN_DELAY_DAYS = 25
CACHE_CABUNDLE_TIMEOUT = datetime.timedelta(hours=8)

# Options that are basically passed to curl.
# Expect compression to be factor 10 at best, and abort connection after factor 100.
MAX_HEADER = crawl.models.HEADERS_MAX_LENGTH * 10
MAX_HEADER_STOP_COUNT = crawl.models.HEADERS_MAX_LENGTH * 100
MAX_BODY = crawl.models.CONTENT_MAX_LENGTH * 10
MAX_BODY_STOP_COUNT = crawl.models.CONTENT_MAX_LENGTH * 100
# bytes per second; only a guideline, not a limit:
MAX_RECV_SPEED_BPS = 1_048_576
MAX_CONN_TIMEOUT_MS = 10_000
IGNORED_STATUS_CODES = {
    301,  # Moved Permanently
    302,  # Found (in practice: temporary redirect)
    304,  # Not Modified (should never be seen though)
    307,  # Temporary Redirect
    308,  # Permanent Redirect
    400,  # Oh no
    401,  # Unauthorized
    402,  # Payment required
    403,  # Forbidden
    404,  # Not Found
    410,  # Gone
    500,  # Internal Server Error
    502,  # Bad Gateway
}

logger = logging.getLogger(__name__)


def current_crawl_cutoff():
    return common.now_tzaware() - datetime.timedelta(days=CRAWL_DOMAIN_DELAY_DAYS)


def bump_domain(domain):
    # This might also be called while following a redirect-chain.
    domain.last_contacted = common.now_tzaware()
    domain.save()


def bump_locked_domain_or_none(domain):
    # Called by both web UI / commandline ("crawl this URL immediately"), and randomized crawler.
    # NOTE: The locking mechanics here are finnicky.
    # Lockless doesn't really work:
    # - That would require switching postgres to IsolationLevel.SERIALIZABLE (by default it is only "read committed", which is NOT sufficient for this purpose).
    # - Also, it would require some kind of automatic retry mechanism, which, ehhhhh.
    # So instead, we require that the storage.Domain row is locked.
    # I wish this could be asserted here.
    # In manual experiments, postgres waits as expected.
    # Note that contention should be reasonably low anyway, and failure doesn't cost too much.
    # TODO: Check whether this ever fails in production.
    cutoff = current_crawl_cutoff()
    if domain.last_contacted is not None and domain.last_contacted >= cutoff:
        return None
    bump_domain(domain)
    return domain


def pick_random_crawlable_url_from_bumped_domain(bumped_domain):
    # For each CrawlableUrl in the bumped_domain, determine when it was crawled most recently.
    # Then, choose the CrawlableUrl whose most recent crawl is the longest ago, or never happened.
    # Don't be fooled by the seeming simplicity of the query: It contains two joins and a sort!
    return (
        storage.models.CrawlableUrl.objects.filter(domain=bumped_domain)
        .annotate(last_crawl=Max("url__result__crawl_begin"))
        .order_by(F("last_crawl").asc(nulls_first=True))[0]
    )


def pick_and_bump_random_crawlable_url():
    # This is called only from the crawler.
    # We want to make sure that no two crawler processes ever poll the same domain at the same time.
    # (Failure means that we make two requests to the same domain, probably to the same URL)
    # This must be the "top" atomic layer, because we want to make sure that "last_contacted" is persisted.
    with transaction.atomic(durable=True):
        try:
            # First, choose the oldest domain that we want to crawl.
            # Note that this query contains a join, and isn't trivially cheap.
            oldest_domain = (
                storage.models.Domain.objects.filter(crawlableurl__isnull=False)
                .order_by(F("last_contacted").asc(nulls_first=True))
                .select_for_update()[0:1]  # LOCK
                .get()
            )
            # Note: This contains a lot of decisions on how to choose the URL "properly", so let me elaborate:
            # - First, I want to avoid "empty draws", e.g. choosing a random URL and only later
            #   finding out that it's a DisasterURL or that the domain has been contacted too
            #   recently.
            # - Focus on domains, not URLs: Just because a domain has many URLs, that does not
            #   really make it more interesting for our purposes.
            # - Avoid "random", since that causes duplicated work and missed URLs: Keep in mind,
            #   drawing with replacement from a pool of n elements, it takes about n ln n draws
            #   until all elements have been seen at least once. This means if we go round-robin,
            #   we actually *save* a lot of resources. With 422438 domains, that's nearly a factor
            #   of 13, whoa! Let's avoid doing this much work, and causing that much internet traffic.
            # - Once we have picked a Domain, we can bump last_contacted and release the lock,
            #   and then take our sweet time to determine a CrawlableUrl from that Domain.
        except storage.models.Domain.DoesNotExist:
            logger.warning("WARNING: Crawler trying to run on empty DB?!")
            return None
        chosen_domain = bump_locked_domain_or_none(oldest_domain)
    if chosen_domain is None:
        # This can only happen if chosen_domain was very recently crawled,
        # which implies that all other domains are *even more* recent.
        logger.warning("WARNING: Crawler trying to run on very small DB?!")
        logger.warning(f"    Domain with oldest last_contacted={oldest_domain.last_contacted}")
        logger.warning(f"    but cutoff={current_crawl_cutoff()}")
        logger.warning(f"    {common.now_tzaware()=}")
        return None
    # Now that we no longer hold the lock, we can run a more elaborate query:
    return pick_random_crawlable_url_from_bumped_domain(chosen_domain)


def compress_lossy(content_raw, max_length):
    if isinstance(content_raw, bytearray):
        # pybrotli can't handle bytearray :-(
        content_raw = bytes(content_raw)
    candidate_bytes = brotli.compress(content_raw)
    if len(candidate_bytes) <= max_length:
        # We got lucky!
        return candidate_bytes
    # We have to bisect the length.
    safe_len = 0
    unsafe_len = len(content_raw)
    compressed_bytes = b";"  # == brotli.compress(b"")
    # INVARIANT: safe_len < unsafe_len
    # INVARIANT: compressed_bytes decompresses to safe_len many bytes
    # INVARIANT: len(compressed_bytes) <= max_length
    # INVARIANT: brotli.compress(content_raw[:safe_len]) == compressed_bytes
    # Each iteration tries to halve 'unsafe_len - safe_len'.
    while unsafe_len > safe_len + 1:
        # If unsafe_len - safe_len > 2, then rounding doesn't matter.
        # If unsafe_len - safe_len < 2, then we wouldn't be here.
        assert unsafe_len - safe_len >= 2
        # If unsafe_len - safe_len == 2, then they have the same parity, which means the sum is
        # even, so integer division is safe (i.e. does actual progress).
        candidate_len = (safe_len + unsafe_len) // 2
        candidate_bytes = brotli.compress(content_raw[:candidate_len])
        if len(candidate_bytes) <= max_length:
            assert safe_len < candidate_len
            safe_len = candidate_len
            compressed_bytes = candidate_bytes
            # Note that we could stop here, but then we might miss out by nearly factor two.
        else:
            # Note that it is technically possible that a longer prefix compresses to a shorter
            # string. However, that is very rare, and would likely only gain a few bytes.
            assert unsafe_len > candidate_len
            unsafe_len = candidate_len
            # Don't touch compressed_bytes!
    return compressed_bytes


def compress_content_lossy(content_raw, given_size, is_truncated, max_length):
    """
    Takes content bytes (possibly already truncated) and a 'is_truncated' boolean.
    Returns a (compressed_bytes, orig_size) tuple.
    'compressed_bytes' is guaranteed to be at most 'max_length' bytes long, and
    decompresses to a prefix of the content_raw bytes.
    'orig_size' is an int that follows the 'crawl.models.ResultSuccess.{headers,content}_orig_size'
    convention: non-negative when exact, negative when inexact.
    Note in particular that truncation is only signaled implicitly!
    """
    assert given_size >= len(content_raw)
    orig_size = given_size
    if is_truncated:
        assert orig_size > 0
        orig_size = -orig_size
    return (compress_lossy(content_raw, max_length), orig_size)


class CrawlProcess:
    def __init__(self, url_obj):
        self.url_obj = url_obj
        self.has_submitted = False
        self.result = None

    def __enter__(self):
        assert self.result is None
        assert not self.has_submitted
        # We want to make absolutely certain that the connection intent is registered, for possible
        # recovery or crash investigation. To do that, we need to be the outermost atomic:
        with transaction.atomic(durable=True):
            self.result = crawl.models.Result.objects.create(url=self.url_obj, crawl_begin=common.now_tzaware())
        # Now that it has been created, proceed with the network part of crawling.
        return self

    def submit_success(self, status_code, headers_raw, headers_size, headers_truncated, content_raw, content_size, content_truncated, next_url):
        assert self.result is not None
        assert not self.has_submitted
        headers, headers_orig_size = compress_content_lossy(headers_raw, headers_size, headers_truncated, crawl.models.HEADERS_MAX_LENGTH)
        content, content_orig_size = compress_content_lossy(content_raw, content_size, content_truncated, crawl.models.CONTENT_MAX_LENGTH)
        if content_raw == b"" and content_size == 0 and not content_truncated:
            # No need to save empty files. (Server errors, 500s, etc.)
            content_file = None
        elif status_code in IGNORED_STATUS_CODES:
            # No need to save useless files (Redirects, 404s, etc.)
            content_file = None
        else:
            content_file = ContentFile(content, name="<ignored>")
        # "atomic" is just (premature?) optimization, combining the two writes into one:
        with transaction.atomic():
            result_success = crawl.models.ResultSuccess.objects.create(
                result=self.result,
                status_code=status_code,
                headers=headers,
                headers_orig_size=headers_orig_size,
                content_file=content_file,
                content_orig_size=content_orig_size,
                next_url=next_url,
                # no "next_request" yet
            )
            self.result.crawl_end = common.now_tzaware()
            self.result.save()
        # Only now switch off exception-logging:
        self.has_submitted = True
        # Return the ResultSuccess row, the caller might want to set next_request:
        return result_success

    def submit_error(self, curl_errdict):
        assert self.result is not None
        assert not self.has_submitted
        curl_errdict["type"] = "curl_error"
        # "atomic" is just (premature?) optimization, combining the two writes into one:
        with transaction.atomic(durable=True):
            crawl.models.ResultError.objects.create(
                result=self.result,
                is_internal_error=False,
                description_json=json.dumps(curl_errdict),
            )
            self.result.crawl_end = common.now_tzaware()
            self.result.save()
        # Only now switch off exception-logging:
        self.has_submitted = True

    def __exit__(self, type, value, tb):
        assert self.result is not None
        if self.has_submitted:
            return
        self.has_submitted = True  # Are there any edge cases where this is read again?
        description = dict(
            type="exception_or_missing_submit",
            exc_type=repr(type),
            value=repr(value),
            traceback=traceback.format_tb(tb),
        )
        print("".join(traceback.format_tb(tb)))
        # If we're not the outermost atomic, then this insertion would be rolled back instantly.
        # We try to avoid that by using durable=True.
        with transaction.atomic(durable=True):
            crawl.models.ResultError.objects.create(
                result=self.result,
                is_internal_error=True,
                description_json=json.dumps(description),
            )
            self.result.crawl_end = common.now_tzaware()
            self.result.save()
        # Indicate that we did NOT gracefully recover. This will hopefully stop the crawler.
        return None


class LeakyBuf:
    """
    Users are only meant to read the slots ba, size, and truncated.
    """

    def __init__(self, max_save, stop_count):
        self.ba = bytearray()
        self.size = 0
        self.truncated = False
        self._max_save = max_save
        self._stop_count = stop_count
        self._exc = None

    def recv_callback(self, recv_buf):
        try:
            self.size += len(recv_buf)
            if len(self.ba) < self._max_save:
                # Note that this might overshoot, but don't truncate yet:
                self.ba.extend(recv_buf)
            if self.size > self._stop_count:
                # Abort even trying to count the number of bytes in the response.
                # We might be under some kind of attack.
                self.truncated = True
                return -1
            # Returning None implies that all bytes were digested
        except BaseException as e:
            # Must not raise an exception into the C stack. Save it for re-raising:
            self._exc = e
            return -1


class LockedCurlResult:
    def __init__(self):
        self.header = LeakyBuf(MAX_HEADER, MAX_HEADER_STOP_COUNT)
        self.body = LeakyBuf(MAX_BODY, MAX_BODY_STOP_COUNT)
        self.status_code = 0
        self.location = None


class LockedCurl:
    def __init__(self, *, verbose=False):
        self.c = pycurl.Curl()
        if verbose:
            self.c.setopt(pycurl.VERBOSE, True)
            print(f"Curl config:")
            print(f"  {MAX_HEADER=}")
            print(f"  {MAX_HEADER_STOP_COUNT=}")
            print(f"  {MAX_BODY=}")
            print(f"  {MAX_BODY_STOP_COUNT=}")
            print(f"  {settings.CAINFO_ROOT_AND_INTERMEDIATE=}")
            print(f"  {settings.CRAWLER_USERAGENT_EMAIL=}")
        self.last_cabundle_read = common.now_tzaware()
        # Note that this is only about local caching of settings, especially of the heavy CA bundle.
        self.c.setopt(pycurl.CAINFO, settings.CAINFO_ROOT_AND_INTERMEDIATE.encode())
        # Also disable the system store, fail-fast, to detect config problems quicker:
        self.c.setopt(pycurl.CAPATH, None)
        # Connections cannot usually be shared or reused, since we actively *avoid* contacting the
        # same host twice. However, if we follow a redirect, then re-using the same curl object can
        # reuse a TLS session or even the entire connection, hence:
        # Do not use CURLOPT_FORBID_REUSE: Sometimes we get redirects.
        # Do not turn off HTTP Keep-Alive: Sometimes we get redirects.
        # Do not use CURLOPT_HEADER! (mixes header and body, hard to parse)
        # Do not use CURLOPT_MAXFILESIZE(_LARGE)! (not honored in some circumstances)
        # Do not use CURLOPT_RESOLVER_START_FUNCTION: called at the wrong time, CURLOPT_RESOLVER
        # only does a static pre-cache.
        # At the time of writing, the term "SuperTallSoupFleece" has zero hits on Google. So if
        # you're here because you saw SuperTallSoupFleece in your webserver log, maybe it was this
        # program – maybe I was even the person running it! Write me an e-mail and say hi :D
        self.c.setopt(
            pycurl.USERAGENT,
            f"monosmdom-crawler/0.0.1 (contact: {settings.CRAWLER_USERAGENT_EMAIL}) (codename: SuperTallSoupFleece)".encode(),
        )
        self.c.setopt(pycurl.MAX_RECV_SPEED_LARGE, MAX_RECV_SPEED_BPS)
        # Enable all built-ins (which also enables auto-decompression)
        self.c.setopt(pycurl.ACCEPT_ENCODING, "")
        self.c.setopt(pycurl.PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS)
        self.c.setopt(pycurl.TIMEOUT_MS, MAX_CONN_TIMEOUT_MS)
        # TODO: intercept "local" IPs with https://curl.se/libcurl/c/CURLOPT_SOCKOPTFUNCTION.html
        # This will not actually prevent the socket from being connected, but it will abort the
        # connection before any bytes are sent. This prevents any possible damage (which shouldn't
        # be too much anyway, since an attacker could only trigger an arbitrary GET request without
        # controlling cookies or auth info).
        # TODO: Look into CURLOPT_LOW_SPEED_TIME and CURLOPT_LOW_SPEED_LIMIT: How well can I use them?

    def check_cabundle(self):
        now = common.now_tzaware()
        if now - self.last_cabundle_read > CACHE_CABUNDLE_TIMEOUT:
            print("Re-reading CA bundle ...")
            # I sincerely hope this triggers a re-read of the ca bundle file.
            # Or perhaps it re-reads the file on every single request?
            # TODO: Figure out *when and if exactly* curl reads the cabundle ("cainfo") file.
            self.c.setopt(pycurl.CAINFO, settings.CAINFO_ROOT_AND_INTERMEDIATE.encode())
            self.last_cabundle_read = now

    def __del__(self):
        self.c.close()

    def crawl_response_or_errdict(self, url):
        """
        Returns a (result, errdict) tuple, exactly one of these is None.
        `url` is a URL string.
        `referer` is either None, or a string with a referer. Example:
            "https://www.openstreetmap.org/node/221301860#key=website"
        `result` is either None or an instance of LockedCurlResult, which indicates that HTTP
            request and response were successfully exchanged. Perhaps a HTTP 500 response, perhaps
            the body or even the header was truncated, but successful nonetheless. If the header
            was truncated, then no body was saved at all.
        `errdict` is either None or a dict, which indicates that a fatal error was encountered, like
            domain name resolution failure or a timeout. The dict has exactly the following keys:
            - "errcode": The value is an int, specifically a pycurl.E_* error code.
            - "errstr": The value is a str, specifically the corresponding error string to the error
                code. This *might* contain slightly more information, but usually not much.
            - "response_code": The value is an int. It is either 0, or if it could be determined,
                the HTTP status code.
            - "header_size_recv": The value is an int, the number of bytes read before the fatal error.
            - "body_size_recv": The value is an int, the number of bytes read before the fatal error.
        """
        result = LockedCurlResult()
        self.c.setopt(pycurl.HEADERFUNCTION, result.header.recv_callback)
        self.c.setopt(pycurl.WRITEFUNCTION, result.body.recv_callback)
        self.c.setopt(pycurl.URL, url.encode())
        # Can't use referers, since unsetopt(pycurl.REFERER) refuses to work :(
        try:
            # Note: perform() calls into the C stack, which calls into the python stack in
            # SingleBuf.recv_callback. Python exceptions during recv_callback are instead saved in
            # SingleBuf.exc, and are then re-raised during E_WRITE_ERROR handling below.
            self.c.perform()
        except pycurl.error as e:
            code = e.args[0]
            errstr = e.args[1]
            assert len(e.args) == 2, e.args
            if code == pycurl.E_WRITE_ERROR:
                # We aborted receiving data in the body, but everything before then was successful.
                # We can safely ignore the error – unless it is due to a python exception.
                # This should be handled outside this "except" block to simplify backtraces.
                pass
            else:
                errdict = dict(
                    errcode=code,
                    errstr=errstr,
                    # curl internally overwrites the response code with 0 before doing anything in
                    # perform(), so this is never outdated:
                    response_code=self.c.getinfo(pycurl.RESPONSE_CODE),
                    header_size_recv=result.header.size,
                    body_size_recv=result.body.size,
                )
                return None, errdict
        if result.header._exc is not None:
            raise result.header._exc
        if result.body._exc is not None:
            raise result.body._exc
        result.status_code = self.c.getinfo(pycurl.RESPONSE_CODE)
        result.location = self.c.getinfo(pycurl.REDIRECT_URL)
        return result, None
