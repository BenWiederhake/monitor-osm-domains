from django.db import transaction
from django.db.models import F
from django.db.models import Max
from monosmdom_server import common
import crawl
import datetime
import logging
import storage


CRAWL_DOMAIN_DELAY_DAYS = 60

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


def pick_and_bump_random_crawlable_url():
    # This is called only from the crawler.
    # We want to make sure that no two crawler processes ever poll the same domain at the same time.
    # (Failure means that we make two requests to the same domain, potentially to the same URL)
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
    # For each CrawlableUrl in the chosen_domain, determine when it was crawled most recently.
    # Then, choose the CrawlableUrl whose most recent crawl is the longest ago, or never happened.
    # Don't be fooled by the seeming simplicity of the query: It contains two joins and a sort!
    oldest_crawlable_url = (
        storage.models.CrawlableUrl.objects.filter(domain=oldest_domain)
        .annotate(last_crawl=Max("url__result__crawl_begin"))
        .order_by(F("last_crawl").asc(nulls_first=True))[0]
    )
    return oldest_crawlable_url


class CrawlProcess:
    def __init__(self, crawlable_url):
        self.crawlable_url = crawlable_url
        self.has_submitted = False
        self.result_success = None
        # We want to make absolutely certain that the connection intent is registered, for possible
        # recovery or crash investigation. To do that, we need to be the outermost atomic:
        with transaction.atomic(durable=True):
            self.result = models.Result.objects.create(url=crawlable_url.url, crawl_begin=common.now_tzaware())

    def __enter__(self):
        return self

    def submit(self, status_code, headers_raw, headers_truncated, content_raw, content_truncated, next_url):
        assert not self.has_submitted
        self.has_submitted = True
        raise NotImplementedError()
        headers_zlib = None
        with transaction.atomic():
            self.result_success = models.ResultSuccess.objects.create(
                result=self.result,
                status_code=status_code,
            )
            self.result.crawl_end = common.now_tzaware()
            self.result.save()
    # status_code = models.PositiveSmallIntegerField()
    # headers_zlib = models.BinaryField(null=True)
    # headers_orig_size = models.PositiveIntegerField()
    # # File is uncompressed, as the filesystem quantizes to the next block size.
    # content_file = models.FileField(upload_to=user_directory_path, null=True, db_index=True)
    # content_orig_size = models.PositiveIntegerField()
    # # Note: Redirect-chain depth is implicit.
    # # Don't point to CrawlableUrl! We don't necessarily want to automatically crawl that URL in the future.
    # # Note: If next_url is non-None but next_request is None, this can have many reasons, including: Invalid URL, ignored domain, redirect limit reached.
    # next_url = models.ForeignKey(storage.models.Url, on_delete=models.SET_NULL, null=True)
    # next_request = models.ForeignKey(Result, on_delete=models.SET_NULL, null=True, related_name="redir_set")

    def __exit__(self, type, value, traceback):
        raise NotImplementedError()
