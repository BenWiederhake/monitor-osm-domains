from django.db import models
import storage
import secrets
import base64
import uuid

# Questions that the models shall answer:
# - Show me the most recent crawling results.
# - What are all crawling results for this specific URL?
# - What about the redirect chain?
# - What can be purged?
# - Was this result truncated while receiving?
# - What is the size, at minimum?
# - Is this purged?
# - What are the bytes, or at least *some* bytes?

# So we need the following concepts:
# - Result
# - content storage

# Stats for better intuition:
# - 535k unique URLs that we actually want to crawl
# - 397k registered domains that we actually want to crawl
# - Ideally we visit each registered domain at least once a year
# - That's one registered domain every 80 seconds on average.
# - Assuming 2 KB per crawl, that's 775 MiB per year.
# And that's just for Germany, at the time of writing.
# That means I'll have to actively take care of interpreting the crawled
# webpages and purging them, if I ever want a higher time resolution.


# >>> {k: len(v) for k, v in examples.items()}
# {'sedopark': 619, 'sedopark_nocookie': 386, 'gunicorn': 523, 'nextcloud': 1412, 'tiny_gunicorn': 258, 'tiny_tl': 194}
# >>> {k: len(lz4.frame.compress(v)) for k, v in examples.items()}
# {'sedopark': 598, 'sedopark_nocookie': 364, 'gunicorn': 447, 'nextcloud': 1122, 'tiny_gunicorn': 242, 'tiny_tl': 211}  # Strictly worse than zlib
# >>> {k: len(bz2.compress(v)) for k, v in examples.items()}
# {'sedopark': 545, 'sedopark_nocookie': 323, 'gunicorn': 398, 'nextcloud': 983, 'tiny_gunicorn': 227, 'tiny_tl': 203}  # Strictly worse than zlib
# >>> {k: len(lzma.compress(v)) for k, v in examples.items()}
# {'sedopark': 552, 'sedopark_nocookie': 348, 'gunicorn': 412, 'nextcloud': 920, 'tiny_gunicorn': 252, 'tiny_tl': 236}  # Strictly worse than zlib
# >>> {k: len(zstd.compress(v)) for k, v in examples.items()}
# {'sedopark': 485, 'sedopark_nocookie': 283, 'gunicorn': 350, 'nextcloud': 858, 'tiny_gunicorn': 184, 'tiny_tl': 174}  # Strictly worse than zlib
# >>> {k: len(gzip.compress(v)) for k, v in examples.items()}
# {'sedopark': 484, 'sedopark_nocookie': 286, 'gunicorn': 350, 'nextcloud': 841, 'tiny_gunicorn': 195, 'tiny_tl': 180}  # Strictly worse than zlib
# >>> {k: len(zlib.compress(v)) for k, v in examples.items()}
# {'sedopark': 472, 'sedopark_nocookie': 274, 'gunicorn': 338, 'nextcloud': 829, 'tiny_gunicorn': 183, 'tiny_tl': 168}  # Strictly worse than brotli
# >>> {k: len(brotli.compress(v)) for k, v in examples.items()}
# {'sedopark': 412, 'sedopark_nocookie': 216, 'gunicorn': 274, 'nextcloud': 734, 'tiny_gunicorn': 144, 'tiny_tl': 154}
# So let's use brotli for this particular usecase.


HEADERS_MAX_LENGTH = 1024
CONTENT_MAX_LENGTH = 4096


USER_DIRECTORY_PATH_REGEX = "res/[2-7A-Z]{2}/[2-7A-Z]{16}\.br"


def user_directory_path(_instance, _filename):
    # File will be uploaded to MEDIA_ROOT/<filename_returned_by_this_function>
    # Note that "filename_returned_by_this_function" will be stored verbatim in the DB, so make an
    # attempt to keep it reasonably small.
    # Q: But won't the sheer size of the *INLINE* header data negate all that?
    # A: Kinda, but this way they can be purge separately. Dunno.
    # Beyond 10 GiB this would become unwieldy. With a block size of 4K this means we have at most
    # 2_621_440 files. This means we should have more than 42.6 bits of entropy. That's much less
    # than a UUID (124-126 bits of entropy)!
    # Choose a two-level structure: We have a top-level dir with d sub-dirs, and those contain the
    # files. If we distribute the files uniformly randomly, they will most likely not be perfectly
    # evenly distributed, see the balls-into-bins problem:
    #   https://en.wikipedia.org/wiki/Balls_into_bins_problem#Random_allocation
    # Adapting the Wikipedia equation, we have to expect that the largest sub-dir ends up with:
    #   whpmax = n / d + sqrt(n * ln(d) / d)
    # number of files. For some reasonable values of d:
    #   d = 256 --> whpmax = 10479 (bad!)
    #   d = 1024 --> whpmax = 2694
    #   d = 4096 --> whpmax = 713
    # Both d=1024 and d=4096 are perfectly fine. I did all these calculations because I expected
    # the whpmax to be *much* worse. Oh well, "again what learned".
    # How should the entropy be encoded?
    # - base64: Relies on "A" and "a" being different files. This may be fine, but let's avoid it.
    # - base32: 16 chars hold 10 bytes (80 bits) of entropy. d=1024.
    # - hex (base16): 16 chars hold 8 bytes (64 bits) of entropy. d=4096.
    # Choose base32 since n is likely to be much smaller, and I want to avoid having to deal with
    # 4096 folders.
    basename = base64.b32encode(secrets.token_bytes(10)).decode()
    assert len(basename) == 16
    return f"res/{basename[:2]}/{basename}.br"


class Result(models.Model):
    # Don't point to CrawlableUrl! This allows us to crawl a URL and later decide that it is a disaster URL, without any data loss.
    url = models.ForeignKey(storage.models.Url, on_delete=models.RESTRICT)
    crawl_begin = models.DateTimeField(db_index=True)
    crawl_end = models.DateTimeField(db_index=True, null=True)
    # CONSTRAINT, unmappable: There must not be a ResultSuccess and a ResultError row pointing at the same Result row.
    # CONSTRAINT, unmappable: If crawl_end is non-Null, then a ResultSuccess/ResultError must exist for this Result.

    def __str__(self):
        return f"<Result#{self.id} {self.url.truncated}>"


# "Success" simply means that the server responded with *something* that could be interpreted as a valid HTTP response.
# So 200 is successful, 301 is successful, 404 is successful, 500 is successful.
# "Error" means that the server couldn't be reached, or didn't respond with something we could interpret.


class ResultSuccess(models.Model):
    result = models.OneToOneField(Result, on_delete=models.CASCADE, primary_key=True)
    status_code = models.PositiveSmallIntegerField()
    headers = models.BinaryField(null=True)  # brotli compressed
    headers_orig_size = models.IntegerField()  # The negative value "-x" means "Dunno, gave up after reading x bytes"
    content_file = models.FileField(upload_to=user_directory_path, null=True, db_index=True)  # Also brotli compressed
    content_orig_size = models.IntegerField()  # The negative value "-x" means "Dunno, gave up after reading x bytes"
    # Note: Redirect-chain depth is implicit.
    # Don't point to CrawlableUrl! We don't necessarily want to automatically crawl that URL in the future.
    # Note: If next_url is non-None but next_request is None, this can have many reasons, including: Invalid URL, ignored domain, redirect limit reached.
    next_url = models.ForeignKey(storage.models.Url, on_delete=models.SET_NULL, null=True)
    next_request = models.ForeignKey(Result, on_delete=models.SET_NULL, null=True, related_name="redir_set")
    # TODO: Add constraint: (next_url is None) implies (next_request is None)

    def __str__(self):
        return f"<ResultSuccess#{self.result_id} {self.status_code} {self.result.url.truncated}>"


class ResultError(models.Model):
    result = models.OneToOneField(Result, on_delete=models.CASCADE, primary_key=True)
    # is_internal_error == True implies that description_json is a dict with keys:
    #  - "type": value is always "exception_or_missing_submit"
    #  - "exc_type": value is the result of repr(type)
    #  - "value": value is probably the result of repr(value), but that's subject to change
    # is_internal_error == False implies that description_json is a dict with keys:
    #  - "type": value is always "curl_errdict"
    #  - for other keys, see LockedCurl.crawl_response_or_errdict()
    is_internal_error = models.BooleanField()
    description_json = models.TextField()

    def __str__(self):
        return f"<ResultError#{self.result_id} {self.result.url.truncated}>"


class SquatProof(models.Model):
    evidence = models.OneToOneField(ResultSuccess, on_delete=models.RESTRICT)
    squatter = models.TextField()

    def __str__(self):
        return f"<SquatProof#{self.id} {self.evidence.result.url.truncated}>"
