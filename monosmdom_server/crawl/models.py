from django.db import models
import storage
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
HEADERS_MAX_DETECT_LENGTH = 100 * HEADERS_MAX_LENGTH  # Give up trying to read gigantic headers
CONTENT_MAX_LENGTH = 4096
CONTENT_MAX_DETECT_LENGTH = 10 * CONTENT_MAX_LENGTH  # Enable crude analysis of content sizes


USER_DIRECTORY_PATH_REGEX = "crawlresults_maybetrunc/[0-9a-f]{3}/[0-9a-f]{32}\.dat"


def user_directory_path(instance, filename):
    # file will be uploaded to MEDIA_ROOT/user_<id>/<filename>
    hexstring = uuid.uuid4().hex
    return f"crawlresults_maybetrunc/{hexstring[:3]}/{hexstring}.dat"


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
    headers_zlib = models.BinaryField(null=True)  # Uses brotli compression, despite its name
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
    # TODO: Unclear format of the error description, since there are myriad ways to fail.
    description_json = models.TextField()

    def __str__(self):
        return f"<ResultError#{self.result_id} {self.result.url.truncated}>"
