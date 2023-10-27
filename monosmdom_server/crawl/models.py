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

# Small example:
# - uncompressed: 194 bytes
# - lz4: 209 bytes
# - lzma: 232 bytes
# - bz2: 198 bytes
# - zlib: 168 bytes (!!!)
# Medium example:
# - uncompressed: 386 bytes
# - lz4: 360 bytes
# - zlib: 274 bytes
# Large example:
# - uncompressed: 619 bytes
# - lz4: 594 bytes
# - lzma: 552 bytes
# - bz2: 545 bytes
# - zlib: 472 bytes (!!!)


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
    headers_zlib = models.BinaryField(null=True)
    headers_orig_size = models.PositiveIntegerField()
    # File is uncompressed, as the filesystem quantizes to the next block size.
    content_file = models.FileField(upload_to=user_directory_path, null=True, db_index=True)
    content_orig_size = models.PositiveIntegerField()
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
