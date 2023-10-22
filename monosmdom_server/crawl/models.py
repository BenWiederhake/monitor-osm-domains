from django.db import models
import storage
import uuid

# Questions that the models shall answer:
# - Show me the most recent crawling results.
# - What are all crawling results for this specific URL?
# - What about the redirect chain?
# - What can be purged?

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


HEADERS_MAX_LENGTH = 256
CONTENT_MAX_LENGTH = 2048


def user_directory_path(instance, filename):
    # file will be uploaded to MEDIA_ROOT/user_<id>/<filename>
    hexstring = uuid.uuid4().hex
    return f"crawlresults_maybetrunc/{hexstring[:3]}/{hexstring[3:]}.dat.lz4"


class Result(models.Model):
    # Don't point to CrawlableUrl! This allows us to crawl a URL and later decide that it is a disaster URL, without any data loss.
    url = models.ForeignKey(storage.models.Url, on_delete=models.RESTRICT)
    crawl_begin = models.DateTimeField(db_index=True)
    crawl_end = models.DateTimeField(db_index=True)

    def __str__(self):
        return f"<Result#{self.id} {self.url.truncated}>"


# "Success" simply means that the server responded with *something* that could be interpreted as a valid HTTP response.
# So 200 is successful, 301 is successful, 404 is successful, 500 is successful.
# "Error" means that the server couldn't be reached, or didn't respond with something we could interpret.


class ResultSuccess(Result):
    # Note: Inheritance!
    status_code = models.PositiveSmallIntegerField()
    headers_lz4 = models.BinaryField(null=True)
    headers_orig_size = models.PositiveIntegerField()
    content_lz4 = models.FileField(upload_to=user_directory_path, null=True)
    content_truncated = models.PositiveIntegerField()
    # Note: Redirect-chain depth is implicit.
    # Don't point to CrawlableUrl! We don't necessarily want to automatically crawl that URL in the future.
    next_url = models.ForeignKey(storage.models.Url, on_delete=models.SET_NULL, null=True)
    next_request = models.ForeignKey(Result, on_delete=models.SET_NULL, null=True, related_name="redir_set")

    def __str__(self):
        return f"<ResultSuccess#{self.result_ptr_id} {self.url.truncated}>"


class ResultError(Result):
    # Note: Inheritance!
    # TODO: Unclear format of the error description, since there are myriad ways to fail.
    description_json = models.TextField()

    def __str__(self):
        return f"<ResultError#{self.result_ptr_id} {self.url.truncated}>"
