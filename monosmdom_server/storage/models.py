from django.db import models
import uuid

# Questions that the models shall answer:
# - Give me a random URL with at least one occurrence whose domain hasn't been contacted for at least 5 minutes and isn't ignored.
# - Show me the most recent crawling results.
# - Show me all ignored domains.
# - For this domain, what are the associated URLs?
# - What are all crawling results for this specific URL?
# - What are all occurrences of this specific URL?
# - Show me all disastrous URLs and their reasons.
# - Show me the domains with the most URLs.

# So we need the following concepts:
# - Domain (deduced from hostname and publicsuffixlist)
# - URL
# - CrawlResult
# - Occurrence

# Stats for better intuition:
# - 422438 actual domains (22.6 chars on average)
# - maybe 400k? registrable domains after aggregating with publicsuffixlist
# - 574090 unique tag-values
# - 572470 unique URLs that should be crawled
# - maybe 500k? unique URLs that we actually want to crawl (without ignored domains)
# And that's just for Germany, at the time of writing.


HEADERS_MAX_LZ4_LENGTH = 4096


def user_directory_path(instance, filename):
    # file will be uploaded to MEDIA_ROOT/user_<id>/<filename>
    hexstring = uuid.uuid4().hex
    return f"crawlresults_maybetrunc/{hexstring[:3]}/{hexstring[3:]}.dat.lz4"


class Domain(models.Model):
    # Domain names can be insanely long. OSM Germany contains a working domain name with 76 characters!
    domain_name = models.CharField(max_length=200, db_index=True)
    last_contacted = models.DateTimeField(db_index=True, default=None, null=True)


class Url(models.Model):
    # URLs can be insanely long, but there also seems to be a limit of 255 characters. Use that!
    # We overshoot by a bit, just for extra safety margin in case of UTF-8 counting weirdness.
    url = models.CharField(max_length=300, unique=True)
    # Exactly one of the following must be true for each "Url":
    # - The "Url" has one-or-more "DisasterUrl"s associated with it, and zero "CrawlableUrl"s.
    #   This happens when a URL occurs in the OSM dataset, but "obviously" cannot work.
    # - The "Url" has zero "DisasterUrl"s associated with it, and exactly one "CrawlableUrl"s.
    #   This happens when a URL occurs in the OSM dataset, and might work.
    # - The "Url" has zero "DisasterUrl"s associated with it, and also zero "CrawlableUrl"s.
    #   This happens when a URL is outdated, or was discovered as part of a redirection chain.
    # Note that this is NOT a case of model inheritance.


class DisasterUrl(models.Model):
    url = models.ForeignKey(Url, on_delete=models.RESTRICT)
    reason = models.CharField()


class CrawlableUrl(models.Model):
    # Despite the name, a Url may be associated with zero or one CrawlableUrl:
    url = models.OneToOneField(Url, on_delete=models.RESTRICT, primary_key=True)
    domain = models.ForeignKey(Domain, on_delete=models.RESTRICT)


# TODO: Move to crawler app.
class CrawlResult(models.Model):
    # Don't point to CrawlableUrl! This allows us to crawl a URL and later decide that it is a disaster URL, without any data loss.
    url = models.ForeignKey(Url, on_delete=models.RESTRICT)
    crawl_begin = models.DateTimeField(db_index=True)
    crawl_end = models.DateTimeField(db_index=True)


# "Success" simply means that the server responded with *something* that could be interpreted as a valid HTTP response.
# So 200 is successful, 301 is successful, 404 is successful, 500 is successful.
# "Error" means that the server couldn't be reached, or didn't respond with something we could interpret.


class CrawlResultSuccess(CrawlResult):
    # Note: Inheritance!
    status_code = models.PositiveSmallIntegerField()
    headers_lz4 = models.BinaryField(max_length=HEADERS_MAX_LZ4_LENGTH, null=True)
    content_lz4 = models.FileField(upload_to=user_directory_path, null=True)
    # TODO: Also record redirect-chain depth?
    # Don't point to CrawlableUrl! Don't want to intentionally crawl that URL.
    next_url = models.ForeignKey(Url, on_delete=models.SET_NULL, null=True)


class CrawlResultError(CrawlResult):
    # Note: Inheritance!
    # TODO: Unclear format of the error description, since there are myriad ways to fail.
    description_json = models.TextField()


class OccurrenceInOsm(models.Model):
    url = models.ForeignKey(Url, on_delete=models.RESTRICT)
    osm_item_type = models.CharField(max_length=1, blank=False)
    osm_item_id = models.BigIntegerField()
    osm_tag_key = models.CharField(max_length=100)
    osm_tag_value = models.CharField(max_length=300)
    # Note that duplicates are meaningful (e.g. "website=https://foo.com;https://foo.com"), although not very informative.
    # These will be completely wiped and re-written on every import anyway.
