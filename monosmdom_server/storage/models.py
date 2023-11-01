from django.db import models

# Questions that the models shall answer:
# - Give me a random URL with at least one occurrence whose domain hasn't been contacted for at least 5 minutes and isn't ignored.
# - Show me all ignored domains. (Not really supported)
# - For this domain, what are the associated URLs?
# - What are all occurrences of this specific URL?
# - Show me all disastrous URLs and their reasons.
# - Show me the domains with the most URLs.

# So we need the following concepts:
# - Domain (deduced from hostname and publicsuffixlist)
# - URL
# - Occurrence

# Stats for better intuition:
# - 422438 actual domains (22.6 chars on average)
# - maybe 400k? registrable domains after aggregating with publicsuffixlist
# - 574090 unique tag-values
# - 572470 unique URLs that should be crawled
# - maybe 500k? unique URLs that we actually want to crawl (without ignored domains)
# And that's just for Germany, at the time of writing.


URL_TRUNCATION_LENGTH = 50


class Domain(models.Model):
    # Domain names can be insanely long. OSM Germany contains a working domain name with 76 characters!
    domain_name = models.CharField(max_length=200, db_index=True)
    last_contacted = models.DateTimeField(db_index=True, default=None, null=True)

    def __str__(self):
        return f"<Domain#{self.id} {self.domain_name}>"


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

    def __str__(self):
        return f"<Url#{self.id} {self.truncated}>"

    @property
    def truncated(self):
        use_url = self.url
        if len(use_url) > URL_TRUNCATION_LENGTH:
            use_url = use_url[:URL_TRUNCATION_LENGTH - 1] + "…"
        return use_url


class DisasterUrl(models.Model):
    url = models.ForeignKey(Url, on_delete=models.RESTRICT)
    reason = models.CharField()

    def __str__(self):
        return f"<DisasterUrl#{self.id} {self.url.truncated}>"


class CrawlableUrl(models.Model):
    # Despite the name, a Url may be associated with zero or one CrawlableUrl:
    url = models.OneToOneField(Url, on_delete=models.RESTRICT, primary_key=True)
    domain = models.ForeignKey(Domain, on_delete=models.RESTRICT)

    def __str__(self):
        return f"<CrawlableUrl#{self.url_id} {self.url.truncated}>"


class OccurrenceInOsm(models.Model):
    url = models.ForeignKey(Url, on_delete=models.RESTRICT)
    osm_item_type = models.CharField(max_length=1, blank=False)
    osm_item_id = models.BigIntegerField()
    osm_tag_key = models.CharField(max_length=100)
    osm_tag_value = models.CharField(max_length=300)
    # Note that duplicates are meaningful (e.g. "website=https://foo.com;https://foo.com"), although not very informative.
    # These will be completely wiped and re-written on every import anyway.

    def __str__(self):
        return f"<OccurrenceInOsm#{self.id} {self.osm_item_type}{self.osm_item_id}>"

    class Meta:
        verbose_name = "occurrence in OSM"
        verbose_name_plural = "occurrences in OSM"


class Import(models.Model):
    urlfile_name = models.CharField(max_length=200)
    import_begin = models.DateTimeField()
    import_end = models.DateTimeField()
    additional_data = models.TextField()  # JSON
