from django.shortcuts import render
from monosmdom_server import common
import crawl.models
import storage.models


# FIXME: How to make this the default?
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z"


def compute_fresh_stats():
    stats = dict()
    stats["Current server time"] = common.now_tzaware().strftime(DATETIME_FORMAT)
    stats["OSM import epoch"] = storage.models.Import.objects.count()
    # This code was written long after the crawler already started running,
    # and I don't care too much about being able to run other instances.
    # So, we happily break here if there was no input yet.
    most_recent_import = storage.models.Import.objects.order_by("-import_end")[0]
    stats["Most recent OSM import"] = most_recent_import.import_end.strftime(DATETIME_FORMAT)
    occ_total = storage.models.OccurrenceInOsm.objects.count()
    stats["OSM item-tags with URLs"] = f"{occ_total} entries"
    crawl_total = crawl.models.Result.objects.count()
    crawl_success = crawl.models.ResultSuccess.objects.count()
    crawl_error = crawl.models.ResultError.objects.count()
    stats["Crawled"] = f"{crawl_total} URLs ({crawl_success} successes, {crawl_error} errors, {crawl_total - crawl_success - crawl_error} unfinished)"
    url_total = storage.models.Url.objects.count()
    url_disaster_total = storage.models.DisasterUrl.objects.count()  # TODO add link to detections
    url_crawlable_total = storage.models.CrawlableUrl.objects.count()
    stats["Known URLs"] = f"{url_total} URLs ({url_disaster_total} nonsensical URLs, {url_crawlable_total} crawled URLs, rest uninteresting)"
    domain_total = storage.models.Domain.objects.count()
    # Computing more detailed stats requires slightly more expensive DB operations.
    # I don't want that to be easily triggerable from the outside.
    stats["Known domains"] = f"{domain_total} domains (nonsensical + crawled + uninteresting)"
    # We happily break here if there was no crawl attempt yet.
    most_recent_crawl = crawl.models.Result.objects.order_by("-crawl_begin")[0]
    stats["Most recent crawl attempt"] = f"{most_recent_crawl.crawl_begin.strftime(DATETIME_FORMAT)} of {most_recent_crawl.url.url}"
    return stats


def show_health_stats():
    stats = dict()
    stats["hello"] = "world"
    return stats
