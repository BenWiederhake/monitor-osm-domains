from django.conf import settings
from django.db.models import F
from django.shortcuts import render
from monosmdom_server import common
import crawl.models
import storage.models
import datetime
import os


def compute_fresh_stats():
    stats = dict()
    stats["Current server time"] = common.strftime(common.now_tzaware())
    stats["OSM import epoch"] = storage.models.Import.objects.count()
    # This code was written long after the crawler already started running,
    # and I don't care too much about being able to run other instances.
    # So, we happily break here if there was no input yet.
    most_recent_import = storage.models.Import.objects.order_by("-import_end")[0]
    stats["Most recent OSM import"] = common.strftime(most_recent_import.import_end)
    occ_total = storage.models.OccurrenceInOsm.objects.count()
    stats["OSM item-tags with URLs"] = f"{occ_total:,} entries"
    crawl_total = crawl.models.Result.objects.count()
    crawl_success = crawl.models.ResultSuccess.objects.count()
    crawl_error = crawl.models.ResultError.objects.count()
    stats["Crawled"] = f"{crawl_total:,} URLs ({crawl_success:,} successes, {crawl_error:,} errors, {crawl_total - crawl_success - crawl_error:,} unfinished)"
    url_total = storage.models.Url.objects.count()
    url_disaster_total = storage.models.DisasterUrl.objects.count()  # TODO add link to detections
    url_crawlable_total = storage.models.CrawlableUrl.objects.count()
    stats["Known URLs"] = f"{url_total:,} URLs ({url_disaster_total:,} nonsensical URLs, {url_crawlable_total:,} crawled URLs, rest uninteresting)"
    domain_total = storage.models.Domain.objects.count()
    # Computing more detailed stats requires slightly more expensive DB operations.
    # I don't want that to be easily triggerable from the outside.
    stats["Known domains"] = f"{domain_total:,} domains (nonsensical + crawled + uninteresting)"
    # We happily break here if there was no crawl attempt yet.
    most_recent_crawl = crawl.models.Result.objects.order_by("-crawl_begin")[0]
    stats["Most recent crawl attempt"] = f"{common.strftime(most_recent_crawl.crawl_begin)} of {most_recent_crawl.url.url}"
    return stats


def compute_expensive_stats():
    stats = dict()
    # Most recent finished crawl SUCCESS, TODO: one for each HTTP status code?
    recent_success = crawl.models.ResultSuccess.objects.order_by("-result__crawl_begin")[0]
    stats["Most recent successful crawl"] = f"{common.strftime(recent_success.result.crawl_begin)} {recent_success.result.url} (HTTP {recent_success.status_code})"
    # Most recent finished crawl EXTERNAL error
    recent_external = crawl.models.ResultError.objects.order_by("-result__crawl_begin").filter(is_internal_error=False)[0]
    stats["Most recent externally failed crawl"] = f"{common.strftime(recent_external.result.crawl_begin)} {recent_external.result.url} (reason: {recent_external.description_json})"
    # Most recent finished crawl INTERNAL error
    recent_internal = crawl.models.ResultError.objects.order_by("-result__crawl_begin").filter(is_internal_error=True)[0]
    stats["Most recent internally failed crawl"] = f"{common.strftime(recent_internal.result.crawl_begin)} {recent_internal.result.url}"
    # Most recent UNFINISHED crawl attempts (when and url)
    unfinished_crawl_attempts = crawl.models.Result.objects.order_by("-crawl_begin").filter(resultsuccess__isnull=True, resulterror__isnull=True)[: 3]
    for i, result in enumerate(unfinished_crawl_attempts):
        stats[f"Most recent unfinished crawl (n-{i})"] = f"{common.strftime(result.crawl_begin)} {result.url}"
    # Slowest result in the last 7 days
    # TODO: Also for the last 24 hours?
    seven_days_ago = common.now_tzaware() - datetime.timedelta(days=7)
    slowest_week = crawl.models.Result.objects.filter(crawl_end__isnull=False).order_by(F("crawl_begin") - F("crawl_end"))[0]
    stats["Slowest crawl in the last seven days"] = f"{slowest_week.crawl_end - slowest_week.crawl_begin} {recent_internal.result.url}"
    # incomplete redirect-chain in the last 7 days
    # TODO: longest redirect-chain in the last 7 days
    aborted_chain = crawl.models.ResultSuccess.objects.filter(next_url__isnull=False, next_request__isnull=True).order_by("-result__crawl_begin")[: 1]
    if not aborted_chain:
        stats["Most recent aborted redirect chain"] = "<never happened>"
    else:
        stats["Most recent aborted redirect chain"] = f"{common.strftime(aborted_chain[0].result.crawl_begin)} {aborted_chain[0]} pointing to un-crawled {aborted_chain[0].next_url}"
    # Total header and body data (raw / compressed)
    counts = {"headers": 0, "header_raw_bytes": 0, "header_compressed_bytes": 0, "bodies": 0, "body_raw_bytes": 0}
    print("compute_expensive_stats now summing all sizes, this might take a while ...")
    for success in crawl.models.ResultSuccess.objects.all().iterator():
        if success.headers is not None:
            counts["headers"] += 1
            counts["header_raw_bytes"] += abs(success.headers_orig_size)
            counts["header_compressed_bytes"] += len(success.headers)
        if success.content_file is not None:
            counts["bodies"] += 1
            counts["body_raw_bytes"] += abs(success.content_orig_size)
            # TODO: body compressed cannot be done on server :'(
            # counts["header_compressed_bytes"] += len(success.content_file.read())
    print("compute_expensive_stats summing all sizes DONE")
    stats["Saved headers"] = f'{counts["headers"]:,}'
    stats["Total header size (untruncated)"] = f'{counts["header_raw_bytes"]:,}'
    stats["Stored header size (truncated, compressed)"] = f'{counts["header_compressed_bytes"]:,}'
    stats["Saved bodies"] = f'{counts["bodies"]:,}'
    stats["Total body size (untruncated)"] = f'{counts["body_raw_bytes"]:,}'
    # Last modification time of the configured combined.pem
    try:
        combined_pem_stat = os.stat(settings.CAINFO_ROOT_AND_INTERMEDIATE)
        stats["Intermediate certs timestamp"] = common.strftime(datetime.datetime.fromtimestamp(combined_pem_stat.st_mtime))
    except:
        stats["Intermediate certs timestamp"] = "ERROR! Could not read file (?!)"
    return stats
