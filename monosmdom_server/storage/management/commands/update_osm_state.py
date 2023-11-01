from django.core.management.base import BaseCommand
from django.db import connection, transaction
from monosmdom_server import common
from storage import logic, models
import json
import random
import datetime
import urllib.parse


REPORT_PERCENT_STEP = 2  # Must be a divisor of 100
ANSI_RESET = "\x1b[0m"
ANSI_RED = "\x1b[91m"
ANSI_GREEN = "\x1b[92m"


assert 100 % REPORT_PERCENT_STEP == 0


def read_urlfile(urlfile):
    # File format example, shown with `gron all.monosmdom.json | less`:
    # json = {};
    # json.disasters = {};
    # json.disasters["http:// bsr.de"] = {};
    # json.disasters["http:// bsr.de"].occs = [];
    # json.disasters["http:// bsr.de"].occs[0] = {};
    # json.disasters["http:// bsr.de"].occs[0].id = 10079244117;
    # json.disasters["http:// bsr.de"].occs[0].k = "website";
    # json.disasters["http:// bsr.de"].occs[0].orig_url = "http:// bsr.de";
    # json.disasters["http:// bsr.de"].occs[0].t = "n";
    # json.disasters["http:// bsr.de"].reasons = [];
    # json.disasters["http:// bsr.de"].reasons[0] = "weird character b' '";
    # json.disasters["http:// www.kelmsküche.de"] = {};
    # … more disasters …
    # json.simplified_urls = {};
    # json.simplified_urls["http://0039italy-shop.com/"] = [];
    # json.simplified_urls["http://0039italy-shop.com/"][0] = {};
    # json.simplified_urls["http://0039italy-shop.com/"][0].id = 900538158;
    # json.simplified_urls["http://0039italy-shop.com/"][0].k = "website";
    # json.simplified_urls["http://0039italy-shop.com/"][0].orig_url = "http://0039italy-shop.com/";
    # json.simplified_urls["http://0039italy-shop.com/"][0].t = "w";
    # json.simplified_urls["http://01ulf6.wix.com/soetbeer"] = [];
    # … more simplified_urls, then EOF.
    with open(urlfile, "r") as fp:
        data = json.load(fp)
    assert data["v"] == 1
    assert data["type"] == "monitor-osm-domains extraction results, filtered"
    assert set(data.keys()) == set("v type disasters simplified_urls".split())
    return data['disasters'], data['simplified_urls']


def show_summary(when):
    print()
    print(f"  Stats {when}:")
    print(f"    Crawl results: {models.CrawlResultSuccess.objects.count()}s + {models.CrawlResultError.objects.count()}e (should stay constant)")
    urls_total = models.Url.objects.count()
    disaster_entries_total = models.DisasterUrl.objects.count()
    disaster_urls_total = models.DisasterUrl.objects.all().distinct().count()
    crawlable_total = models.CrawlableUrl.objects.count()
    print(f"    Total URLs known: {urls_total}")
    print(f"      … disaster URLs: {disaster_urls_total}")
    print(f"        … total entries: {disaster_entries_total}")
    print(f"      … crawlable URLs: {crawlable_total}")
    print(f"      … ignored/obsolete: {urls_total - disaster_urls_total - crawlable_total} (calculated)")
    print(f"    Total Domains known: {models.Domain.objects.count()}")
    print(f"    Total URL detections in OSM data: {models.OccurrenceInOsm.objects.count()}")
    print()


def register_occurrence(url_object, occ_dict):
    # Example input, shown in gron format:
    # occ_dict.id = 10079244117;
    # occ_dict.k = "website";
    # occ_dict.orig_url = "http:// bsr.de";
    # occ_dict.t = "n";
    assert set(occ_dict.keys()) == {"id", "k", "orig_url", "t"}, occ_dict
    occ_obj = models.OccurrenceInOsm.objects.create(
        url=url_object,
        osm_item_type=occ_dict["t"],
        osm_item_id=occ_dict["id"],
        osm_tag_key=occ_dict["k"],
        osm_tag_value=occ_dict["orig_url"],
    )
    # Note that this creates duplicates if the imported data contains e.g. "website=https://foo.com;https://foo.com".
    # This is not very informative, but it seems even more wasteful to keep an index and try to avoid duplicates.
    # These will be completely wiped and re-written on every import anyway.


def update_osm_state(disasters, simplified_urls):
    # No need to touch CrawlResult, CrawlResultSuccess, CrawlResultError at all.
    # We completely delete and re-write the tables CrawlableUrl, DisasterUrl, OccurrenceInOsm.
    # The tables Domain and Url are extended, and existing data remains untouched.
    print(f"    Wiping old OSM state (tables CrawlableUrl, DisasterUrl, OccurrenceInOsm) …")
    models.CrawlableUrl.objects.all().delete()
    models.DisasterUrl.objects.all().delete()
    models.OccurrenceInOsm.objects.all().delete()
    print(f"    Importing disaster URLs …")
    for url_string, disaster_context in disasters.items():
        assert set(disaster_context.keys()) == {"occs", "reasons"}
        url_object = logic.upsert_url(url_string)
        for disaster_reason in disaster_context["reasons"]:
            models.DisasterUrl.objects.create(url=url_object, reason=disaster_reason)
        for occ_dict in disaster_context["occs"]:
            register_occurrence(url_object, occ_dict)
    print(f"    Importing and checking simplified URLs …")
    done_items = 0
    percent_last_reported = 0
    percent_step_began = common.now_tzaware()
    for url_string, occs in simplified_urls.items():
        # Note: This duplicates the "Syntactical" check that was already done during "extract/cleanup.py".
        # However, this means very little additional work, and deduplicating the code seems more important on this occasion.
        # TODO: The semantical checks still are a lot of work. Can this be precomputed and bulk-inserted instead?
        maybe_crawlable = logic.try_crawlable_url(url_string, create_disaster=True)
        for occ_dict in occs:
            register_occurrence(maybe_crawlable.url_obj, occ_dict)
        # if maybe_crawlable.crawlable_url_obj_or_none is None:
        #     # The disaster reason was already added, nothing to do.
        #     continue
        # # Mark this CrawlableUrl as active:
        # maybe_crawlable.crawlable_url_obj_or_none.in_osm_data = True
        # # TODO: Should "in_osm_data" be precomputed? Or computed on the fly? Or a materialized view?
        done_items += 1
        percent_done = done_items * 100 / len(simplified_urls)
        if percent_done >= percent_last_reported + REPORT_PERCENT_STEP:
            percent_last_reported += REPORT_PERCENT_STEP
            percent_step_ended = common.now_tzaware()
            remaining_time = (percent_step_ended - percent_step_began) * (100 - percent_done) / REPORT_PERCENT_STEP
            eta = (percent_step_ended + remaining_time).strftime("%F %T")
            time_now = percent_step_ended.strftime("%F %T")
            print(f"      {percent_last_reported:03}% done ({done_items}/{len(simplified_urls)} at time {time_now}, ETA {eta})")
            percent_step_began = percent_step_ended


def get_confirmation():
    random_digit = random.randrange(10)
    expected_input = f"OVER{random_digit}WRITE"
    query = f"Does that seem reasonable? Type 'over{random_digit}write' in ALLCAPS to commit the overwrite; anything else aborts.\n"
    try:
        actual_input = input(query)
        if actual_input == expected_input:
            return True, None
        else:
            return False, f"'{expected_input}' != '{actual_input}'"
    except KeyboardInterrupt:
        return False, "<Ctrl-C>"


class Command(BaseCommand):
    help = "Overwrites the set of URLs-to-be-crawled"

    def add_arguments(self, parser):
        parser.add_argument("urlfile", metavar="file_with_all_osm_urls.monosmdom.json")

    def handle(self, *, urlfile, **options):
        print(f"Initializing PSL …")
        logic.get_cached_psl()

        print(f"Reading {urlfile=} …")
        disasters, simplified_urls = read_urlfile(urlfile)

        with transaction.atomic():
            show_summary("before")
            print(f"Importing at least {len(disasters)} disasters and at most {len(simplified_urls)} crawlable URLs …")
            print(f"    (Numbers might change slightly due to ignored or unregistered domains.)")
            update_osm_state(disasters, simplified_urls)
            show_summary("after")
            should_commit, reason = get_confirmation()
            if should_commit:
                print(f"{ANSI_GREEN}Committing!{ANSI_RESET} Making state permanent …")
                # Making state permanent by returning from "atomic".
            else:
                print(f"{ANSI_RED}Rolling back!{ANSI_RESET} No changes will be applied ({reason})")
                transaction.set_rollback(True)
        if should_commit:
            print("Running 'VACUUM ANALYZE' …")
            with connection.cursor() as cursor:
                cursor.execute("VACUUM ANALYZE")
        print("All done!")
