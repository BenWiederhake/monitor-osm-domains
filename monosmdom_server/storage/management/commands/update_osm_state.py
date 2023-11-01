from django.core.management.base import BaseCommand
from django.db import connection, transaction
from monosmdom_server import common
from storage import logic, models
import crawl.models
import json
import random
import datetime
import urllib.parse


REPORT_PERCENT_STEP = 2  # Must be a divisor of 100
ANSI_RESET = "\x1b[0m"
ANSI_RED = "\x1b[91m"
ANSI_GREEN = "\x1b[92m"
# How many seconds do 1 million inserts though bulk_create take?
# This is only used for ETA estimation.
MEGA_INSERT_TIME = datetime.timedelta(seconds=75, milliseconds=600)


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
    crawl_successes = crawl.models.ResultSuccess.objects.count()
    crawl_errors = crawl.models.ResultError.objects.count()
    print(f"    Crawl results: {crawl_successes}s + {crawl_errors}e (should stay constant)")
    urls_total = models.Url.objects.count()
    disaster_entries_total = models.DisasterUrl.objects.count()
    disaster_urls_total = models.DisasterUrl.objects.all().distinct().count()
    crawlable_total = models.CrawlableUrl.objects.count()
    ignored_obsolete = urls_total - disaster_urls_total - crawlable_total
    print(f"    Total URLs known: {urls_total}")
    print(f"      … disaster URLs: {disaster_urls_total}")
    print(f"        … total entries: {disaster_entries_total}")
    print(f"      … crawlable URLs: {crawlable_total}")
    print(f"      … ignored/obsolete: {ignored_obsolete} (calculated)")
    domain_count = models.Domain.objects.count()
    print(f"    Total Domains known: {domain_count}")
    osm_occurrence_count = models.OccurrenceInOsm.objects.count()
    print(f"    Total URL detections in OSM data: {osm_occurrence_count}")
    print()
    return dict(
        crawl=dict(
            success=crawl_successes,
            error=crawl_errors,
        ),
        urls=dict(
            total=urls_total,
            disaster=dict(
                entries=disaster_entries_total,
                unique_urls=disaster_urls_total,
            ),
            crawlable=crawlable_total,
            ignored_obsolete=ignored_obsolete,
        ),
        domains=domain_count,
        osm_occurrences=osm_occurrence_count,
    )


def register_occurrence(url_object, occ_dict, cache):
    # Example input, shown in gron format:
    # occ_dict.id = 10079244117;
    # occ_dict.k = "website";
    # occ_dict.orig_url = "http:// bsr.de";
    # occ_dict.t = "n";
    assert set(occ_dict.keys()) == {"id", "k", "orig_url", "t"}, occ_dict
    cache.cache_occ(
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
    # How to import the data performantly?
    # - Ideally, we would use some kind of "automatic bulk upsert" scheme, that magically deals with
    #   foreign keys. That does not exist, however.
    # - Lacking that, we would ideally collect instances by class, and then bulk upsert all Domains,
    #   then all Urls, propagate their IDs to CrawlableUrl and DisasterUrl … – but alas, bulk_create
    #   with update_conflicts=True does NOT return primary keys:
    #     https://code.djangoproject.com/ticket/7596#comment:16
    #   Ironically, all that would be required is adding "RETURNING serial_pk" to the SQL query.
    # - So instead, we insert the Urls and Domains row-by-row, and only use fast bulk inserts for
    #   DisasterUrl, CrawlableUrl, and OccurrenceInOsm.
    # - The original "insert everything row by row" method is much slower.
    # I timed it using a random subset of the real data, specifically 2%. The last method took 22.76
    # seconds, and the second-last method only 10.6 seconds. I believe this can be reduced more.
    cache = logic.LeafModelBulkCache()
    for url_string, disaster_context in disasters.items():
        assert set(disaster_context.keys()) == {"occs", "reasons"}
        url_object = logic.upsert_url(url_string)
        for disaster_reason in disaster_context["reasons"]:
            logic.LeafModelBulkCache.create_durl(cache, url=url_object, reason=disaster_reason)
        for occ_dict in disaster_context["occs"]:
            register_occurrence(url_object, occ_dict, cache)
    print(f"    Importing and checking simplified URLs …")
    done_items = 0
    percent_last_reported = 0
    percent_step_began = common.now_tzaware()
    time_before = percent_step_began
    time_in_crurl = datetime.timedelta(0)
    time_in_register_occ = datetime.timedelta(0)
    for url_string, occs in simplified_urls.items():
        # Note: This duplicates the "Syntactical" check that was already done during "extract/cleanup.py".
        # However, this means very little additional work, and deduplicating the code seems more important on this occasion.
        # TODO: The semantical checks still are a lot of work. Can this be precomputed and bulk-inserted instead?
        t1 = common.now_tzaware()
        maybe_crawlable = logic.try_crawlable_url(url_string, create_disaster=True, cache=cache)
        t2 = common.now_tzaware()
        for occ_dict in occs:
            register_occurrence(maybe_crawlable.url_obj, occ_dict, cache)
        t3 = common.now_tzaware()
        time_in_crurl += t2 - t1
        time_in_register_occ += t3 - t2
        done_items += 1
        percent_done = done_items * 100 / len(simplified_urls)
        if percent_done >= percent_last_reported + REPORT_PERCENT_STEP:
            percent_last_reported += REPORT_PERCENT_STEP
            percent_step_ended = common.now_tzaware()
            time_now = percent_step_ended.strftime("%F %T")
            bulk_timedelta = MEGA_INSERT_TIME * cache.insert_count() / 1e6
            remaining_time = (percent_step_ended - percent_step_began) * (100 - percent_done) / REPORT_PERCENT_STEP
            eta = (percent_step_ended + remaining_time + bulk_timedelta).strftime("%F %T")
            print(f"      {percent_last_reported:3}% done ({done_items:6}/{len(simplified_urls):6} at time {time_now}, ETA {eta})")
            percent_step_began = percent_step_ended
    t1 = common.now_tzaware()
    time_now = t1.strftime("%F %T")
    bulk_inserts = cache.insert_count()
    bulk_timedelta = MEGA_INSERT_TIME * bulk_inserts / 1e6
    eta = (t1 + bulk_timedelta).strftime("%F %T")
    print(f"    Flushing bulk insert cache at time {time_now}, ETA {eta}")
    cache.flush()
    t2 = common.now_tzaware()
    print(f"        hardcoded {MEGA_INSERT_TIME=}")
    # Note: Timedelta is highly non-associative here! Its best resolution is microseconds, which
    # would lose lots of precision.
    measured_mega_insert_time = (t2 - t1) * (1e6 / bulk_inserts)
    print(f"        measured MEGA_INSERT_TIME={measured_mega_insert_time!r}")
    time_in_bulk = t2 - t1
    time_after = common.now_tzaware()
    time_now = t2.strftime("%F %T")
    print(f"Import finished at {time_now}, total time taken: {time_after - time_before}")
    print(f"    try_crawlable_url accounts for {time_in_crurl}")
    print(f"    register_occurrence accounts for {time_in_register_occ}")
    print(f"    flushing bulk cache ({bulk_inserts} rows) accounts for {time_in_bulk}")


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
            import_begin = common.now_tzaware()
            summary_before = show_summary("before")
            print(f"Importing at least {len(disasters)} disasters and at most {len(simplified_urls)} crawlable URLs …")
            print(f"    (Numbers might change slightly due to ignored or unregistered domains.)")
            update_osm_state(disasters, simplified_urls)
            summary_after = show_summary("after")
            additional_data = dict(
                summary_before=summary_before,
                summary_after=summary_after,
            )
            models.Import.objects.create(
                urlfile_name=urlfile,
                import_begin=import_begin,
                import_end=common.now_tzaware(),
                additional_data=json.dumps(additional_data),
            )
            should_commit, reason = get_confirmation()
            if should_commit:
                print(f"{ANSI_GREEN}Committing!{ANSI_RESET} Making state permanent. This might take a while …")
                # Making state permanent by returning from "atomic".
            else:
                print(f"{ANSI_RED}Rolling back!{ANSI_RESET} No changes will be applied ({reason})")
                transaction.set_rollback(True)
        if should_commit:
            print("Running 'VACUUM ANALYZE' …")
            with connection.cursor() as cursor:
                cursor.execute("VACUUM ANALYZE")
        print("All done!")
