from django.core.management.base import BaseCommand
from django.db import connection, transaction
from storage import logic, models
import json
import random
import urllib.parse


ANSI_RESET = "\x1b[0m"
ANSI_RED = "\x1b[91m"
ANSI_GREEN = "\x1b[92m"


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
    print(f"    Total URLs known: {models.Url.objects.count()}")
    print(f"      … disaster reasons: {models.DisasterUrl.objects.count()}")
    print(f"      … crawlable: {models.CrawlableUrl.objects.count()}")
    print(f"    Total Domains known: {models.Domain.objects.count()}")
    print(f"    Total URL detections in OSM data: {models.OccurrenceInOsm.objects.count()}")
    print()


def update_osm_state(disasters, simplified_urls):
    # No need to touch CrawlResult, CrawlResultSuccess, CrawlResultError at all.
    # We completely delete and re-write the tables CrawlableUrl, DisasterUrl, OccurrenceInOsm.
    # The tables Domain and Url are extended, and existing data remains untouched.
    models.CrawlableUrl.objects.all().delete()
    models.DisasterUrl.objects.all().delete()
    models.OccurrenceInOsm.objects.all().delete()
    pass  # FIXME
    return 0, 0


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
            num_disasters, num_valid_urls = update_osm_state(disasters, simplified_urls)
            print(f"Finished importing {num_disasters} disasters and {num_valid_urls} crawlable URLs.")
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
