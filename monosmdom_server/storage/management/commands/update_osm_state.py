from django.core.management.base import BaseCommand
from django.db import connection, transaction
from storage import models
import json
import publicsuffix2
import random


ANSI_RESET = "\x1b[0m"
ANSI_RED = "\x1b[91m"
ANSI_GREEN = "\x1b[92m"


def initialize_psl(no_fetch_pls):
    if no_fetch_pls:
        print("WARNING: Using outdated vendored publicsuffixlist.")
        return publicsuffix2.PublicSuffixList()
    return publicsuffix2.PublicSuffixList(publicsuffix2.fetch())


def read_urlfile(urlfile):
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
    total_domains = models.Domain.objects.count()
    ignored_domains = models.Domain.objects.filter(is_ignored=True).count()
    print(f"    Total Domains known: {total_domains}")
    print(f"      … ignored: {ignored_domains}")
    print(f"      … to be crawled: {total_domains - ignored_domains}")
    print(f"    Total URL detections in OSM data: {models.OccurrenceInOsm.objects.count()}")
    print()


def update_osm_state(disasters, simplified_urls):
    pass  # FIXME


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
        parser.add_argument("--no-fetch-pls", help="Don't fetch the public suffix list from the official site.", action="store_true")

    def handle(self, *, urlfile, no_fetch_pls, **options):
        print(f"Initializing PSL …")
        psl = initialize_psl(no_fetch_pls)

        print(f"Reading {urlfile=} …")
        disasters, simplified_urls = read_urlfile(urlfile)

        with transaction.atomic():
            show_summary("before")
            print(f"Importing {len(disasters)} disasters and {len(simplified_urls)} crawlable URLs …")
            update_osm_state(disasters, simplified_urls)
            show_summary("after")
            should_commit, reason = get_confirmation()
            if should_commit:
                print(f"{ANSI_GREEN}Committing!{ANSI_RESET} Making state permanent …")
                # Making state permanent by returning from "atomic".
            else:
                print(f"{ANSI_RED}Rolling back!{ANSI_RESET} No changes will be applied: ({reason})")
                transaction.set_rollback(True)
        if should_commit:
            print("Running 'VACUUM ANALYZE' …")
            with connection.cursor() as cursor:
                cursor.execute("VACUUM ANALYZE")
        print("All done!")
