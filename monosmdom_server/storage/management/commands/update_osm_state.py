from django.core.management.base import BaseCommand
from django.db import connection, transaction
from storage import models
import json
import random


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


class Command(BaseCommand):
    help = "Overwrites the set of URLs-to-be-crawled"

    def add_arguments(self, parser):
        parser.add_argument("urlfile", metavar="file_with_all_osm_urls.monosmdom.json")

    def handle(self, urlfile, *args, **options):
        print(f"Reading {urlfile=} …")
        with open(urlfile, "r") as fp:
            data = json.load(fp)
        assert data["v"] == 1
        assert data["type"] == "monitor-osm-domains extraction results, filtered"
        assert set(data.keys()) == set("v type disasters simplified_urls".split())
        with transaction.atomic():
            show_summary("before")
            print(f"Importing {len(data['disasters'])} disasters and {len(data['simplified_urls'])} crawlable URLs …")
            update_osm_state(data["disasters"], data["simplified_urls"])
            # Pretend we do the import here
            show_summary("after")
            random_digit = random.randrange(10)
            expected_input = f"OVER{random_digit}WRITE"
            query = f"Does that seem reasonable? Type 'over{random_digit}write' in ALLCAPS to commit the overwrite; anything else aborts.\n"
            try:
                actual_input = input(query)
                should_roll_back = expected_input != actual_input
            except KeyboardInterrupt:
                actual_input = "<Ctrl-C>"
                should_roll_back = True
            if should_roll_back:
                print(f"Rolling back! No changes will be applied. ('{actual_input}' != '{expected_input}')")
                transaction.set_rollback(True)
            else:
                print("Okay! Making state permanent …")
                # Making state permanent by returning from "atomic".
        if not should_roll_back:
            print("Vacuuming … (this recomputes index statistics)")
            with connection.cursor() as cursor:
                cursor.execute("VACUUM ANALYZE")
        print("All done!")
