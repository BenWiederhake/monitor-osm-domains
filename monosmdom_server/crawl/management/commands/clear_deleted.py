#!/usr/bin/env python3

from crawl import models
from django.core.management.base import BaseCommand
from monosmdom_server import common
import os.path
import random
import time


# How many field should we clear at most?
LIMIT_MAX_CLEAR = 2000


def get_confirmation():
    random_digit = random.randrange(10)
    expected_input = f"DEL{random_digit}ETE"
    query = f"Does that seem reasonable? Type 'del{random_digit}ete' in ALLCAPS to clear the references to these non-existent files; anything else aborts.\n"
    try:
        actual_input = input(query)
        if actual_input == expected_input:
            return True
        else:
            return False
    except KeyboardInterrupt:
        return False


class Command(BaseCommand):
    help = "Clear FileField cells that point to deleted files."

    def handle(self, **options):
        print(f"Collecting …")
        collected = []
        total_considered = 0
        # Skip those which have no file associated anyway:
        qs = models.ResultSuccess.objects.exclude(content_file="")
        aborted = False
        for e in qs.iterator():
            total_considered += 1
            if os.path.exists(e.content_file.path):
                # Nevermind!
                continue
            # Found an outdated row!
            collected.append(e)
            if len(collected) > LIMIT_MAX_CLEAR:
                aborted = True
                break
        if aborted:
            print("=======")
            print(f"WARNING: Checking aborted after {len(collected)} faulty rows in {total_considered} total rows. Perhaps the setup is broken?")
            print("=======")
        print(f"Done checking {total_considered} rows with associated files!")
        if not collected:
            print("No missing files detected. Have a nice day!")
            return
        print(f"Would clear {len(collected)} references to missing files, e.g. {collected[0].content_file} in {collected[0]}.")
        if not get_confirmation():
            print(f"Not updating (aborted by user)")
            return
        print(f"Updating {len(collected)} rows …")
        t1 = time.time()
        for e in collected:
            e.content_file = None
        models.ResultSuccess.objects.bulk_update(collected, ["content_file"])
        t2 = time.time()
        print(f"Finished updating {len(collected)} rows in {t2 - t1:.3f}s.")
