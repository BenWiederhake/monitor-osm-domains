#!/usr/bin/env python3

from django.core.management.base import BaseCommand
from crawl import logic


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--url")

    def handle(self, *, url, **options):
        if url is None:
            url = "https://www.google.com/"  # "good" response
            # url = "https://eatcs.org/"  # large response, needs intermediate cert
            # url = "https://invalid.org/"  # good dns, does not connect I think?
            # url = "https://invalid.invalid/"  # bad dns (duh)
            # url = "http://www.kaffeewerkstatt.center/"  # 300
        print(f"Crawling >>{url}<< …")
        c = logic.LockedCurl(verbose=True)
        result, errdict = c.crawl_response_or_errdict(url)
        if result is not None:
            print(f"{result.body.ba=}")
            print()
            print(f"{result.header.ba=}")
            print()
            print(f"{result.header.size=} (saved {len(result.header.ba)}) {result.header.truncated=}")
            print(f"{result.body.size=} (saved {len(result.body.ba)}) {result.body.truncated=}")
            print(f"HTTP {result.status_code=:03} {result.location=}")
        if errdict is not None:
            print(f"{errdict=}")
        assert (result is None) != (errdict is None)
