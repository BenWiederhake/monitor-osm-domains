#!/usr/bin/env python3

from crawl import logic
from django.core.management.base import BaseCommand
from django.db import transaction
import functools
import storage
import storage.logic
import time

SLEEP_IF_NO_MATCH_SECONDS = 3600
MAX_REDIRECT_DEPTH = 10
# A redirect really *really* should be cheap. If this overwhelmes your server, it absolutely is your
# own fault. Browsers wait 0 seconds. And yet, we try to be extra super-nice and wait some time.
SLEEP_REDIRECT_SECONDS = 2.0


def crawl_prepared_url(crurl, curl_wrapper):
    if crurl is None:
        # Nothing matched; presumably because either the DB is empty, or somehow we crawled
        # everything so fast that we ran out of things to do. Slow down:
        print(f"Nothing to do! Sleeping for {SLEEP_IF_NO_MATCH_SECONDS} seconds …")
        time.sleep(SLEEP_IF_NO_MATCH_SECONDS)
        return
    last_request = None
    for _ in range(MAX_REDIRECT_DEPTH):
        assert crurl is not None
        print(f"Crawling {crurl} now …")
        with logic.CrawlProcess(crurl) as process:
            if last_request is not None:
                last_request.next_request = process.result
                last_request.save()
            result, errdict = curl_wrapper.crawl_response_or_errdict(crurl.url.url)
            crurl = None  # Done crawling, probably.
            if result is not None:
                next_url = None
                if result.location is not None:
                    maybe_next_crawlable = storage.logic.try_crawlable_url(result.location)
                    next_url = maybe_next_crawlable.url_obj
                    # If a redirect to a valid URL that we want to crawl, continue there:
                    crurl = maybe_next_crawlable.crawlable_url_obj_or_none
                result_success = process.submit_success(
                    result.status_code,
                    result.header.ba,
                    result.header.size,
                    result.header.truncated,
                    result.body.ba,
                    result.body.size,
                    result.body.truncated,
                    next_url,
                )
                print(f"    saved as {result_success.content_file}")
                last_request = process.result
            if errdict is not None:
                process.submit_error(curl_errdict)
        if crurl is None:
            # Done crawling the original crawlable URL, we have reached the end of the (possibly
            # empty) redirect chain.
            return
        time.sleep(SLEEP_REDIRECT_SECONDS)
    print(f"Redirect chain is too long! {MAX_REDIRECT_DEPTH=}")


def crawl_random_url(curl_wrapper):
    crurl = logic.pick_and_bump_random_crawlable_url()
    crawl_prepared_url(crurl, curl_wrapper)


def lock_then_bump_domain(domain):
    with transaction.atomic(durable=True):
        # This is an ugly hack.
        domain_locked = storage.models.Domain.objects.filter(id=domain.id).select_for_update().get()
        return logic.bump_locked_domain_or_none(domain_locked)


def crawl_domain(domain_row, curl_wrapper):
    bumped_domain = lock_then_bump_domain(domain_row)
    # We want to let crawl_prepared_url() handle the 'None' case, so let's treat "None" as a reasonably-valid domain:
    if bumped_domain is None:
        print(f"Note: Domain {domain_row.domain_name} is still on cooldown. last_contacted={domain_row.last_contacted} CRAWL_DOMAIN_DELAY_DAYS={logic.CRAWL_DOMAIN_DELAY_DAYS}")
        chosen_crurl = None
    else:
        chosen_crurl = logic.pick_random_crawlable_url_from_bumped_domain(bumped_domain)
    crawl_prepared_url(chosen_crurl, curl_wrapper)


def crawl_cli_url(crurl_row, curl_wrapper):
    if lock_then_bump_domain(crurl_row.domain) is None:
        print(f"Too soon! Domain is still on cooldown. last_contacted={crurl_row.domain.last_contacted} CRAWL_DOMAIN_DELAY_DAYS={logic.CRAWL_DOMAIN_DELAY_DAYS}")
        return
    crawl_prepared_url(crurl_row, curl_wrapper)


class Command(BaseCommand):
    help = "Crawl one (or more) URLs, maybe hand-picked, maybe fully-random, maybe semi-random."

    def add_arguments(self, parser):
        command_group = parser.add_mutually_exclusive_group(required=True)
        command_group.add_argument(
            "--domain",
            help="Crawl random URL from given domain; must already exist in database and match EXACTLY",
        )
        command_group.add_argument(
            "--url",
            help="Crawl specific URL; must already exist in database and match EXACTLY",
        )
        command_group.add_argument(
            "--random-url",
            help="Crawl uniformly random URL from the database",
            action="store_true",
        )
        parser.add_argument(
            "--next-delay-seconds",
            help="Enables endless crawling, waiting X seconds between each crawling attempt.",
            type=float,
        )

    def handle(self, *, domain, url, random_url, next_delay_seconds, **options):
        assert next_delay_seconds is None or next_delay_seconds > 0
        assert not (url is not None and next_delay_seconds is not None), "--url and --next-delay-seconds are mutually exclusive"
        if domain is not None and next_delay_seconds is not None:
            print(f"WARNING: This will ONLY crawl random urls from the domain >>{domain}<<, and nothing else!")
            print("    (… which will probably quickly lead to running out of URLs to be crawled.)")
        if domain is not None:
            domain_row = storage.models.Domain.objects.get(domain_name=domain)
            do_one_crawl = functools.partial(crawl_domain, domain_row)
        elif random_url:
            do_one_crawl = crawl_random_url
        elif url is not None:
            crurl_row = storage.models.Url.objects.get(url=url).crawlableurl
            assert crurl_row is not None, "URL exists, but is not marked as crawlable for unknown reasons."
            do_one_crawl = functools.partial(crawl_cli_url, crurl_row)
        curl_wrapper = logic.LockedCurl()
        while True:
            do_one_crawl(curl_wrapper)
            if next_delay_seconds is not None:
                print(f"Sleeping {next_delay_seconds} seconds …")
                time.sleep(next_delay_seconds)
            else:
                break
        print("Done crawling!")