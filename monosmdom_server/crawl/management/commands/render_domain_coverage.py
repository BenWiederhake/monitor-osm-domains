#!/usr/bin/env python3

from django.core.management.base import BaseCommand
from django.db import transaction
from monosmdom_server import common
import datetime
import os.path
import storage
import subprocess
import tempfile


# How many seconds does an output unit have?
# "1" means that the output value "42" means 42 seconds.
# "60" means that the output value "42" means 42 minutes.
# "24 * 3600" means that the output value "42" means 42 days.
OUTPUT_UNIT_NAME = "days"
OUTPUT_UNIT_SECONDS = 24 * 3600

GNUPLOT_PROGRAM_COMMON_FORMAT = """
set datafile separator ",";
set xlabel "Time [{OUTPUT_UNIT_NAME}]";
set style data linespoints;
set xtics 1;
set grid;
plot "{filename}" using 1:2 title "domains historically covered", "" using 1:3 title "best case prediction";
"""

GNUPLOT_PROGRAM_INTERACTIVE_FORMAT = GNUPLOT_PROGRAM_COMMON_FORMAT + "pause mouse close;"
GNUPLOT_PROGRAM_TOFILE_FORMAT = 'set terminal png size 1780,920 enhanced;set output "{output_filename}";' + GNUPLOT_PROGRAM_COMMON_FORMAT


def fetch_domain_times():
    with transaction.atomic():
        domains_uncontacted = storage.models.Domain.objects.filter(last_contacted__isnull=True, crawlableurl__isnull=False).distinct("id").count()
        domain_time_tuples = list(storage.models.Domain.objects.filter(last_contacted__isnull=False, crawlableurl__isnull=False).distinct("id").values_list("last_contacted"))
    print(f"  Got {len(domain_time_tuples)} results. Unpacking …")
    domain_times = [last_contacted for (last_contacted,) in domain_time_tuples if last_contacted is not None]
    print(f"  got {domains_uncontacted} uncontacted domains and {len(domain_times)} datetimes (e.g. {domain_times[0]}).")
    print("  Sorting …")
    domain_times.sort(reverse=True)
    print(f"  Percentiles are 0%={domain_times[0]}, 50%={domain_times[len(domain_times) // 2]}, 100%={domain_times[-1]}.")
    return domain_times, (domains_uncontacted + len(domain_times))


def compute_coverage(sorted_domain_times, now, total_domains):
    assert now > sorted_domain_times[0], f"freshest domain from {sorted_domain_times[0]} is NEWER than now={now}?!"
    cumulative_coverage = []  # Initial `(datetime.timedelta(), 0)` entry is implicit
    best_case_duration = datetime.timedelta(days=10000000)  # Ugly hack
    for covered_before, domain_time in enumerate(sorted_domain_times):
        time_since = now - domain_time
        now_covered = covered_before + 1
        cumulative_coverage.append((time_since, now_covered))
        if now_covered < 100:
            continue
        guessed_duration = time_since * total_domains / now_covered
        if guessed_duration < best_case_duration:
            best_case_duration = guessed_duration
    return cumulative_coverage, best_case_duration


def render_as_csv(fp, cumulative_coverage, best_case_duration, now, total_domains):
    fp.write(f"{OUTPUT_UNIT_NAME},domains historically covered,best case prediction\n")
    fp.write("0,0,0\n")
    fp.write(f"{best_case_duration.total_seconds() / OUTPUT_UNIT_SECONDS},,{total_domains}\n")
    for timedelta, covered_domains in cumulative_coverage:
        fp.write(f"{timedelta.total_seconds() / OUTPUT_UNIT_SECONDS},{covered_domains}\n")


def run_gnuplot(csv_data_filename, png_output_filename):
    if png_output_filename is None:
        format_str = GNUPLOT_PROGRAM_INTERACTIVE_FORMAT
    else:
        format_str = GNUPLOT_PROGRAM_TOFILE_FORMAT
    gnuplot_script = format_str.format(
        filename=csv_data_filename,
        output_filename=png_output_filename,
        OUTPUT_UNIT_NAME=OUTPUT_UNIT_NAME,
    )
    subprocess.run(
        ["gnuplot", "-e", gnuplot_script],
        stdin=subprocess.DEVNULL,
        check=True,
    )


def open_or_temporary(to_csv_file):
    if to_csv_file is None:
        return tempfile.NamedTemporaryFile(mode="w+", prefix="domain_coverage_", suffix=".csv")
    else:
        return open(to_csv_file, "w+")


class Command(BaseCommand):
    help = "Crawl one (or more) URLs, maybe hand-picked, maybe fully-random, maybe semi-random."

    def add_arguments(self, parser):
        parser.add_argument(
            "--to-csv-file",
            help="Compute results and write them into CSV file",
        )
        parser.add_argument(
            "--to-png-file",
            help="Compute and plot results and write them into PNG file",
        )
        parser.add_argument(
            "--show-gnuplot",
            help="Compute and plot results and show them in an interactive gnuplot window",
            action="store_true",
        )
        parser.add_argument(
            "--now",
            help="Use the argument as 'now'. Useful to get reasonable predictions when running on\
                  (old) databasedumps. The datetime must be provided in any valid ISO 8601 format:\
                  https://docs.python.org/3/library/datetime.html#datetime.datetime.fromisoformat\
                  Examples (comma-separated): 2011-11-04, 2011-11-04T00:05:23,\
                  2011-11-04 00:05:23.283+00:00",
            type=datetime.datetime.fromisoformat,
            # Cannot determine "now", as it refers to the time *after* the domains have been fetched.
            default=None,
        )

    def handle(self, *, to_csv_file, to_png_file, show_gnuplot, now, **options):
        # Argument checking that should probably be done in ArgumentParser:
        print(f"{to_csv_file=} {to_png_file=} {show_gnuplot=}")
        assert (to_csv_file is not None) + (to_png_file is not None) + show_gnuplot > 0, "At least one option must be given?!"
        if to_csv_file is not None:
            assert not os.path.exists(to_csv_file), f"File {to_csv_file} already exists, refusing to overwrite!"
        print("Fetching domain data …")
        sorted_domain_times, total_domains = fetch_domain_times()
        print("Computing coverage curve and best-case prediction …")
        if now is None:
            # Note: Since the database might return ultra-fresh results, we cannot determine "now"
            # before the query has finished. Likewise, if we assume that by pure chance we receive
            # a domain just milliseconds after it has been crawled, that would seem like a rate of
            # a thousand domains per second – clearly not what we intended. Hence, we artificially
            # inflate "now", so that we can ignore the initial "bump".
            now = common.now_tzaware() + datetime.timedelta(minutes=2)
        cumulative_coverage, best_case_duration = compute_coverage(sorted_domain_times, now, total_domains)
        if to_csv_file is None:
            print("Writing CSV to temporary file …")
        else:
            print(f"Writing CSV to file {to_csv_file} …")
        with open_or_temporary(to_csv_file) as fp:
            if to_csv_file is None:
                to_csv_file = fp.name
            render_as_csv(fp, cumulative_coverage, best_case_duration, now, total_domains)
            # Cannot close NamedTemporary yet, since this would delete it. Instead, only flush:
            fp.flush()
            if to_png_file is not None:
                print(f"Writing PNG to {to_png_file} …")
                run_gnuplot(to_csv_file, to_png_file)
            if show_gnuplot:
                print("Showing interactively …")
                run_gnuplot(to_csv_file, None)
        print("Done with rendering!")
