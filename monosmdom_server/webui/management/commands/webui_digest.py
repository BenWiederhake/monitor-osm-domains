from django.contrib.staticfiles.storage import StaticFilesStorage

from django.core.management.base import BaseCommand
from django.db import transaction
from monosmdom_server import common
from webui import logic, models
from crawl.management.commands import render_domain_coverage
import datetime


def rewrite_coverage_graph():
    png_filename = StaticFilesStorage().path("domains.png")
    print(f"Rendering begin, to {png_filename}")
    sorted_domain_times, total_domains = render_domain_coverage.fetch_domain_times()
    now = common.now_tzaware() + datetime.timedelta(minutes=2)
    cumulative_coverage, best_case_duration = render_domain_coverage.compute_coverage(sorted_domain_times, now, total_domains)
    with render_domain_coverage.open_or_temporary(None) as fp:
        render_domain_coverage.render_as_csv(fp, cumulative_coverage, best_case_duration, now, total_domains)
        # Cannot close NamedTemporary yet, since this would delete it. Instead, only flush:
        fp.flush()
        render_domain_coverage.run_gnuplot(fp.name, png_filename)
    print("Rendering done")


class Command(BaseCommand):
    help = "Cronable script that updates health and webui indices"

    def handle(self, **options):
        digestion_begin = common.now_tzaware()
        rewrite_coverage_graph()
        print("Computing stats …")
        with transaction.atomic():
            fresh_json = logic.compute_fresh_stats()
            expensive_json = logic.compute_expensive_stats()
            # FIXME: Make graph!
            # FIXME: Compute more expensive stats!
            print("Overwriting webui indices …")
            # FIXME: Actually digest stuff for webui!
            print("Appending to DigestionHealth log …")
            models.DigestionHealth.objects.create(
                digestion_begin=digestion_begin,
                digestion_end=common.now_tzaware(),
                # Must convert to list in order to preserve order
                fresh_json=list(fresh_json.items()),
                expensive_json=list(expensive_json.items()),
            )
            print("Committing …")
        print("All done!")
