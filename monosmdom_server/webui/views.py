from django.core.files.storage import FileSystemStorage
from django.http import FileResponse
from django.shortcuts import render
from monosmdom_server import common
from webui import logic, models


def index(request):
    return render(request, "index.html")


def serve_domains_png(_request):
    response = FileResponse(FileSystemStorage().open("domains.png"),)
    return response


def health(request):
    latest_digestionhealth = models.DigestionHealth.objects.order_by("-digestion_begin")[0]
    context = {
        "fresh_stats": logic.compute_fresh_stats().items(),
        "dh_fresh": latest_digestionhealth.fresh_json,
        "dh_expensive": latest_digestionhealth.expensive_json,
        "dh_digestion_begin": common.strftime(latest_digestionhealth.digestion_begin),
        "dh_digestion_time": latest_digestionhealth.digestion_end - latest_digestionhealth.digestion_begin,
    }
    return render(request, "health.html", context=context)
