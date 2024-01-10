from django.shortcuts import render
from webui import logic


def index(request):
    return render(request, "index.html")


def health(request):
    context = dict(fresh_stats=logic.compute_fresh_stats())
    return render(request, "health.html", context=context)
