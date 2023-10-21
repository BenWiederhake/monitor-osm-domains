import datetime
from django.utils import timezone


def now_tzaware():
    return timezone.localtime(timezone.now())
