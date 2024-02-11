from django.utils import timezone


# FIXME: How to make this the default?
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z"


def now_tzaware():
    return timezone.localtime(timezone.now())


def strftime(datetime):
    return datetime.strftime(DATETIME_FORMAT)
