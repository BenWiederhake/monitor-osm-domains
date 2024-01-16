from django.db import models

class DigestionHealth(models.Model):
    digestion_begin = models.DateTimeField()
    digestion_end = models.DateTimeField()
    # https://docs.djangoproject.com/en/4.2/ref/databases/#sqlite-json1
    fresh_json = models.JSONField()
    expensive_json = models.JSONField()
