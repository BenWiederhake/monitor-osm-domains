from django.contrib import admin
from storage import models

admin.site.register(models.CrawlableUrl)
admin.site.register(models.CrawlResult)
admin.site.register(models.CrawlResultError)
admin.site.register(models.CrawlResultSuccess)
admin.site.register(models.DisasterUrl)
admin.site.register(models.Domain)
admin.site.register(models.OccurrenceInOsm)
admin.site.register(models.Url)
