from django.contrib import admin
from storage import models

# TODO: Search?
# TODO: Filter?
# TODO: Order?
# TODO: Optimize using list_select_related?


class ReadOnlyModelAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        # Imports/discoveries shouldn't go through the admin interface.
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        # Cleanups are either rarely necessary, or automatic (table wipe) during import.
        # Also, first check whether related objects still need this object.
        return False


@admin.register(models.Domain)
class DomainAdminForm(ReadOnlyModelAdmin):
    empty_value_display = "(never)"
    list_display = ["domain_name", "last_contacted"]
    readonly_fields = ["domain_name", "last_contacted"]
    # TODO: Action: Set last_contacted to "now"
    # TODO: Action: Clear last_contacted


@admin.register(models.Url)
class UrlAdminForm(ReadOnlyModelAdmin):
    list_display = ["url"]
    readonly_fields = ["url"]
    # TODO: Link to related objects (DisasterUrl, CrawlableUrl, query for CrawlResult, query for OccurrenceInOsm)
    # TODO: Link to OSM website

    def has_add_permission(self, request, obj=None):
        # Imports/discoveries shouldn't go through the admin interface.
        return False

    def has_delete_permission(self, request, obj=None):
        # Cleanups should rarely be necessary, and should first check whether related objects still need this object.
        return False


@admin.register(models.DisasterUrl)
class DisasterUrlAdminForm(ReadOnlyModelAdmin):
    list_display = ["truncated_url", "reason"]
    readonly_fields = ["url", "reason"]

    def truncated_url(self, obj):
        return obj.url.truncated


@admin.register(models.CrawlableUrl)
class CrawlableUrlAdminForm(ReadOnlyModelAdmin):
    list_display = ["truncated_url", "domain"]
    readonly_fields = ["url", "domain"]

    def truncated_url(self, obj):
        return obj.url.truncated


# admin.site.register(models.CrawlResult)
# admin.site.register(models.CrawlResultSuccess)
# admin.site.register(models.CrawlResultError)


@admin.register(models.OccurrenceInOsm)
class OccurrenceInOsmAdminForm(ReadOnlyModelAdmin):
    list_display = ["truncated_url", "osm_entry", "tag"]
    readonly_fields = ["url", "osm_item_type", "osm_item_id", "osm_tag_key", "osm_tag_value"]
    # TODO: Link to OSM website

    def truncated_url(self, obj):
        return obj.url.truncated

    def osm_entry(self, obj):
        return f"{obj.osm_item_type}{obj.osm_item_id}"

    def tag(self, obj):
        tag_value = obj.osm_tag_value
        if len(tag_value) > 20:
            tag_value = tag_value[:20 - 1] + "…"
        return f"{obj.osm_tag_key}={tag_value}"
