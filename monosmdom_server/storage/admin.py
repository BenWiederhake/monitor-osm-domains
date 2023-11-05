from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from storage import models

# TODO: Search?
# TODO: Filter?
# TODO: Order?
# TODO: Optimize using list_select_related?


# TODO: This should live somewhere else.
def item_type_to_name(item_type_char):
    if item_type_char == "n":
        return "node"
    elif item_type_char == "w":
        return "way"
    elif item_type_char == "r":
        return "relation"
    else:
        return f"ERROR{item_type_char}"


# TODO: This should live somewhere else.
def occ_to_editor_url(occ_obj):
    return f"https://www.openstreetmap.org/edit?{item_type_to_name(occ_obj.osm_item_type)}={occ_obj.osm_item_id}"


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


class CrawlableUrlSelfInline(admin.TabularInline):
    model = models.CrawlableUrl
    readonly_fields = ["content_url", "crurl"]
    fields = ["content_url", "crurl"]

    @admin.display(description="URL")
    def content_url(self, obj):
        return obj.url.url

    def crurl(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:storage_crawlableurl_change", args=(obj.id,)), str(obj))


@admin.register(models.Domain)
class DomainAdminForm(ReadOnlyModelAdmin):
    empty_value_display = "(never)"
    list_display = ["domain_name", "last_contacted"]
    readonly_fields = ["domain_name", "last_contacted"]
    # TODO: Action: Set last_contacted to "now"
    # TODO: Action: Clear last_contacted

    inlines = [
        CrawlableUrlSelfInline,
    ]


class CrawlableUrlDomainInline(admin.TabularInline):
    model = models.CrawlableUrl
    readonly_fields = ["domain"]
    fields = ["domain"]


class OsmOccurrenceInline(admin.TabularInline):
    model = models.OccurrenceInOsm
    readonly_fields = ["editor_url", "occ"]
    fields = ["editor_url", "occ"]

    def occ(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:storage_occurrenceinosm_change", args=(obj.id,)), str(obj))

    def editor_url(self, obj):
        osm_url = occ_to_editor_url(obj)
        return format_html("<a href={0}>{0}</a>", osm_url)


class DisasterUrlInline(admin.TabularInline):
    model = models.DisasterUrl
    readonly_fields = ["reason"]
    fields = ["reason"]


@admin.register(models.Url)
class UrlAdminForm(ReadOnlyModelAdmin):
    list_display = ["url"]
    readonly_fields = ["url"]
    inlines = [
        CrawlableUrlDomainInline,
        OsmOccurrenceInline,
        DisasterUrlInline,
    ]
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


@admin.register(models.OccurrenceInOsm)
class OccurrenceInOsmAdminForm(ReadOnlyModelAdmin):
    list_display = ["truncated_url", "osm_entry", "tag"]
    readonly_fields = ["url", "osm_item_type", "osm_item_id", "osm_tag_key", "osm_tag_value", "edit_on_osm"]
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

    def edit_on_osm(self, obj):
        osm_url = occ_to_editor_url(obj)
        return format_html("<a href={0}>{0}</a>", osm_url)


@admin.register(models.Import)
class ImportAdminForm(ReadOnlyModelAdmin):
    list_display = ["import_begin", "urlfile_name"]
    readonly_fields = ["urlfile_name", "import_begin", "import_end", "import_duration", "additional_data"]

    def import_duration(self, obj):
        return obj.import_end - obj.import_begin
