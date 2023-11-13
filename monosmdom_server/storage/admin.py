from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from storage import models
import crawl

# TODO: Search?
# TODO: Filter?
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
def occ_to_editor_id_url(occ_obj):
    return f"https://www.openstreetmap.org/edit?{item_type_to_name(occ_obj.osm_item_type)}={occ_obj.osm_item_id}"


# TODO: This should live somewhere else.
def occ_to_editor_josm_url(occ_obj):
    return f"http://localhost:8111/load_object?objects={occ_obj.osm_item_type}{occ_obj.osm_item_id}"


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
    readonly_fields = ["content_url", "inspect"]
    fields = ["content_url", "inspect"]

    @admin.display(description="URL")
    def content_url(self, obj):
        return obj.url.url

    def inspect(self, obj):
        print(f"Inspect called on {obj}")
        return format_html("<a href={}>{}</a>", reverse("admin:storage_crawlableurl_change", args=(obj.url_id,)), str(obj))


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
    readonly_fields = ["editor_id_url", "editor_josm_url", "inspect", "osm_long", "osm_lat"]
    fields = ["editor_id_url", "editor_josm_url", "inspect", "osm_long", "osm_lat"]

    def inspect(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:storage_occurrenceinosm_change", args=(obj.id,)), str(obj))

    def editor_id_url(self, obj):
        osm_url = occ_to_editor_id_url(obj)
        return format_html("<a href={0}>{0}</a>", osm_url)

    def editor_josm_url(self, obj):
        osm_url = occ_to_editor_josm_url(obj)
        return format_html("<a href={0}>{0}</a>", osm_url)


class DisasterUrlInline(admin.TabularInline):
    model = models.DisasterUrl
    readonly_fields = ["reason"]
    fields = ["reason"]


class ResultInline(admin.TabularInline):
    model = crawl.models.Result
    readonly_fields = ["crawl_begin", "inspect"]
    fields = ["crawl_begin", "inspect"]

    def inspect(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:crawl_result_change", args=(obj.id,)), str(obj))


class ResultSuccessInline(admin.TabularInline):
    # TODO: "prefetch_related" might really help here.
    model = crawl.models.ResultSuccess
    readonly_fields = ["redir_from", "seen_on", "inspect"]
    fields = ["redir_from", "seen_on", "inspect"]
    verbose_name = "Redirect from"
    verbose_name_plural = "Redirects from"

    def redir_from(self, obj):
        source_url = obj.result.url
        return format_html("<a href={}>{}</a>", reverse("admin:storage_url_change", args=(source_url.id,)), str(source_url))

    def seen_on(self, obj):
        return obj.result.crawl_begin

    def inspect(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:crawl_resultsuccess_change", args=(obj.result_id,)), str(obj))


class UrlTypeFilter(admin.SimpleListFilter):
    title = "crawlability"
    parameter_name = "cr_p"  # Only used in the URL

    def lookups(self, _request, _model_admin):
        return [
            ("d", "Disastrous (and new)"),
            ("c", "Crawlable"),
            ("n", "Outdated / Ignored / Redirect"),
        ]

    def queryset(self, request, queryset):
        if self.value() == "d":
            return queryset.filter(
                disasterurl__isnull=False,
            )
        if self.value() == "c":
            return queryset.filter(
                crawlableurl__isnull=False,
            )
        if self.value() == "n":
            return queryset.filter(
                disasterurl__isnull=True,
                crawlableurl__isnull=True,
            )
        # Return None to indicate fallthrough


class UrlRedirInFilter(admin.SimpleListFilter):
    title = "incoming redirect"
    parameter_name = "in3"  # Only used in the URL

    def lookups(self, _request, _model_admin):
        return [
            ("y", "Is redirect target"),
            ("n", "Is not a target"),
        ]

    def queryset(self, request, queryset):
        # TODO: Don't consider "outdated" crawl.models.Result. What is "outdated"?
        if self.value() == "y":
            return queryset.filter(
                resultsuccess__isnull=False,
            ).distinct()
        if self.value() == "n":
            return queryset.filter(
                resultsuccess__isnull=True,
            ).distinct()
        # Return None to indicate fallthrough


class UrlRedirOutFilter(admin.SimpleListFilter):
    title = "outgoing redirect"
    parameter_name = "out3"  # Only used in the URL

    def lookups(self, _request, _model_admin):
        return [
            ("y", "Redirects elsewhere"),
            ("n", "Does not redirect"),
        ]

    def queryset(self, request, queryset):
        # TODO: Don't consider "outdated" crawl.models.Result. What is "outdated"?
        if self.value() == "y":
            return queryset.filter(
                result__resultsuccess__next_url__isnull=False,
            ).distinct()
        if self.value() == "n":
            return queryset.filter(
                result__resultsuccess__next_url__isnull=True,
            ).distinct()
        # Return None to indicate fallthrough


@admin.register(models.Url)
class UrlAdminForm(ReadOnlyModelAdmin):
    list_display = ["url"]
    readonly_fields = ["url"]
    inlines = [
        CrawlableUrlDomainInline,
        OsmOccurrenceInline,
        DisasterUrlInline,
        ResultInline,
        ResultSuccessInline,
    ]
    list_filter = [
        UrlTypeFilter,
        UrlRedirInFilter,
        UrlRedirOutFilter,
    ]

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

    @admin.display(ordering="url__url")
    def truncated_url(self, obj):
        return obj.url.truncated


@admin.register(models.CrawlableUrl)
class CrawlableUrlAdminForm(ReadOnlyModelAdmin):
    list_display = ["truncated_url", "domain"]
    readonly_fields = ["url", "domain"]

    @admin.display(ordering="url__url")
    def truncated_url(self, obj):
        return obj.url.truncated


@admin.register(models.OccurrenceInOsm)
class OccurrenceInOsmAdminForm(ReadOnlyModelAdmin):
    list_display = ["truncated_url", "osm_entry", "tag"]
    readonly_fields = ["url", "osm_item_type", "osm_item_id", "osm_tag_key", "osm_tag_value", "edit_in_id", "edit_in_josm"]
    # TODO: Link to OSM website

    @admin.display(ordering="url__url")
    def truncated_url(self, obj):
        return obj.url.truncated

    @admin.display(ordering="osm_item_id")
    def osm_entry(self, obj):
        return f"{obj.osm_item_type}{obj.osm_item_id}"

    @admin.display(ordering="osm_tag_key")
    def tag(self, obj):
        tag_value = obj.osm_tag_value
        if len(tag_value) > 20:
            tag_value = tag_value[:20 - 1] + "…"
        return f"{obj.osm_tag_key}={tag_value}"

    def edit_in_id(self, obj):
        osm_url = occ_to_editor_id_url(obj)
        return format_html("<a href={0}>{0}</a>", osm_url)

    def edit_in_josm(self, obj):
        osm_url = occ_to_editor_josm_url(obj)
        return format_html("<a href={0}>{0}</a>", osm_url)


@admin.register(models.Import)
class ImportAdminForm(ReadOnlyModelAdmin):
    list_display = ["import_begin", "urlfile_name"]
    readonly_fields = ["urlfile_name", "import_begin", "import_end", "import_duration", "additional_data"]

    def import_duration(self, obj):
        return obj.import_end - obj.import_begin
