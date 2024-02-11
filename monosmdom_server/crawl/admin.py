from crawl import models
from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
import base64

# TODO: Search?
# TODO: Filter?
# TODO: Optimize using list_select_related?


class ReadDeleteOnlyModelAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        # Imports/discoveries shouldn't go through the admin interface.
        return False

    def has_change_permission(self, request, obj=None):
        return False


class ResultSuccessRedirectInline(admin.TabularInline):
    model = models.ResultSuccess
    fk_name = "next_request"
    readonly_fields = ["cause"]
    fields = ["cause"]
    verbose_name = "Redirect from"
    verbose_name_plural = "Redirects from"

    def cause(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:crawl_resultsuccess_change", args=(obj.result_id,)), str(obj))


class ResultSuccessInline(admin.TabularInline):
    model = models.ResultSuccess
    fk_name = "result"
    readonly_fields = ["inspect"]
    fields = ["inspect"]
    verbose_name = "Success"
    verbose_name_plural = "Success"

    def inspect(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:crawl_resultsuccess_change", args=(obj.result_id,)), str(obj))


class ResultErrorInline(admin.TabularInline):
    model = models.ResultError
    readonly_fields = ["inspect"]
    fields = ["inspect"]
    verbose_name = "Error"
    verbose_name_plural = "Error"

    def inspect(self, obj):
        return format_html("<a href={}>{}</a>", reverse("admin:crawl_resulterror_change", args=(obj.result_id,)), str(obj))


class ResultMissingEndFilter(admin.SimpleListFilter):
    title = "crawl_end"
    parameter_name = "crend"  # Only used in the URL

    def lookups(self, _request, _model_admin):
        return [
            ("n", "Missing"),
        ]

    def queryset(self, request, queryset):
        if self.value() == "n":
            return queryset.filter(
                crawl_end__isnull=True,
            )
        # Return None to indicate fallthrough


class ResultTypeFilter(admin.SimpleListFilter):
    title = "outcome"
    parameter_name = "ty"  # Only used in the URL

    def lookups(self, _request, _model_admin):
        return [
            ("s", "Success"),
            ("e", "Error"),
            ("n", "Missing"),
        ]

    def queryset(self, request, queryset):
        if self.value() == "s":
            return queryset.filter(
                resultsuccess__isnull=False,
            )
        if self.value() == "e":
            return queryset.filter(
                resulterror__isnull=False,
            )
        if self.value() == "n":
            return queryset.filter(
                resultsuccess__isnull=True,
                resulterror__isnull=True,
            )
        # Return None to indicate fallthrough


@admin.register(models.Result)
class ResultAdminForm(ReadDeleteOnlyModelAdmin):
    list_display = ["truncated_url", "crawl_begin"]
    readonly_fields = ["url", "crawl_begin", "crawl_end"]
    inlines = [
        ResultSuccessInline,
        ResultErrorInline,
        ResultSuccessRedirectInline,
    ]
    list_filter = [
        ResultMissingEndFilter,
        ResultTypeFilter,
    ]
    # TODO: Action: Remove completely
    # TODO: Action: Go to Error/Success
    # TODO: Info: show whether error/success exist

    @admin.display(ordering="url__url")
    def truncated_url(self, obj):
        return obj.url.truncated


@admin.register(models.ResultSuccess)
class ResultSuccessAdminForm(ReadDeleteOnlyModelAdmin):
    # TODO: Show parent more easily?
    # TODO: Action (conditional): Prune content
    list_display = ["truncated_url", "status_code", "content_orig_size", "crawl_begin"]
    # TODO: headers: Show / offer download
    # TODO: content: Show / offer download
    readonly_fields = ["status_code", "headers_storage_size", "headers_orig_size", "content_file", "content_orig_size", "next_url", "next_request"]

    @admin.display(ordering="result__url__url")
    def truncated_url(self, obj):
        return obj.result.url.truncated

    @admin.display(ordering="result__crawl_begin")
    def crawl_begin(self, obj):
        return obj.result.crawl_begin

    def headers_storage_size(self, obj):
        return len(obj.headers)


@admin.register(models.ResultError)
class ResultErrorAdminForm(ReadDeleteOnlyModelAdmin):
    # TODO: Action (conditional): Prune content
    list_display = ["truncated_url", "is_internal_error", "crawl_begin"]
    readonly_fields = ["is_internal_error", "description_json", "description_json_b64", "show_traceback_code"]

    @admin.display(ordering="result__url__url")
    def truncated_url(self, obj):
        return obj.result.url.truncated

    @admin.display(ordering="result__crawl_begin")
    def crawl_begin(self, obj):
        return obj.result.crawl_begin

    def description_json_b64(self, obj):
        return base64.b64encode(obj.description_json.encode())

    def show_traceback_code(self, obj):
        return "print(''.join(base64.b64decode(b64data).decode()['traceback']))"


@admin.register(models.SquatProof)
class SquatProofAdminForm(ReadDeleteOnlyModelAdmin):
    list_display = ["crawl_begin", "truncated_url", "squatter"]
    readonly_fields = ["crawl_begin", "truncated_url", "squatter"]

    @admin.display(ordering="evidence__result__url__url")
    def truncated_url(self, obj):
        return obj.evidence.result.url.truncated

    @admin.display(ordering="evidence__result__crawl_begin")
    def crawl_begin(self, obj):
        return obj.evidence.result.crawl_begin
