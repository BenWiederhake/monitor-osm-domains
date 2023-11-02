from django.contrib import admin
from crawl import models
import base64

# TODO: Search?
# TODO: Filter?
# TODO: Order?
# TODO: Optimize using list_select_related?


class ReadDeleteOnlyModelAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        # Imports/discoveries shouldn't go through the admin interface.
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(models.Result)
class ResultAdminForm(ReadDeleteOnlyModelAdmin):
    list_display = ["truncated_url", "crawl_begin"]
    readonly_fields = ["url", "crawl_begin", "crawl_end"]
    # TODO: Action: Remove completely
    # TODO: Action: Go to Error/Success
    # TODO: Info: show whether error/success exist

    def truncated_url(self, obj):
        return obj.url.truncated


@admin.register(models.ResultSuccess)
class ResultSuccessAdminForm(ReadDeleteOnlyModelAdmin):
    # TODO: Show parent more easily?
    # TODO: Action (conditional): Prune content
    list_display = ["truncated_url", "status_code", "content_orig_size"]
    # TODO: headers: Show / offer download
    # TODO: content: Show / offer download
    readonly_fields = ["status_code", "headers_storage_size", "headers_orig_size", "content_file", "content_orig_size", "next_url", "next_request"]

    def truncated_url(self, obj):
        return obj.result.url.truncated

    def crawl_begin(self, obj):
        return obj.result.crawl_begin

    def headers_storage_size(self, obj):
        return len(obj.headers_zlib)


@admin.register(models.ResultError)
class ResultErrorAdminForm(ReadDeleteOnlyModelAdmin):
    # TODO: Action (conditional): Prune content
    list_display = ["truncated_url", "is_internal_error", "crawl_begin"]
    readonly_fields = ["is_internal_error", "description_json", "description_json_b64", "show_traceback_code"]

    def truncated_url(self, obj):
        return obj.result.url.truncated

    def crawl_begin(self, obj):
        return obj.result.crawl_begin

    def description_json_b64(self, obj):
        return base64.b64encode(obj.description_json.encode())

    def show_traceback_code(self, obj):
        return "print(''.join(base64.b64decode(b64data).decode()['traceback']))"
