#from django.shortcuts import render
from crawl import models
from django.contrib.auth.decorators import login_required
from django.http import FileResponse
from django.shortcuts import get_object_or_404
import os.path


@login_required()  # TODO: This means everyone with a login, not only admins, can access this.
def serve_protected_media(_request, filepath):
    result_success = get_object_or_404(models.ResultSuccess, content_file=filepath)

    _, file_name = os.path.split(filepath)

    response = FileResponse(result_success.content_file,)
    response["Content-Disposition"] = "attachment; filename=" + file_name

    return response
