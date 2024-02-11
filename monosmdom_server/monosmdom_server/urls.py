"""
URL configuration for monosmdom_server project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.conf import settings
from django.contrib import admin
from django.urls import include, path, re_path
import crawl.models
import crawl.views
import webui.views


STRIPPED_MEDIA_URL = settings.MEDIA_URL.strip("/")

urlpatterns = [
    re_path("assets/domains.png$", webui.views.serve_domains_png, name="domains.png"),
    # Pattern must match crawler.models.ResultSuccess.content_file.upload_to:
    re_path(f'^{STRIPPED_MEDIA_URL}/(?P<filepath>{crawl.models.USER_DIRECTORY_PATH_REGEX})$', crawl.views.serve_protected_media, name='serve_protected_media'),
    path(settings.AT_SUBPATH + "/" if settings.AT_SUBPATH else "", include([
        path("", webui.views.index, name="index"),
        path("health/", webui.views.health, name="webui_health"),
        path(settings.SECRET_ADMIN_PATH, admin.site.urls),
    ])),
]
