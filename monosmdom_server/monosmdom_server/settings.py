"""
Django settings for monosmdom_server project.

Generated by 'django-admin startproject' using Django 4.2.6.

For more information on this file, see
https://docs.djangoproject.com/en/4.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/4.2/ref/settings/
"""

from pathlib import Path
import secret_config


# Custom config
SECRET_ADMIN_PATH = secret_config.SECRET_ADMIN_PATH


# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Miscellaneous security settings
# https://django-secure.readthedocs.io/en/latest/settings.html
# https://docs.djangoproject.com/en/4.2/howto/deployment/checklist/
SECRET_KEY = secret_config.SECRET_KEY
DEBUG = secret_config.DEBUG
ALLOWED_HOSTS = secret_config.ALLOWED_HOSTS

SECURE_HSTS_SECONDS = secret_config.SECURE_HSTS_SECONDS
SECURE_HSTS_INCLUDE_SUBDOMAINS = secret_config.SECURE_HSTS_INCLUDE_SUBDOMAINS
SECURE_HSTS_PRELOAD = secret_config.SECURE_HSTS_PRELOAD
SECURE_CONTENT_TYPE_NOSNIFF = secret_config.SECURE_CONTENT_TYPE_NOSNIFF
SECURE_SSL_REDIRECT = secret_config.SECURE_SSL_REDIRECT
SESSION_COOKIE_SECURE = secret_config.SESSION_COOKIE_SECURE
CSRF_COOKIE_SECURE = secret_config.CSRF_COOKIE_SECURE
X_FRAME_OPTIONS = 'DENY'


# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "storage",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "monosmdom_server.urls"


def debug_processor_list():
    if secret_config.DEBUG:
        return ["django.template.context_processors.debug"]
    return []


TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                *debug_processor_list(),
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "monosmdom_server.wsgi.application"


# Database
# https://docs.djangoproject.com/en/4.2/ref/settings/#databases

if secret_config.DATABASE['NAME'] == 'MAGIC_SQLITE_FILE':
    secret_config.DATABASE['NAME'] = BASE_DIR / 'db.sqlite3'
DATABASES = {
    'default': secret_config.DATABASE,
}


# Password validation
# https://docs.djangoproject.com/en/4.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/4.2/topics/i18n/

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/4.2/howto/static-files/

STATIC_URL = "static/"

# Default primary key field type
# https://docs.djangoproject.com/en/4.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"