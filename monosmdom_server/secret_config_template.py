SECRET_ADMIN_PATH = "siteadmin/"
SECRET_KEY = "INSECURE_REGENERATE_ME_I8cX7SaKl3HthaGroMnlDl2Bg5d4wpUsB5j33UqoYAC82brx9Pk_tEuGimuNIR0c"
DEBUG = True
ALLOWED_HOSTS = ["my.domain.example"]
CSRF_TRUSTED_ORIGINS = ["https://my.domain.example"]
AT_SUBPATH = "foo/bar"  # No slashes on either side
STATIC_ROOT = f"/var/html/www.yourdomain.tld/{AT_SUBPATH}/static"  # Yes leading, no trailing slash
STATIC_URL = f"/{AT_SUBPATH}/static/"  # Yes leading, yes trailing slash
MEDIA_ROOT = "/home/user/tmp/monosmdom_media/"
# MEDIA_URL is implicit
# FIXME: Throttle login requests

SECURE_HSTS_SECONDS = 0
SECURE_HSTS_INCLUDE_SUBDOMAINS = False
SECURE_HSTS_PRELOAD = False
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_SSL_REDIRECT = False
SESSION_COOKIE_SECURE = False
CSRF_COOKIE_SECURE = False

DATABASE_SQLITE = {
    'ENGINE': 'django.db.backends.sqlite3',
    'NAME': 'MAGIC_SQLITE_FILE',
    'USER': '(unused)',
    'PASSWORD': '(unused)',
}
DATABASE_POSTGRES = {
    'ENGINE': 'django.db.backends.postgresql',
    'NAME': 'monosmdom',
    # If you have set up postgres to allow for "local login", then the following are not necessary:
    # 'USER': 'mydatabaseuser',
    # 'PASSWORD': 'mypw',
    # 'HOST': '127.0.0.1',
    # 'PORT': '5432',
}
# Use this to easily switch back to SQLITE in case of some problems:
DATABASE = DATABASE_POSTGRES

# I suggest a weekly cronjob as described here: https://github.com/BenWiederhake/intermediate-ca-bundle#intermediate-ca-bundle
# Basically:
# $ curl --proto '=https' --tlsv1.2 -sSf -O 'https://raw.githubusercontent.com/BenWiederhake/intermediate-ca-bundle/blob/intermediate_certs.pem'
# $ cat /etc/ssl/certs/ca-certificates.crt intermediate_certs.pem > combined.pem
CAINFO_ROOT_AND_INTERMEDIATE = "/path/to/combined.pem"
# If your crawler runs amuck, you need to be reachable in some form. Please check regularly whether you can receive e-mail at this address!
CRAWLER_USERAGENT_EMAIL = ""firstname.lastname@gmail.com"
