SECRET_ADMIN_PATH = "siteadmin/"
SECRET_KEY = "I8cX7SaKl3HthaGroMnlDl2Bg5d4wpUsB5j33UqoYAC82brx9Pk_tEuGimuNIR0c"
DEBUG = True
ALLOWED_HOSTS = []

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
