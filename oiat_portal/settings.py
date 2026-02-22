"""Django settings for the OIAT portal."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def _env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if minimum is not None and value < minimum:
        return default
    return value

# Security: prefer env in production; dev default only when explicitly enabled
_SECRET_KEY = os.getenv("DJANGO_SECRET_KEY")
if _SECRET_KEY:
    SECRET_KEY = _SECRET_KEY
else:
    # Fallback for local dev only; do not use in production
    SECRET_KEY = os.getenv("OIAT_DEV_SECRET_KEY", "django-insecure-dev-only-change-in-production")

# Default DEBUG=True when unset so runserver works with no env (local dev). Set DJANGO_DEBUG=0 in production.
_debug_raw = os.getenv("DJANGO_DEBUG")
if _debug_raw is None:
    DEBUG = True
else:
    DEBUG = _debug_raw.lower() in ("1", "true", "yes")

_raw_hosts = os.getenv("DJANGO_ALLOWED_HOSTS", "").strip()
if _raw_hosts:
    ALLOWED_HOSTS = [h.strip() for h in _raw_hosts.split(",") if h.strip()]
elif DEBUG:
    ALLOWED_HOSTS = ["localhost", "127.0.0.1", "[::1]", "*"]
else:
    # DEBUG=False but no DJANGO_ALLOWED_HOSTS: allow runserver for local testing (production must set DJANGO_ALLOWED_HOSTS).
    ALLOWED_HOSTS = ["localhost", "127.0.0.1", "[::1]"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "apps.core",
    "apps.epos_qbo",
    "apps.dashboard",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "apps.core.middleware.LoginRequiredMiddleware",
]

ROOT_URLCONF = "oiat_portal.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                "apps.epos_qbo.context_processors.dashboard_timezone",
            ],
        },
    },
]

WSGI_APPLICATION = "oiat_portal.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LOGIN_URL = "/login/"
LOGIN_REDIRECT_URL = "/epos-qbo/dashboard/"
LOGOUT_REDIRECT_URL = "/login/"

# Portal workspace selector cards.
PORTAL_SOLUTIONS = [
    {
        "name": "EPOS -> QBO",
        "description": "Monitor runs, manage companies, and trigger sync jobs.",
        "url_name": "epos_qbo:overview",
    }
]

# Dashboard operational knobs (can be overridden via env vars).
OIAT_DASHBOARD_DEFAULT_PARALLEL = _env_int("OIAT_DASHBOARD_DEFAULT_PARALLEL", 2, minimum=1)
OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS = _env_int("OIAT_DASHBOARD_DEFAULT_STAGGER_SECONDS", 2, minimum=0)
OIAT_DASHBOARD_STALE_HOURS_WARNING = _env_int("OIAT_DASHBOARD_STALE_HOURS_WARNING", 48, minimum=1)
OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS = _env_int("OIAT_DASHBOARD_REFRESH_EXPIRING_DAYS", 7, minimum=1)
OIAT_DASHBOARD_REAUTH_GUIDANCE = os.getenv(
    "OIAT_DASHBOARD_REAUTH_GUIDANCE",
    "QBO re-authentication required. Run OAuth flow and store tokens using code_scripts/store_tokens.py.",
)
OIAT_DASHBOARD_RECON_DIFF_WARNING = _env_float("OIAT_DASHBOARD_RECON_DIFF_WARNING", 1.0, minimum=0)
# Timezone for dashboard "today" / "yesterday" (overview KPIs, Run Success, receipts uploaded, Quick Sync default).
# Set to match your scheduler (e.g. America/New_York). If unset, uses TIME_ZONE (UTC).
OIAT_DASHBOARD_TIMEZONE = os.getenv("OIAT_DASHBOARD_TIMEZONE", TIME_ZONE)

# Canonical business-day clock for overview KPIs and quick-sync defaults.
OIAT_BUSINESS_TIMEZONE = os.getenv("OIAT_BUSINESS_TIMEZONE", "Africa/Lagos")
OIAT_BUSINESS_DAY_CUTOFF_HOUR = _env_int("OIAT_BUSINESS_DAY_CUTOFF_HOUR", 5, minimum=0)
OIAT_BUSINESS_DAY_CUTOFF_MINUTE = _env_int("OIAT_BUSINESS_DAY_CUTOFF_MINUTE", 0, minimum=0)

# Production security (when DEBUG is False)
if not DEBUG:
    SECURE_BROWSER_XSS_FILTER = True
    SECURE_CONTENT_TYPE_NOSNIFF = True
    X_FRAME_OPTIONS = "DENY"
    CSRF_COOKIE_SECURE = True
    SESSION_COOKIE_SECURE = True
    SECURE_SSL_REDIRECT = os.getenv("DJANGO_SECURE_SSL_REDIRECT", "").lower() in ("1", "true", "yes")
    SECURE_HSTS_SECONDS = _env_int("DJANGO_SECURE_HSTS_SECONDS", 0, minimum=0)
    SECURE_HSTS_INCLUDE_SUBDOMAINS = SECURE_HSTS_SECONDS > 0
    SECURE_HSTS_PRELOAD = SECURE_HSTS_SECONDS > 0
