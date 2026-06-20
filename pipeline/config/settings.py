"""Django settings for the batch analytics pipeline."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "dev-insecure-change-me")
DEBUG = os.environ.get("DJANGO_DEBUG", "1") == "1"
ALLOWED_HOSTS = os.environ.get("DJANGO_ALLOWED_HOSTS", "*").split(",")

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.staticfiles",
    "rest_framework",
    "analytics",
]

MIDDLEWARE = [
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {"context_processors": []},
    },
]

WSGI_APPLICATION = "config.wsgi.application"

# --- PostgreSQL: the operational system of record ---
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("POSTGRES_DB", "analytics"),
        "USER": os.environ.get("POSTGRES_USER", "analytics"),
        "PASSWORD": os.environ.get("POSTGRES_PASSWORD", "analytics"),
        "HOST": os.environ.get("POSTGRES_HOST", "localhost"),
        "PORT": os.environ.get("POSTGRES_PORT", "5432"),
    }
}

# Allow tests to fall back to sqlite when no Postgres is reachable.
if os.environ.get("USE_SQLITE_FOR_TESTS") == "1":
    DATABASES["default"] = {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "test_db.sqlite3",
    }

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

STATIC_URL = "static/"
USE_TZ = True
TIME_ZONE = "UTC"

REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
    ],
    "UNAUTHENTICATED_USER": None,
}

# --- ClickHouse (analytics serving store) ---
CLICKHOUSE = {
    "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.environ.get("CLICKHOUSE_PORT", "8123")),
    "username": os.environ.get("CLICKHOUSE_USER", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    "database": os.environ.get("CLICKHOUSE_DB", "analytics"),
}

# --- S3 / LocalStack (Bronze data lake) ---
AWS = {
    "endpoint_url": os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566"),
    "access_key": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
    "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    "region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    "bronze_bucket": os.environ.get("S3_BRONZE_BUCKET", "bronze"),
    "firehose_stream": os.environ.get("FIREHOSE_DELIVERY_STREAM", "bronze-delivery"),
}

DATASET_DIR = os.environ.get("DATASET_DIR", str(BASE_DIR.parent / "dataset"))

# Currencies the business accepts. Anything else is a data-quality reject.
SUPPORTED_CURRENCIES = ["USD", "EUR", "GBP"]
REPORTING_CURRENCY = "USD"
