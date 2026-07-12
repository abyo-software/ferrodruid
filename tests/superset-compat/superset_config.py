# SPDX-License-Identifier: BUSL-1.1
# Copyright 2026 abyo software 合同会社 (abyo software LLC)
#
# Wave 33 — Superset configuration for the FerroDruid <-> Superset
# end-to-end test.
#
# Mounted into /app/pythonpath/superset_config.py inside the
# ferrodruid-superset container; Superset auto-loads any module
# named `superset_config` on its PYTHONPATH.

import os

# --- Metadata DB (Postgres, provided by docker-compose) -----------
DATABASE_USER = os.getenv("DATABASE_USER", "superset")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "superset")
DATABASE_HOST = os.getenv("DATABASE_HOST", "ferrodruid-superset-db")
DATABASE_PORT = os.getenv("DATABASE_PORT", "5432")
DATABASE_DB = os.getenv("DATABASE_DB", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}"
    f"@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DB}"
)

# --- Cache / async / Celery ---------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "ferrodruid-superset-redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "ferrodruid_superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": int(REDIS_PORT),
    "CACHE_REDIS_DB": 1,
}

# --- Security / app -----------------------------------------------
SECRET_KEY = os.getenv(
    "SUPERSET_SECRET_KEY",
    "ferrodruid-superset-wave33-test-secret-key-not-for-prod",
)

WTF_CSRF_ENABLED = True
# The Wave 33 harness signs in via /api/v1/security/login (JWT) and
# fetches a CSRF token via /api/v1/security/csrf_token/, so CSRF is
# enforced on session cookies but exempted on JWT-bearing API calls.
WTF_CSRF_EXEMPT_LIST = []

# Flask-Limiter requires either a backing store or an explicit
# disable to silence the bootstrap warning that shows up in the
# Wave 33 logs and makes triage harder.
RATELIMIT_ENABLED = False

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": False,
    "ENABLE_TEMPLATE_PROCESSING": False,
}
