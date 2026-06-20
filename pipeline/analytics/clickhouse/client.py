"""Thin helper around clickhouse-connect, configured from Django settings."""
from functools import lru_cache

from django.conf import settings


def get_client(database: str | None = None):
    """Return a ClickHouse client. Heavy import kept local so the app can be
    imported (and migrations run) without clickhouse-connect installed."""
    import clickhouse_connect

    cfg = settings.CLICKHOUSE
    return clickhouse_connect.get_client(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=database if database is not None else cfg["database"],
    )


@lru_cache(maxsize=1)
def _cached_client():
    return get_client()


def query(sql: str, parameters: dict | None = None):
    """Run a SELECT and return a clickhouse-connect QueryResult."""
    return _cached_client().query(sql, parameters=parameters or {})


def query_rows(sql: str, parameters: dict | None = None) -> list[dict]:
    """Run a SELECT and return a list of dict rows."""
    res = query(sql, parameters)
    cols = res.column_names
    return [dict(zip(cols, row)) for row in res.result_rows]
