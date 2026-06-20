"""Pytest bootstrap. Falls back to sqlite when no Postgres is configured/reachable."""
import os
import socket


def _postgres_reachable() -> bool:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    s = socket.socket()
    s.settimeout(1)
    try:
        s.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        s.close()


# Use sqlite for unit tests unless a real Postgres is available (e.g. in Docker).
if os.environ.get("USE_SQLITE_FOR_TESTS") is None and not _postgres_reachable():
    os.environ["USE_SQLITE_FOR_TESTS"] = "1"


def dataset_dir():
    return os.environ.get("DATASET_DIR", "/data/dataset")
