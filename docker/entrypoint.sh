#!/usr/bin/env bash
set -euo pipefail

# Wait for Postgres to accept connections before doing anything Django-ish.
echo "[entrypoint] waiting for postgres at ${POSTGRES_HOST}:${POSTGRES_PORT} ..."
until python -c "
import os, socket, sys
s = socket.socket()
s.settimeout(2)
try:
    s.connect((os.environ['POSTGRES_HOST'], int(os.environ['POSTGRES_PORT'])))
    s.close()
except Exception:
    sys.exit(1)
" 2>/dev/null; do
  sleep 1
done
echo "[entrypoint] postgres is up."

# Apply Django migrations automatically so the web service is ready to serve.
python manage.py migrate --noinput

exec "$@"
