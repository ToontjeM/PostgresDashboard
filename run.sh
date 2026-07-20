#!/usr/bin/env bash
set -e
source .venv/bin/activate
PORT=${PORT:-5000}
echo "Starting PostgreSQL Dashboard at http://localhost:${PORT}"
exec .venv/bin/python app.py --port "$PORT"
deacivate
