#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create venv if missing
if [ ! -d .venv ]; then
  echo "Creating virtual environment…"
  python3 -m venv .venv
fi

# Install / upgrade dependencies inside the venv
.venv/bin/pip install -q -r requirements.txt

PORT=${PORT:-5000}
echo "Starting PostgreSQL Dashboard at http://localhost:${PORT}"
exec .venv/bin/python app.py --port "$PORT"
