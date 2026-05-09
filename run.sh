#!/bin/bash
set -e
cd "$(dirname "$0")"
pip install -q -r requirements.txt 2>/dev/null
exec python3 -m uvicorn app:app --host 0.0.0.0 --port 8088
