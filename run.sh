#!/bin/bash
set -e

# Load .env if present
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

if [ -z "$TINYFISH_API_KEY" ]; then
  echo "❌  Set TINYFISH_API_KEY in .env or as environment variable"
  exit 1
fi

echo "🚀  Starting ProcureIQ on http://localhost:8000"
python3 -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
