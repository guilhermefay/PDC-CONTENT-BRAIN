#!/usr/bin/env sh
set -eu

# Decode Google service account JSON from Base64 and write to file
if [ -z "${GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64:-}" ]; then
  echo "ERROR: GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64 is not set" >&2
  exit 1
fi

echo "$GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64" | base64 -d > "$GOOGLE_SERVICE_ACCOUNT_JSON"
echo "--- Credentials written to $GOOGLE_SERVICE_ACCOUNT_JSON ---"

# Normalize JSON to escape newlines properly using Python json.load/dump
python3 - <<'EOF'
import os, json, sys
path = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
if not path:
    sys.exit('ERROR: Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable')
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
with open(path, 'w', encoding='utf-8') as f:
    json.dump(data, f)
EOF

echo "--- Credentials JSON normalized with escaped newlines in $GOOGLE_SERVICE_ACCOUNT_JSON ---"

# Execute ETL pipeline
exec python -m etl.annotate_and_index --source gdrive 