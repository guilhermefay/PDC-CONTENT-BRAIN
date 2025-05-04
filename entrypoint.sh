#!/usr/bin/env sh
set -eu

# Decode Google service account JSON from Base64 and write to file
if [ -z "${GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64:-}" ]; then
  echo "ERROR: GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64 is not set" >&2
  exit 1
fi

echo "$GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64" | base64 -d > "$GOOGLE_SERVICE_ACCOUNT_JSON"
echo "--- Credentials written to $GOOGLE_SERVICE_ACCOUNT_JSON ---"

# Normalize JSON by calling the dedicated Python script
python3 normalize_json.py

echo "--- JSON normalization attempted via normalize_json.py ---"

# Execute ETL pipeline
exec python -m etl.annotate_and_index --source gdrive 