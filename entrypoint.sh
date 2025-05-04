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
import os, json, sys, traceback

path = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
print(f"--- Python: Got path={path} ---", file=sys.stderr)

if not path:
    print('ERROR: Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable in Python', file=sys.stderr)
    sys.exit(1)

try:
    print(f"--- Python: Trying to open {path} for reading ---", file=sys.stderr)
    with open(path, 'r', encoding='utf-8') as f_read:
        print(f"--- Python: File {path} opened for reading. ---", file=sys.stderr)
        # Read content first to check
        try:
            content = f_read.read()
            print(f"--- Python: Read {len(content)} chars from {path}. Snippet: ---", file=sys.stderr)
            print(content[:200] + ('...' if len(content) > 200 else ''), file=sys.stderr)
        except Exception as e_read:
            print(f"ERROR: Could not read content from {path}: {e_read}", file=sys.stderr)
            sys.exit(1)

        print(f"--- Python: Trying json.loads() on content from {path} ---", file=sys.stderr)
        # Load from the string content instead of the file object directly
        data = json.loads(content)
        print(f"--- Python: json.loads() successful ---", file=sys.stderr)

    print(f"--- Python: Trying to open {path} for writing ---", file=sys.stderr)
    with open(path, 'w', encoding='utf-8') as f_write:
        print(f"--- Python: File {path} opened for writing. Trying json.dump() ---", file=sys.stderr)
        json.dump(data, f_write, indent=2) # Add indent for readability
        print(f"--- Python: json.dump() successful ---", file=sys.stderr)

except json.JSONDecodeError as e_decode:
    print(f"ERROR: JSONDecodeError loading content from {path}: {e_decode}", file=sys.stderr)
    print(f"Line {e_decode.lineno}, Column {e_decode.colno}: {e_decode.msg}", file=sys.stderr)
    print("
--- Problematic File Content ---", file=sys.stderr)
    print(content, file=sys.stderr) # Print the full content read earlier
    print("--- End Problematic File Content ---", file=sys.stderr)
    sys.exit(1)
except Exception as e_general:
    print(f"ERROR: Unexpected error processing {path}:", file=sys.stderr)
    traceback.print_exc(file=sys.stderr) # Print full traceback for any other error
    sys.exit(1)
EOF

echo "--- Credentials JSON normalized with escaped newlines in $GOOGLE_SERVICE_ACCOUNT_JSON ---"

# Execute ETL pipeline
exec python -m etl.annotate_and_index --source gdrive 