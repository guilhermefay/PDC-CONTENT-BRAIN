import os
import json
import sys
import traceback

path = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
print(f"--- Python Script: Got path={path} ---", file=sys.stderr)

if not path:
    print('ERROR: Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable in Python script', file=sys.stderr)
    sys.exit(1)

try:
    print(f"--- Python Script: Trying to open {path} for reading ---", file=sys.stderr)
    with open(path, 'r', encoding='utf-8') as f_read:
        print(f"--- Python Script: File {path} opened for reading. Trying json.load() ---", file=sys.stderr)
        # Tentar carregar diretamente do arquivo agora
        data = json.load(f_read)
        print(f"--- Python Script: json.load() successful ---", file=sys.stderr)

    print(f"--- Python Script: Trying to open {path} for writing ---", file=sys.stderr)
    with open(path, 'w', encoding='utf-8') as f_write:
        print(f"--- Python Script: File {path} opened for writing. Trying json.dump() ---", file=sys.stderr)
        json.dump(data, f_write, indent=2)
        print(f"--- Python Script: json.dump() successful ---", file=sys.stderr)

except json.JSONDecodeError as e_decode:
    print(f"ERROR: JSONDecodeError loading from {path}: {e_decode}", file=sys.stderr)
    print(f"Line {e_decode.lineno}, Column {e_decode.colno}: {e_decode.msg}", file=sys.stderr)
    # Se derro, tentar ler e imprimir o conteúdo problemático
    try:
        with open(path, 'r', encoding='utf-8') as f_prob:
            content = f_prob.read()
            print("\n--- Problematic File Content ---", file=sys.stderr)
            print(content, file=sys.stderr)
            print("--- End Problematic File Content ---", file=sys.stderr)
    except Exception as e_read_prob:
        print(f"ERROR: Could not even read the problematic file content: {e_read_prob}", file=sys.stderr)
    sys.exit(1)
except Exception as e_general:
    print(f"ERROR: Unexpected error in normalize_json.py processing {path}:", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

print(f"--- Python Script: Successfully normalized {path} ---", file=sys.stderr) 