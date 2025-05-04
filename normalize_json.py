import os
import json
import sys
import traceback
import base64

# Obter o caminho do arquivo de saída
output_path = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
# Obter o conteúdo Base64
base64_content = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64')

print(f"--- Python Script: Got output_path={output_path} ---", file=sys.stderr)
# Não imprimir o conteúdo base64 completo nos logs por segurança
print(f"--- Python Script: Got GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64 (length={len(base64_content) if base64_content else 0}) ---", file=sys.stderr)

if not output_path:
    print('ERROR: Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable in Python script', file=sys.stderr)
    sys.exit(1)
if not base64_content:
    print('ERROR: Missing GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64 environment variable in Python script', file=sys.stderr)
    sys.exit(1)

try:
    print(f"--- Python Script: Decoding Base64 content... ---", file=sys.stderr)
    # Decodificar Base64
    decoded_bytes = base64.b64decode(base64_content)
    decoded_json_string = decoded_bytes.decode('utf-8')
    print(f"--- Python Script: Base64 decoded successfully. ---", file=sys.stderr)

    print(f"--- Python Script: Writing decoded JSON to {output_path} ---", file=sys.stderr)
    # Escrever o JSON decodificado no arquivo
    with open(output_path, 'w', encoding='utf-8') as f_write:
        f_write.write(decoded_json_string)
    print(f"--- Python Script: Successfully wrote credentials to {output_path} ---", file=sys.stderr)

    # Opcional: Se a normalização ainda for necessária (provavelmente não mais)
    # print(f"--- Python Script: Attempting to normalize {output_path} ---", file=sys.stderr)
    # with open(output_path, 'r', encoding='utf-8') as f_read:
    #     data = json.load(f_read)
    # with open(output_path, 'w', encoding='utf-8') as f_norm_write:
    #     json.dump(data, f_norm_write)
    # print(f"--- Python Script: Normalization complete for {output_path} ---", file=sys.stderr)

except base64.binascii.Error as b64_error:
    print(f"ERROR: Base64 decoding failed: {b64_error}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"ERROR: Unexpected error in normalize_json.py processing: {e}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

# Se chegou aqui, tudo correu bem
sys.exit(0) 