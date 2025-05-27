# worker_service/etl/test_supabase_connect.py
import httpx
import asyncio
import os
import sys
import logging
import traceback
from datetime import datetime, timezone # Adicionado para timestamp no log

LOG_FILE_PATH = "/app/test_run_output.txt"

# Limpar o arquivo de log no início de cada execução do script
if os.path.exists(LOG_FILE_PATH):
    os.remove(LOG_FILE_PATH)
# Criar o arquivo para garantir que ele exista com as permissões corretas se o script rodar como não-root
# No entanto, dentro do Docker, o usuário padrão costuma ser root.
try:
    with open(LOG_FILE_PATH, "a") as f:
        f.write("") # Apenas para tocar o arquivo
except Exception as e:
    print(f"AVISO: Não foi possível criar/tocar o arquivo de log {LOG_FILE_PATH} inicialmente: {e}", flush=True)


def log_output(message: str, level: str = "INFO"):
    timestamp = datetime.now(timezone.utc).isoformat()
    # Remover os caracteres de escape ANSI das mensagens do logger antes de escrever no arquivo
    # AnsiEscapes.strip_ansi(msg)
    # Mas como 'message' aqui é uma string pura, não precisamos nos preocupar com isso ainda.
    full_message_for_file = f"{timestamp} - {level} - test_connect - {message}"
    full_message_for_print = f"{timestamp} - {level} - test_connect - {message}" # Pode ser diferente se quisermos formatar print
    
    try:
        with open(LOG_FILE_PATH, "a") as f:
            f.write(full_message_for_file + "\n")
    except Exception as e:
        # Se não puder escrever no arquivo, tente printar o erro do log no arquivo e no stdout
        print(f"ERRO CRÍTICO AO LOGAR: Não foi possível escrever em {LOG_FILE_PATH}: {e}", flush=True)
        try:
            with open(LOG_FILE_PATH, "a") as f: # Tentar novamente, pode ser problema de permissão intermitente
                f.write(f"{datetime.now(timezone.utc).isoformat()} - ERROR - test_connect - CRITICAL LOGGING ERROR: Failed to write to log file: {e}\n")
        except:
            pass 
    
    print(full_message_for_print, flush=True)

# Configurar o logger do Python
# Remover handlers existentes para evitar duplicação se o script for re-importado
root_logger = logging.getLogger()
if root_logger.hasHandlers():
    root_logger.handlers.clear()

# Adicionar nosso handler customizado
class FileAndPrintHandler(logging.Handler):
    def emit(self, record):
        # Formatar a mensagem do log
        log_entry = self.format(record)
        # Usar nossa função log_output para escrever no arquivo e printar
        # Passar o nome do logger e lineno para similaridade com o formato original
        # Adicionar traceback se existir
        msg_to_log = f"[{record.name}:{record.lineno}] {log_entry}"
        if record.exc_info:
            msg_to_log += "\n" + "".join(traceback.format_exception(record.exc_info[0], record.exc_info[1], record.exc_info[2]))
        log_output(msg_to_log, level=record.levelname)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(message)s", # O handler customizado já inclui timestamp, level, etc.
    handlers=[FileAndPrintHandler()]
)

# Níveis de log para bibliotecas
logging.getLogger("hpack").setLevel(logging.WARNING) 
logging.getLogger("httpcore").setLevel(logging.DEBUG) # Mais verboso para httpcore
logging.getLogger("httpx").setLevel(logging.DEBUG) # Mais verboso para httpx

log_output("--- SCRIPT test_supabase_connect.py INICIADO ---")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    log_output("ERRO: SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados no ambiente!", level="ERROR")
    exit(1)

endpoint_path = "/rest/v1/" 
TARGET_URL = f"{SUPABASE_URL}{endpoint_path}"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}", 
}

async def run_test(version_label: str, use_http2: bool):
    log_output(f"--- INICIANDO TESTE: {version_label} (HTTP/2={use_http2}) ---")
    try:
        timeout_config = httpx.Timeout(30.0, connect=10.0) 
        async with httpx.AsyncClient(http2=use_http2, timeout=timeout_config) as client:
            log_output(f"Cliente {version_label} criado. Headers para envio: {headers}")
            log_output(f"Fazendo GET para: {TARGET_URL} com http2={use_http2}")
            
            response = await client.get(TARGET_URL, headers=headers)
            
            log_output(f"Status da Resposta ({version_label}): {response.status_code}")
            
            response_text_bytes = await response.aread()
            response_text = response_text_bytes.decode('utf-8', errors='replace')

            if 200 <= response.status_code < 300:
                log_output(f"Corpo da Resposta ({version_label}) (primeiros 500 chars): {response_text[:500]}...")
            else:
                log_output(f"Corpo da Resposta ({version_label}) (erro): {response_text}", level="WARNING")
            
            response.raise_for_status() 
        log_output(f"--- TESTE {version_label} CONCLUÍDO COM SUCESSO ---")
    except httpx.HTTPStatusError as e:
        response_body_on_error_bytes = await e.response.aread()
        response_body_on_error = response_body_on_error_bytes.decode('utf-8', errors='replace')
        log_output(f"ERRO HTTPStatusError ({version_label}): {e.response.status_code} - {response_body_on_error}", level="ERROR")
        # O logger padrão já incluirá o traceback se exc_info=True, mas aqui estamos usando nossa função customizada.
        # Para ter o traceback no arquivo, precisamos formatá-lo e passá-lo.
        # O handler customizado agora faz isso se record.exc_info estiver presente.
        logging.getLogger("test_connect").error(f"Exceção HTTPStatusError em {version_label}", exc_info=True) # Log com traceback
    except httpx.RequestError as e:
        log_output(f"ERRO RequestError ({version_label}): {type(e)} - {e}", level="ERROR")
        log_output(f"Request que falhou: {e.request}", level="DEBUG")
        logging.getLogger("test_connect").error(f"Exceção RequestError em {version_label}", exc_info=True)
    except Exception as e:
        log_output(f"ERRO INESPERADO ({version_label}): {type(e)} - {e}", level="ERROR")
        logging.getLogger("test_connect").error(f"Exceção Inesperada em {version_label}", exc_info=True)
    finally:
        log_output(f"--- FIM DO TESTE: {version_label} ---")

async def main():
    log_output(f"URL Supabase a ser usada: {SUPABASE_URL}")
    log_output(f"Service Key (primeiros 5 chars): {SUPABASE_KEY[:5]}...")
    
    await run_test("HTTP/2 (padrão)", use_http2=True)
    
    log_output("\n--- Pausa de 2 segundos antes do próximo teste ---\n")
    await asyncio.sleep(2)

    await run_test("HTTP/1.1 (forçado)", use_http2=False)

    log_output("--- SCRIPT DE TESTE FINALIZADO ---")

if __name__ == "__main__":
    asyncio.run(main()) 