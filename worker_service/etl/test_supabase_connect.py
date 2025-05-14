# worker_service/etl/test_supabase_connect.py
import httpx
import asyncio
import os
import sys # Importar sys para o logger
import logging
import traceback # Adicionado para melhor log de exceção

# Configurar logging para ver detalhes do httpx/httpcore
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s",
    stream=sys.stdout, # Garantir que o log vá para stdout
)
logger = logging.getLogger("test_connect")
# Silenciar um pouco o hpack que é muito verboso, a menos que precisemos dele especificamente
logging.getLogger("hpack.hpack").setLevel(logging.WARNING)
logging.getLogger("httpcore.http2").setLevel(logging.INFO) # INFO para httpcore.http2 pode ser útil
logging.getLogger("httpcore.connection").setLevel(logging.INFO)


print("--- SCRIPT test_supabase_connect.py INICIADO ---", flush=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRO: SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados no ambiente!", flush=True)
    logger.error("SUPABASE_URL ou SUPABASE_SERVICE_KEY não configurados no ambiente!")
    exit(1)

# Testar o endpoint base do PostgREST, que deve retornar as definições da API
endpoint_path = "/rest/v1/" 
TARGET_URL = f"{SUPABASE_URL}{endpoint_path}"

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}", 
}

async def run_test(version_label: str, use_http2: bool):
    print(f"--- INICIANDO TESTE: {version_label} (HTTP/2={use_http2}) ---", flush=True)
    logger.info(f"Iniciando teste: {version_label} (HTTP/2={use_http2}) para {TARGET_URL}")
    try:
        timeout_config = httpx.Timeout(30.0, connect=10.0) 
        async with httpx.AsyncClient(http2=use_http2, timeout=timeout_config) as client:
            print(f"Cliente {version_label} criado. Headers para envio: {headers}", flush=True)
            logger.debug(f"Cliente {version_label} (http2={use_http2}) criado. Headers para envio: {headers}")
            
            print(f"Fazendo GET para: {TARGET_URL}", flush=True)
            logger.info(f"Fazendo GET para: {TARGET_URL} com http2={use_http2}")
            
            response = await client.get(TARGET_URL, headers=headers)
            
            print(f"Status da Resposta ({version_label}): {response.status_code}", flush=True)
            logger.info(f"Status da Resposta ({version_label}): {response.status_code}")
            
            response_text_bytes = await response.aread() # Ler o corpo da resposta como bytes
            response_text = response_text_bytes.decode('utf-8', errors='replace') # Decodificar para string

            if 200 <= response.status_code < 300:
                print(f"Corpo da Resposta ({version_label}) (primeiros 500 chars): {response_text[:500]}...", flush=True)
                logger.info(f"Corpo da Resposta ({version_label}): {response_text[:500]}...")
            else:
                print(f"Corpo da Resposta ({version_label}) (erro): {response_text}", flush=True)
                logger.warning(f"Corpo da Resposta ({version_label}) (erro): {response_text}")
            
            response.raise_for_status() 
        print(f"--- TESTE {version_label} CONCLUÍDO COM SUCESSO ---", flush=True)
        logger.info(f"TESTE {version_label} CONCLUÍDO COM SUCESSO")
    except httpx.HTTPStatusError as e:
        response_body_on_error_bytes = await e.response.aread()
        response_body_on_error = response_body_on_error_bytes.decode('utf-8', errors='replace')
        print(f"ERRO HTTPStatusError ({version_label}): {e.response.status_code} - {response_body_on_error}", flush=True)
        logger.error(f"ERRO HTTPStatusError ({version_label}): {e.response.status_code} - {response_body_on_error}", exc_info=False)
        logger.debug(f"Detalhes da exceção HTTPStatusError: {e}", exc_info=True)
    except httpx.RequestError as e:
        print(f"ERRO RequestError ({version_label}): {type(e)} - {e}", flush=True)
        logger.error(f"ERRO RequestError ({version_label}): {type(e)} - {e}", exc_info=False)
        logger.debug(f"Request que falhou: {e.request}", exc_info=True)
    except Exception as e:
        print(f"ERRO INESPERADO ({version_label}): {type(e)} - {e}", flush=True)
        logger.error(f"ERRO INESPERADO ({version_label}): {type(e)} - {e}", exc_info=True)
    finally:
        print(f"--- FIM DO TESTE: {version_label} ---", flush=True)
        logger.info(f"FIM DO TESTE: {version_label}")

async def main():
    print(f"URL Supabase a ser usada: {SUPABASE_URL}", flush=True)
    logger.info(f"URL Supabase a ser usada: {SUPABASE_URL}")
    print(f"Service Key (primeiros 5 chars): {SUPABASE_KEY[:5]}...", flush=True)
    logger.info(f"Service Key (primeiros 5 chars): {SUPABASE_KEY[:5]}...")
    
    await run_test("HTTP/2 (padrão)", use_http2=True)
    
    print("\n--- Pausa de 2 segundos antes do próximo teste ---\n", flush=True)
    logger.info("Pausa de 2 segundos antes do próximo teste")
    await asyncio.sleep(2)

    await run_test("HTTP/1.1 (forçado)", use_http2=False)

    print("--- SCRIPT DE TESTE FINALIZADO ---", flush=True)
    logger.info("SCRIPT DE TESTE FINALIZADO")

if __name__ == "__main__":
    asyncio.run(main()) 