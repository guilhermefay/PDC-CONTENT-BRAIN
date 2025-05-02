import asyncio
import asyncpg
import os
import logging
from dotenv import load_dotenv

# Configurar logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_connection():
    """Carrega .env e tenta conectar ao Postgres via asyncpg."""
    logger.info("Carregando variáveis do arquivo .env...")
    # Tenta carregar .env do diretório atual
    loaded = load_dotenv(override=True, verbose=True) 
    if not loaded:
        logger.warning("Arquivo .env não encontrado no diretório atual.")
        # Tentar carregar do diretório pai se estivermos em um subdiretório comum
        parent_dotenv = os.path.join(os.path.dirname(__file__), '..', '.env')
        if os.path.exists(parent_dotenv):
            logger.info(f"Tentando carregar .env de {parent_dotenv}")
            loaded = load_dotenv(dotenv_path=parent_dotenv, override=True, verbose=True)
            if not loaded:
                 logger.error("Não foi possível carregar o .env.")
                 return
        else:
            logger.error("Não foi possível carregar o .env.")
            return
            
    host = os.getenv("R2R_POSTGRES_HOST")
    port = os.getenv("R2R_POSTGRES_PORT")
    dbname = os.getenv("R2R_POSTGRES_DBNAME")
    user = os.getenv("R2R_POSTGRES_USER")
    password = os.getenv("R2R_POSTGRES_PASSWORD")

    if not all([host, port, dbname, user, password]):
        logger.error("Uma ou mais variáveis R2R_POSTGRES_* não encontradas no ambiente após carregar .env.")
        logger.error(f"Host: {host}, Port: {port}, DB: {dbname}, User: {user}, Pass: {'*' * len(password) if password else None}")
        return

    logger.info(f"Tentando conectar a: postgresql://{user}:***@{host}:{port}/{dbname}")

    conn = None
    try:
        conn = await asyncpg.connect(
            user=user,
            password=password,
            database=dbname,
            host=host,
            port=int(port) # Port precisa ser inteiro
        )
        logger.info("Conexão bem-sucedida!")
        # Opcional: Fazer uma query simples
        # version = await conn.fetchval('SELECT version();')
        # logger.info(f"Versão do Postgres: {version}")

    except asyncpg.exceptions.InvalidPasswordError:
        logger.error("Erro de conexão: Senha inválida.")
    except ConnectionRefusedError:
         logger.error(f"Erro de conexão: Conexão recusada. Verifique se o host/porta ({host}:{port}) estão corretos e se há firewall.")
    except OSError as e:
         # Captura especificamente o erro de DNS/rede
         logger.error(f"Erro de OS (provavelmente DNS ou Rede) ao conectar: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado durante a conexão: {e}", exc_info=True)
    finally:
        if conn:
            await conn.close()
            logger.info("Conexão fechada.")

if __name__ == "__main__":
    asyncio.run(test_connection()) 