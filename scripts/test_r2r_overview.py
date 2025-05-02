import os
import logging
import uuid
from dotenv import load_dotenv
from infra.r2r_client import R2RClientWrapper
import json

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

# Define a query de busca (não usada diretamente, mas mantida)
SEARCH_QUERY = "qual o conteúdo do documento de teste simples?"
# Define o ID específico para filtrar
TARGET_DOC_ID = "67118294-1b14-5b07-ab67-ed588a8ba556"

def main():
    """Inicializa o R2RClientWrapper e realiza uma busca filtrada por document_id."""
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Initialize the client wrapper
    base_url = os.getenv("R2R_BASE_URL")
    logger.info(f"Inicializando R2RClientWrapper (usará R2R_BASE_URL='{base_url}' do ambiente)")
    try:
        r2r_client = R2RClientWrapper()
        logger.info(f"R2RClientWrapper initialized for base URL: {r2r_client.base_url}")
    except Exception as e:
        logging.error(f"Erro ao inicializar R2RClientWrapper: {e}")
        return

    # Testar a busca por documento específico
    try:
        # Criar filtro específico para document_id
        specific_filters = {"document_id": {"$eq": TARGET_DOC_ID}}
        logger.info(f"Chamando search com query='{SEARCH_QUERY}' e filtros={specific_filters}...")
        # A query pode ser genérica, o filtro é o importante
        search_results = r2r_client.search(query=SEARCH_QUERY, limit=5, filters=specific_filters)

        if search_results and search_results.get("success"):
            results = search_results.get("results", [])
            logging.info(f"Busca por document_id={TARGET_DOC_ID} retornou {len(results)} resultado(s).")
            # Comentar prints de saída
            # print("--- Resultados da Busca por ID ---")
            # print(json.dumps(search_results, indent=2, ensure_ascii=False))
            # print("-----------------------------")
        else:
            logging.error(f"Busca por document_id={TARGET_DOC_ID} falhou.")
            # Comentar prints de saída
            # print("--- Erro na Busca por ID ---")
            # print(json.dumps(search_results, indent=2, ensure_ascii=False))
            # print("-------------------------")

    except Exception as e:
        logging.error(f"Erro durante a execução da busca: {e}", exc_info=True)

if __name__ == "__main__":
    main() 