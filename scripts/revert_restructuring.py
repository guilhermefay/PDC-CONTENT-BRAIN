import os
import logging
import argparse
import sys
from datetime import datetime, timezone

from supabase import create_client, Client
from dotenv import load_dotenv

# --- Configuração de Logging ---
LOG_FILE_PATH = 'scripts/revert_restructuring.log'
# Remover handlers existentes para evitar duplicação
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuração Supabase ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Erro: SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem ser definidos no arquivo .env")
    sys.exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Cliente Supabase inicializado com sucesso.")
except Exception as e:
    logger.error(f"Erro ao inicializar cliente Supabase: {e}")
    sys.exit(1)

# --- Constantes --- 
DB_DELETE_BATCH_SIZE = 100
DB_UPDATE_BATCH_SIZE = 100

def revert_changes(supabase_client: Client, gdrive_id_to_revert: str, dry_run: bool = True):
    """
    Reverte as alterações de reestruturação para um gdrive_id específico.
    Deleta os chunks criados pelo processo (identificados por 'restructured_at')
    e reativa os chunks originais (identificados por 'obsolete_restructured').
    """
    logger.info(f"--- Iniciando reversão para gdrive_id: {gdrive_id_to_revert} (DRY-RUN={dry_run}) ---")

    # 1. Encontrar e deletar Novos Chunks (criados pela reestruturação)
    new_chunk_ids_to_delete = []
    offset = 0
    logger.info(f"Buscando chunks RECENTES (com 'restructured_at') para deletar...")
    while True:
        try:
            # Usar o operador '?' para checar a existência da chave 'restructured_at'
            # Infelizmente, supabase-py pode não suportar '?' diretamente no filtro.
            # Alternativa: Buscar todos e filtrar no cliente, ou usar .rpc() com SQL.
            # VAMOS TENTAR A BUSCA E FILTRO NO CLIENTE (menos eficiente):
            response = supabase_client.table('documents') \
                                     .select('document_id, metadata') \
                                     .eq('metadata->>gdrive_id', gdrive_id_to_revert) \
                                     .range(offset, offset + DB_UPDATE_BATCH_SIZE - 1) \
                                     .execute()

            if hasattr(response, 'data') and response.data:
                batch_ids = [item['document_id'] for item in response.data 
                             if 'restructured_at' in item.get('metadata', {})]
                if batch_ids:
                    new_chunk_ids_to_delete.extend(batch_ids)
                    logger.debug(f"Encontrados {len(batch_ids)} novos chunks neste lote (offset {offset}). Total até agora: {len(new_chunk_ids_to_delete)}")
                
                if len(response.data) < DB_UPDATE_BATCH_SIZE:
                    break # Último lote
                offset += DB_UPDATE_BATCH_SIZE
            else:
                if hasattr(response, 'error') and response.error:
                    logger.error(f"Erro Supabase ao buscar novos chunks (offset {offset}): {response.error}")
                break # Sai se não houver mais dados ou erro
        except Exception as e:
            logger.error(f"Exceção ao buscar novos chunks (offset {offset}): {e}")
            break

    logger.info(f"Total de {len(new_chunk_ids_to_delete)} novos chunks (com 'restructured_at') encontrados para deletar.")

    if not dry_run:
        if new_chunk_ids_to_delete:
            logger.info(f"[LIVE RUN] Deletando {len(new_chunk_ids_to_delete)} novos chunks em lotes...")
            for i in range(0, len(new_chunk_ids_to_delete), DB_DELETE_BATCH_SIZE):
                batch_ids = new_chunk_ids_to_delete[i:i + DB_DELETE_BATCH_SIZE]
                try:
                    logger.debug(f"Deletando lote {i // DB_DELETE_BATCH_SIZE + 1} de novos chunks...")
                    delete_response = supabase_client.table('documents').delete().in_('document_id', batch_ids).execute()
                    if hasattr(delete_response, 'error') and delete_response.error:
                        logger.error(f"Erro ao deletar lote de novos chunks (iniciando em {i}): {delete_response.error}")
                    else:
                        logger.info(f"Lote {i // DB_DELETE_BATCH_SIZE + 1} de novos chunks deletado com sucesso.")
                except Exception as e_delete:
                    logger.error(f"Exceção ao deletar lote de novos chunks (iniciando em {i}): {e_delete}")
        else:
            logger.info("[LIVE RUN] Nenhum novo chunk encontrado para deletar.")
    else:
        if new_chunk_ids_to_delete:
            logger.info(f"[DRY RUN] {len(new_chunk_ids_to_delete)} novos chunks seriam deletados.")
        else:
            logger.info("[DRY RUN] Nenhum novo chunk seria deletado.")

    # 2. Encontrar e reativar Chunks Antigos (marcados como obsoletos)
    old_chunk_ids_to_reactivate = []
    offset = 0
    logger.info(f"Buscando chunks ANTIGOS ('obsolete_restructured') para reativar...")
    while True:
        try:
            response = supabase_client.table('documents') \
                                     .select('document_id') \
                                     .eq('metadata->>gdrive_id', gdrive_id_to_revert) \
                                     .eq('indexing_status', 'obsolete_restructured') \
                                     .range(offset, offset + DB_UPDATE_BATCH_SIZE - 1) \
                                     .execute()

            if hasattr(response, 'data') and response.data:
                batch_ids = [item['document_id'] for item in response.data]
                old_chunk_ids_to_reactivate.extend(batch_ids)
                logger.debug(f"Encontrados {len(batch_ids)} chunks antigos neste lote (offset {offset}). Total até agora: {len(old_chunk_ids_to_reactivate)}")
                if len(response.data) < DB_UPDATE_BATCH_SIZE:
                    break # Último lote
                offset += DB_UPDATE_BATCH_SIZE
            else:
                if hasattr(response, 'error') and response.error:
                    logger.error(f"Erro Supabase ao buscar chunks antigos (offset {offset}): {response.error}")
                break # Sai se não houver mais dados ou erro
        except Exception as e:
            logger.error(f"Exceção ao buscar chunks antigos (offset {offset}): {e}")
            break

    logger.info(f"Total de {len(old_chunk_ids_to_reactivate)} chunks antigos ('obsolete_restructured') encontrados para reativar.")

    if not dry_run:
        if old_chunk_ids_to_reactivate:
            update_payload = {
                'keep': True, 
                'indexing_status': 'pending', 
                'annotation_status': 'pending', # Resetar anotação também
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            logger.info(f"[LIVE RUN] Reativando {len(old_chunk_ids_to_reactivate)} chunks antigos em lotes...")
            logger.info(f"Payload de reativação: {update_payload}")
            for i in range(0, len(old_chunk_ids_to_reactivate), DB_UPDATE_BATCH_SIZE):
                batch_ids = old_chunk_ids_to_reactivate[i:i + DB_UPDATE_BATCH_SIZE]
                try:
                    logger.debug(f"Reativando lote {i // DB_UPDATE_BATCH_SIZE + 1} de chunks antigos...")
                    update_response = supabase_client.table('documents') \
                                                      .update(update_payload) \
                                                      .in_('document_id', batch_ids) \
                                                      .execute()
                    if hasattr(update_response, 'error') and update_response.error:
                        logger.error(f"Erro ao reativar lote de chunks antigos (iniciando em {i}): {update_response.error}")
                    else:
                        logger.info(f"Lote {i // DB_UPDATE_BATCH_SIZE + 1} de chunks antigos reativado com sucesso.")
                except Exception as e_update:
                    logger.error(f"Exceção ao reativar lote de chunks antigos (iniciando em {i}): {e_update}")
        else:
             logger.info("[LIVE RUN] Nenhum chunk antigo encontrado para reativar.")
    else:
        if old_chunk_ids_to_reactivate:
            logger.info(f"[DRY RUN] {len(old_chunk_ids_to_reactivate)} chunks antigos seriam reativados (keep=True, status=pending).")
        else:
            logger.info("[DRY RUN] Nenhum chunk antigo seria reativado.")

    logger.info(f"--- Reversão para gdrive_id: {gdrive_id_to_revert} concluída --- ")

def main():
    parser = argparse.ArgumentParser(description="Reverte as alterações de reestruturação de chunks para um gdrive_id específico.")
    parser.add_argument('--gdrive-id', type=str, required=True,
                        help="O ID do documento original do Google Drive a ser revertido.")
    parser.add_argument('--dry-run', dest='dry_run', action='store_true',
                        help='Executa em modo simulação (não altera o banco - padrão)')
    parser.add_argument('--no-dry-run', dest='dry_run', action='store_false',
                        help='Executa de fato (altera o banco)')
    parser.set_defaults(dry_run=True)

    args = parser.parse_args()

    logger.info(f"[MAIN] Iniciando script de reversão com argumentos: {args}")

    if args.dry_run:
        logger.info("***** EXECUTANDO EM MODO DRY-RUN *****")
    else:
        logger.warning("***** EXECUTANDO EM MODO LIVE RUN - ALTERAÇÕES SERÃO FEITAS NO BANCO! *****")
        # Adicionar um pequeno delay para o usuário ler o aviso?
        # import time
        # time.sleep(3)

    try:
        revert_changes(
            supabase_client=supabase,
            gdrive_id_to_revert=args.gdrive_id,
            dry_run=args.dry_run
        )
    except Exception as e_main:
        logger.critical(f"[MAIN] Erro crítico não tratado durante a reversão: {e_main}", exc_info=True)

    logger.info("[MAIN] Script de reversão finalizado.")

if __name__ == "__main__":
    main() 