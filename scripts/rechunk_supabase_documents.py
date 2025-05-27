import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
import logging
import argparse
from supabase import create_client, Client
# from ingestion.gdrive_ingest import authenticate_gdrive
import uuid
from dotenv import load_dotenv
from googleapiclient.errors import HttpError
from datetime import datetime, timezone
import re # Adicionado para get_source_type_from_folder_info
import time # Adicionado para timestamp

# Configuração de logging O MAIS CEDO POSSÍVEL
LOG_FILE_PATH = 'scripts/enrich_metadata.log'
# Remover handlers existentes para evitar duplicação se o script for re-executado no mesmo processo
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.DEBUG, # 1. Nível de Log alterado para DEBUG
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='w'), # 'w' para sobrescrever a cada run
        logging.StreamHandler() # Para console
    ]
)

logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
# 2. Uso de SUPABASE_SERVICE_ROLE_KEY confirmado
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
# 1. Restaurando BATCH_SIZE
BATCH_SIZE = 100 # Para teste de depuração -> Restaurado para 100
CHUNK_TOKEN_LIMIT = 2000
# MAX_LIVE_TEST_BATCHES = 1 # Mantido comentado para execução completa

assert SUPABASE_URL and SUPABASE_KEY, "SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem estar definidos no .env"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# REGRAS DE MAPEAMENTO: Caminho da Pasta (ou nome) -> source_type
# A ordem importa (mais específico primeiro). Case-insensitive.
# Usar barras / como separador padrão.
FOLDER_PATH_TO_SOURCE_TYPE_RULES = {
    # Caminhos Completos (mais específicos)
    "/ARQUIVOS PDC/SIPCON 1 E 2": "evento_sipcon", # Nova Regra SIPCON
    "/TIME MARKETING DIGITAL/CONTEÚDO E CANAIS/CONTEÚDO YOUTUBE/1. CRONOGRAMA DE CONTEÚDO/ROTEIROS DE VÍDEOS": "roteiro_video_youtube",
    "/TIME MARKETING DIGITAL/CONTEÚDO E CANAIS/CONTEÚDO YOUTUBE": "video_youtube",
    "/TIME MARKETING DIGITAL/CONTEÚDO E CANAIS/CONTEÚDO INSTAGRAM": "conteudo_social",
    "/TIME MARKETING DIGITAL/LANÇAMENTOS": "material_lancamento",
    "/COPY/ANÚNCIOS": "anuncio_copy",
    "/FUNIS (E-MAIL, PERPÉTUO, ISCA)": "email",
    "/AULAS/TRANSCRIÇÕES": "transcricao_aula",
    # Nomes de Pasta (menos específicos, se caminho completo não bater)
    "SIPCON": "evento_sipcon", # Fallback para nome SIPCON
    "ANÚNCIOS": "anuncio_copy",
    "COPY": "anuncio_copy",
    "E-MAIL": "email",
    "EMAIL": "email",
    "ROTEIROS YOUTUBE": "roteiro_video_youtube",
    "CONTEÚDO YOUTUBE": "video_youtube",
    "TRANSCRIÇÕES": "transcricao_aula",
    "AULAS": "material_aula",
    "CONTEÚDO INSTAGRAM": "conteudo_social",
    "LANÇAMENTOS": "material_lancamento",
    # Adicione mais regras conforme a sua estrutura
}

# Cache para detalhes de pastas do GDrive
gdrive_folder_cache = {}

def get_folder_info_from_gdrive(service, folder_id, max_depth=10):
    """Busca nome e caminho completo de uma pasta no GDrive, com cache e limite de profundidade."""
    if not folder_id or folder_id == 'root': # Condição de parada
        return {'name': '[ROOT]', 'full_path': '/'}

    if folder_id in gdrive_folder_cache:
        return gdrive_folder_cache[folder_id]

    try:
        # Pedir nome e ID do pai na mesma chamada
        folder_metadata = service.files().get(fileId=folder_id, fields='id, name, parents').execute()
        folder_name = folder_metadata.get('name')
        parent_ids = folder_metadata.get('parents')

        if not folder_name:
            logger.warning(f"[GDRIVE API] Nome não encontrado para folder_id: {folder_id}")
            # Cache da falha parcial, mas tenta buscar pai
            parent_info = {'name': '[UNKNOWN]', 'full_path': '/[UNKNOWN]'}
        else:
            parent_info = {'name': '[ROOT]', 'full_path': '/'} # Default se não tiver pai ou erro

        # Lógica recursiva (ou iterativa) para obter caminho completo
        if parent_ids and max_depth > 0:
            # Assume o primeiro pai para simplificar, GDrive pode ter múltiplos
            parent_id = parent_ids[0]
            # Chamada recursiva com limite de profundidade para evitar loops infinitos
            parent_folder_info = get_folder_info_from_gdrive(service, parent_id, max_depth - 1)
            # Montar o caminho completo
            # Evitar barras duplicadas na raiz
            if parent_folder_info['full_path'] == '/':
                 current_full_path = f"/{folder_name}"
            else:
                 current_full_path = f"{parent_folder_info['full_path']}/{folder_name}"
            parent_info = {'name': folder_name, 'full_path': current_full_path}
        elif folder_name: # Caso base: pasta sem pai ou profundidade máxima atingida
             parent_info = {'name': folder_name, 'full_path': f"/{folder_name}"}

        gdrive_folder_cache[folder_id] = parent_info
        return parent_info

    except HttpError as e:
        # Tratar erro 404 (Not Found) especificamente, pode ser pasta deletada ou sem permissão
        if e.resp.status == 404:
             logger.warning(f"[GDRIVE API] Pasta não encontrada (404) para folder_id: {folder_id}. Pode ter sido deletada ou sem permissão.")
             error_info = {'name': '[NOT_FOUND]', 'full_path': '/[NOT_FOUND]'}
        else:
             logger.error(f"[GDRIVE API] Erro HTTP {e.resp.status} ao buscar info da pasta {folder_id}: {e}")
             error_info = {'name': '[HTTP_ERROR]', 'full_path': '/[HTTP_ERROR]'}
        gdrive_folder_cache[folder_id] = error_info
        return error_info
    except Exception as e:
        logger.error(f"[GDRIVE API] Erro inesperado ao buscar info da pasta {folder_id}: {e}")
        error_info = {'name': '[ERROR]', 'full_path': '/[ERROR]'}
        gdrive_folder_cache[folder_id] = error_info
        return error_info

def diagnose_metadata_structure(service, sample_size=5):
    logger.info(f"[DIAGNÓSTICO METADADOS] Buscando {sample_size} amostras de metadados de chunks...")
    try:
        response = supabase.table('documents').select('metadata').limit(sample_size).execute()
        if response.data:
            for i, record in enumerate(response.data):
                metadata = record.get('metadata')
                logger.info(f"Amostra {i+1} - Metadados crus: {metadata}")
                if metadata and service:
                    parent_folder_id = metadata.get('gdrive_parent_id')
                    if parent_folder_id:
                        folder_info = get_folder_info_from_gdrive(service, parent_folder_id)
                        logger.info(f"    -> Info da pasta pai (ID: {parent_folder_id}): {folder_info}")
        else:
            logger.warning("[DIAGNÓSTICO METADADOS] Nenhuma amostra encontrada.")
    except Exception as e:
        logger.error(f"[DIAGNÓSTICO METADADOS] Erro ao buscar amostras: {e}")

def get_large_chunks(token_limit=CHUNK_TOKEN_LIMIT):
    """Busca todos os chunks cujo content excede o limite de tokens."""
    response = supabase.table('documents').select('*').execute()
    if not hasattr(response, 'data') or not response.data:
        return []
    return [chunk for chunk in response.data if count_tokens(chunk['content']) > token_limit]

def insert_new_chunks(new_chunks, dry_run=True):
    import uuid
    valid_chunks = []
    for idx, chunk in enumerate(new_chunks):
        # Salva o antigo document_id para rastreabilidade
        old_doc_id = chunk.get('document_id') or chunk.get('metadata', {}).get('document_id')
        # Sempre gera um novo UUID para cada chunk novo
        novo_uuid = str(uuid.uuid4())
        chunk['document_id'] = novo_uuid
        # Garante que metadata existe e é dict
        if 'metadata' not in chunk or not isinstance(chunk['metadata'], dict):
            chunk['metadata'] = {}
        # Atualiza o document_id nos metadados
        chunk['metadata']['document_id'] = novo_uuid
        # Salva o original_document_id nos metadados
        chunk['metadata']['original_document_id'] = old_doc_id
        logger.info(f"[NOVO CHUNK] idx={idx} document_id={novo_uuid} original_document_id={old_doc_id}")
        valid_chunks.append(chunk)
    if dry_run:
        logger.info(f"[DRY-RUN] Inseriria {len(valid_chunks)} novos chunks no Supabase.")
        return True
    response = supabase.table('documents').insert(valid_chunks).execute()
    if hasattr(response, 'error') and response.error:
        logger.error(f"Erro ao inserir novos chunks: {response.error}")
        return False
    return True

def delete_old_chunk(document_id, dry_run=True):
    if dry_run:
        logger.info(f"[DRY-RUN] Deletaria chunk antigo document_id={document_id}")
        return True
    response = supabase.table('documents').delete().eq('document_id', document_id).execute()
    if hasattr(response, 'error') and response.error:
        logger.error(f"Erro ao deletar chunk antigo: {response.error}")
        return False
    return True

def rechunk_all(dry_run=True, min_chunk_tokens=300, use_ai=False):
    rodada = 1
    while True:
        logger.info(f"Iniciando rodada {rodada} de rechunk...")
        total_rechunked = 0
        algum_rechunkado = False
        large_chunks = get_large_chunks()
        if not large_chunks:
            logger.info("Nenhum chunk grande encontrado nesta rodada.")
            break
        for chunk in large_chunks:
            logger.info(f"[DIAGNÓSTICO] Metadados originais do chunk document_id={chunk['document_id']}: {chunk['metadata']}")
            logger.info(f"Processando chunk antigo document_id={chunk['document_id']} (tokens={count_tokens(chunk['content'])})")
            novos_chunks = split_content_into_chunks(
                chunk['content'],
                chunk['metadata'],
                max_chunk_tokens=CHUNK_TOKEN_LIMIT,
                min_chunk_chars=min_chunk_tokens,
                model_name=os.getenv("OPENAI_MODEL", "gpt-4o")
            )
            for idx, novo in enumerate(novos_chunks):
                logger.info(f"[DIAGNÓSTICO] Metadados do novo chunk {idx}: {novo['metadata']}")
                novo['metadata']['original_document_id'] = chunk['document_id']
                novo['metadata']['rechunked'] = True
                novo['metadata']['rechunked_at'] = str(uuid.uuid4())
                # Remover 'chunk_index' do dicionário principal, manter só em metadata
                if 'chunk_index' in novo:
                    del novo['chunk_index']  # chunk_index deve existir apenas em metadata
                logger.info(f"Novo chunk {idx}: {count_tokens(novo['content'])} tokens | split_type={novo['metadata'].get('section_split') and 'section' or novo['metadata'].get('sentence_split') and 'sentence' or novo['metadata'].get('token_split') and 'token' or 'paragraph'}")
            insert_new_chunks(novos_chunks, dry_run=dry_run)
            delete_old_chunk(chunk['document_id'], dry_run=dry_run)
            total_rechunked += 1
            algum_rechunkado = True
        logger.info(f"Rodada {rodada} finalizada. Total de chunks grandes reprocessados nesta rodada: {total_rechunked}")
        # Validação final
        all_chunks = supabase.table('documents').select('document_id,content').execute()
        acima_limite = [c for c in all_chunks.data if count_tokens(c['content']) > CHUNK_TOKEN_LIMIT]
        if not acima_limite:
            logger.info("Nenhum chunk acima do limite encontrado após o processo.")
            break
        else:
            logger.warning(f"Ainda existem {len(acima_limite)} chunks acima do limite após a rodada {rodada}!")
            if not algum_rechunkado:
                logger.error("Nenhum chunk foi reprocessado nesta rodada, mas ainda há chunks acima do limite. Pode haver erro de lógica ou dados corrompidos.")
                break
            rodada += 1

def get_source_type_from_folder_info(folder_info):
    """Aplica as regras de mapeamento para obter o source_type."""
    if not folder_info or not isinstance(folder_info, dict):
        return "desconhecido"

    full_path_upper = folder_info.get('full_path', '').upper() # Case-insensitive para caminhos completos
    folder_name_upper = folder_info.get('name', '').upper() # Case-insensitive para nomes de pasta

    # 1. Tentar por caminho completo mais específico (LOOP PRINCIPAL)
    for path_rule, source_type_val in FOLDER_PATH_TO_SOURCE_TYPE_RULES.items():
        # Apenas regras que começam com '/' são consideradas regras de caminho completo
        if path_rule.startswith('/') and path_rule.upper() in full_path_upper:
            logger.debug(f"Regra de caminho completo aplicada: '{path_rule}' -> '{source_type_val}' para '{full_path_upper}'")
            return source_type_val

    # 2. Regras adicionais baseadas em padrões de caminho (adicionadas após análise do log)
    if "/ARQUIVOS PDC/ACELERADOR DE CONSULTORIO PEDIATRICO" in full_path_upper:
        return "acp"
    elif "/ARQUIVOS PDC/PDC ESPECIALIDADES/MODULO" in full_path_upper:
         return "curso_pdc_especialidades"
    elif "/ARQUIVOS PDC/PEDCLASS" in full_path_upper:
         return "curso_pedclass"
    elif "/ARQUIVOS PDC/MANUAL DE ATUALIZACOES PEDIATRICAS" in full_path_upper:
         return "manual_atualizacao_ped"
    elif "/ARQUIVOS PDC/NOTES" in full_path_upper:
         return "pdc_notes"
    elif "/ARQUIVOS PDC/PDC URGENCIAS" in full_path_upper:
         return "curso_urgencias_pdc"
    elif "/ARQUIVOS PDC/PROTOCOLO SECRETARIA VENDEDORA" in full_path_upper:
         return "protocolo_secretaria" # Ou 'psv' se preferir
    elif "/TIME MARKETING DIGITAL/PROVAS SOCIAIS" in full_path_upper:
         return "prova_social"
    # Adicionar mais regras elif aqui conforme necessário
    elif "/ARQUIVOS PDC/PEDIATRA DE CONSULTÓRIO/MODULO" in full_path_upper: # Regra anterior mantida
        logger.debug(f"Regra específica 'Pediatra de Consultório' aplicada para '{full_path_upper}' -> 'material_aula_curso'")
        return "material_aula_curso"

    # 3. Tentar por nome da pasta como fallback (LOOP DE FALLBACK)
    for name_rule, source_type_val in FOLDER_PATH_TO_SOURCE_TYPE_RULES.items():
         # Apenas regras que NÃO começam com '/' são consideradas regras de nome de pasta
         if not name_rule.startswith('/') and name_rule.upper() == folder_name_upper:
            logger.debug(f"Regra de nome de pasta (fallback) aplicada: '{name_rule}' -> '{source_type_val}' para '{folder_name_upper}' (caminho completo: '{full_path_upper}')")
            return source_type_val

    logger.debug(f"Nenhuma regra aplicada para '{full_path_upper}' (nome: '{folder_name_upper}'). Retornando 'desconhecido'.")
    return "desconhecido"

def enrich_metadata_all(service, dry_run=True):
    logger.info(f"Iniciando enriquecimento de metadados. DRY-RUN={dry_run}")
    offset = 0
    total_updated_count = 0
    total_failed_count = 0
    batches_processed_count = 0 # Novo: Contador de lotes

    while True:
        # Novo: Lógica para limitar lotes em teste live
        # if not dry_run and batches_processed_count >= MAX_LIVE_TEST_BATCHES: # Removido/Comentado
        #     logger.info(f"Atingido o limite de {MAX_LIVE_TEST_BATCHES} lotes para teste em modo live. Interrompendo.") # Removido/Comentado
        #     break # Removido/Comentado

        logger.info(f"Buscando lote de documentos: offset={offset}, limit={BATCH_SIZE}")
        try: 
            response = supabase.table('documents').select('document_id, metadata').offset(offset).limit(BATCH_SIZE).execute()
        except Exception as e:
            logger.error(f"Erro ao buscar lote de documentos (offset {offset}): {e}")
            break 
        
        if not response.data:
            logger.info("Nenhum documento restante para processar.")
            break

        documents_to_update = []
        for doc in response.data:
            document_id = doc.get('document_id')
            metadata = doc.get('metadata', {})
            
            if not metadata: 
                logger.warning(f"Document ID {document_id}: Metadados ausentes ou vazios. Pulando.")
                continue

            parent_folder_id = metadata.get('gdrive_parent_id')
            
            if not parent_folder_id:
                logger.warning(f"Document ID {document_id}: gdrive_parent_id não encontrado nos metadados. Pulando obtenção de info da pasta.")
                continue 

            folder_info = get_folder_info_from_gdrive(service, parent_folder_id)
            
            full_folder_path = folder_info.get('full_path')
            # logger.debug(f"Document ID {document_id}: full_folder_path = '{full_folder_path}' (Type: {type(full_folder_path)})") # Comentado após depuração

            source_type = get_source_type_from_folder_info(folder_info)

            new_metadata_payload = {}
            changed = False

            if source_type and source_type != metadata.get('source_type'):
                new_metadata_payload['source_type'] = source_type
                changed = True
            
            if full_folder_path and full_folder_path not in ['/[UNKNOWN]', '/[NOT_FOUND]', '/[HTTP_ERROR]', '/[ERROR]'] and \
               full_folder_path != metadata.get('gdrive_full_folder_path'):
                new_metadata_payload['gdrive_full_folder_path'] = full_folder_path
                changed = True

            if changed:
                new_metadata_payload['metadata_enriched_at'] = datetime.now(timezone.utc).isoformat()
                updated_metadata_field = metadata.copy() 
                updated_metadata_field.update(new_metadata_payload) 
                documents_to_update.append({
                    'document_id': document_id,
                    'metadata': updated_metadata_field 
                })
                logger.debug(f"Document ID {document_id}: Agendado para atualização com payload: {new_metadata_payload}")
            else:
                logger.debug(f"Document ID {document_id}: Nenhum novo dado de enriquecimento para atualizar (source_type='{source_type}', full_folder_path='{full_folder_path}').")

        if documents_to_update:
            if not dry_run:
                logger.info(f"[LIVE RUN] Atualizando metadados de {len(documents_to_update)} documentos individualmente (offset {offset})...")
                batch_success_count = 0
                batch_failed_count = 0
                for item_to_update in documents_to_update:
                    doc_id_to_update = item_to_update.get('document_id')
                    metadata_to_update = item_to_update.get('metadata')
                    if not doc_id_to_update or metadata_to_update is None: 
                        logger.warning(f"Item inválido no lote de atualização (offset {offset}): {item_to_update}. Pulando.")
                        batch_failed_count += 1
                        continue
                    
                    try:
                        update_response = supabase.table('documents') \
                                                  .update({'metadata': metadata_to_update}) \
                                                  .eq('document_id', doc_id_to_update) \
                                                  .execute()

                        # logger.debug(f"Supabase update response for doc_id {doc_id_to_update}: count={getattr(update_response, 'count', 'N/A')}, data={getattr(update_response, 'data', 'N/A')}, error={getattr(update_response, 'error', 'N/A')}, status_code={getattr(update_response, 'status_code', 'N/A')}") # Comentado após depuração

                        if hasattr(update_response, 'error') and update_response.error:
                            logger.error(f"Erro na API Supabase ao atualizar doc_id {doc_id_to_update}: {update_response.error}")
                            batch_failed_count += 1
                        elif hasattr(update_response, 'data') and update_response.data:
                            batch_success_count += len(update_response.data) 
                            logger.debug(f"Dados retornados para doc_id {doc_id_to_update}: {update_response.data}")
                        else:
                             logger.warning(f"Resposta de update para doc_id {doc_id_to_update} não continha dados ou erro explícito. Resposta: {update_response}")

                    except Exception as e_update:
                        logger.error(f"Exceção durante o update do doc_id {doc_id_to_update}: {e_update}")
                        batch_failed_count += 1
                
                total_updated_count += batch_success_count
                total_failed_count += batch_failed_count
                logger.info(f"Lote (offset {offset}) finalizado: Sucessos={batch_success_count}, Falhas={batch_failed_count}")

            else:
                logger.info(f"[DRY-RUN] Atualizaria metadados de {len(documents_to_update)} documentos (offset {offset}).")
                total_updated_count += len(documents_to_update)
        else:
            logger.info(f"Nenhum documento precisou de atualização neste lote (offset {offset}).")

        offset += BATCH_SIZE
        batches_processed_count += 1 # Novo: Incrementar contador de lotes

    logger.info(f"Enriquecimento de metadados finalizado. Total atualizados: {total_updated_count}. Total falhas: {total_failed_count}. Lotes processados: {batches_processed_count}.")

def main():
    parser = argparse.ArgumentParser(description="Rechunking e enriquecimento de metadados de documentos no Supabase.")
    parser.add_argument('--dry-run', dest='dry_run', action='store_true', help='Executa em modo simulação (não altera o banco - padrão)')
    parser.add_argument('--no-dry-run', dest='dry_run', action='store_false', help='Executa de fato (altera o banco)')
    parser.set_defaults(dry_run=True) 
    parser.add_argument("--enrich", action="store_true", help="Ativa o modo de enriquecimento de metadados.")
    parser.add_argument("--rechunk", action="store_true", help="Ativa o modo de resegmentação de chunks gigantes.")
    args = parser.parse_args()

    logger.info(f"[MAIN] Script iniciado com os seguintes argumentos: {args}")

    gdrive_service = None
    if args.enrich:
        try:
            logger.info("[MAIN] Autenticando com Google Drive...")
            gdrive_service = authenticate_gdrive()
            if gdrive_service:
                logger.info("[MAIN] Autenticação com Google Drive bem-sucedida.")
            else:
                logger.error("[MAIN] Falha ao autenticar com Google Drive. O enriquecimento de metadados baseado em GDrive será limitado ou impossível.")
        except Exception as e:
            logger.error(f"[MAIN] Erro durante a autenticação do Google Drive: {e}")

    if args.enrich:
        if not gdrive_service:
            logger.error("[MAIN] Não é possível executar o enriquecimento de metadados (--enrich) pois a autenticação com o Google Drive falhou.")
        else:
            logger.info(f"[MAIN] Iniciando processo de enriquecimento de metadados. DRY-RUN={args.dry_run}") 
            enrich_metadata_all(gdrive_service, dry_run=args.dry_run)
            logger.info("[MAIN] Processo de enriquecimento de metadados finalizado.")
    
    if args.rechunk:
        logger.info(f"[MAIN] Iniciando processo de resegmentação de chunks gigantes. DRY-RUN={args.dry_run}")
        rechunk_all(dry_run=args.dry_run, use_ai=True)
        logger.info("[MAIN] Processo de resegmentação de chunks gigantes finalizado.")
    
    if not args.enrich and not args.rechunk:
        logger.info("Nenhuma ação principal (--enrich ou --rechunk) foi especificada. Use --enrich para enriquecer metadados ou --rechunk para resegmentar chunks gigantes. Use --help para ver as opções.")

    logger.info("[MAIN] Script finalizado.")

if __name__ == "__main__":
    try:
        from ingestion.gdrive_ingest import count_tokens, split_content_into_chunks
    except ImportError as e:
        def count_tokens(text):
            # Fallback simples: conta caracteres
            return len(text)
        def split_content_into_chunks(*args, **kwargs):
            raise ImportError(f"split_content_into_chunks não pôde ser importada: {e}. Corrija a estrutura do projeto.")
    main() 