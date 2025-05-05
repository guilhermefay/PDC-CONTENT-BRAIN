# etl/annotate_and_index.py
"""
Pipeline ETL principal para processar conteúdo do Google Drive,
 anotação com CrewAI, armazenamento no Supabase e indexação no R2R Cloud.

Este script orquestra as seguintes etapas:
1. Ingestão de Dados: Busca conteúdo (documentos e vídeos) de pastas
   configuradas no Google Drive usando `ingestion/gdrive_ingest.py`,
   que por sua vez utiliza `ingestion/video_transcription.py` para vídeos.
2. Chunking: Divide o conteúdo textual (de documentos e transcrições) em
   pedaços menores (chunks) baseados em contagem de tokens.
3. Anotação (Opcional): Usa um agente CrewAI (AnnotatorAgent) para avaliar cada
   chunk, decidindo se deve ser mantido (`keep=True/False`) para o RAG
   e atribuindo tags relevantes.
4. Armazenamento no Supabase: Salva TODOS os chunks (com metadados e anotações)
   na tabela `documents` do Supabase para registro e análise futura.
5. Indexação no R2R (Opcional): Envia os chunks marcados com `keep=True` para
   a instância R2R Cloud configurada para indexação vetorial, tornando-os
   pesquisáveis pela API RAG.
6. Rastreamento de Arquivos Processados: Mantém um registro no Supabase
   (tabela `processed_files`) para evitar reprocessar arquivos já ingeridos.

Requer configuração via variáveis de ambiente (ver `.env.sample`).
"""

# --- DEBUG LOG INICIO --- 
import logging
import os
import sys
# --- Add subprocess import ---
import subprocess
print("--- DEBUG: annotate_and_index.py STARTING ---", file=sys.stderr)
try:
    # Adiciona o diretório raiz do projeto ao PYTHONPATH
    # Isso garante que módulos como 'agents', 'infra', etc., sejam encontrados
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    print(f"--- DEBUG: Project root added to sys.path: {project_root} ---", file=sys.stderr)

    # --- Execute normalize_json.py ---
    normalize_script_path = os.path.join(os.path.dirname(__file__), '..', 'normalize_json.py') # Assumes it's in the root /app
    normalize_script_path = os.path.abspath(normalize_script_path) # Get absolute path
    print(f"--- DEBUG: Attempting to run normalize_json.py at {normalize_script_path} ---", file=sys.stderr)
    # Ajuste para caminho relativo simples assumindo que Docker WORKDIR é /app
    normalize_script_in_container = 'normalize_json.py' 
    result = subprocess.run(['python3', normalize_script_in_container], check=True, capture_output=True, text=True)
    # Corrigir f-string com aspas triplas para permitir quebras de linha
    print(f'''--- DEBUG: normalize_json.py stdout:
{result.stdout} ---''', file=sys.stderr)
    # Corrigir f-string com aspas triplas para permitir quebras de linha
    print(f'''--- DEBUG: normalize_json.py stderr:
{result.stderr} ---''', file=sys.stderr)
    print(f"--- DEBUG: normalize_json.py finished successfully ---", file=sys.stderr)
    # --- Fim da execução ---

except Exception as e_init:
    print(f"--- DEBUG: ERROR during initial setup or normalize_json execution: {e_init} ---", file=sys.stderr)
    # Imprimir traceback completo para depuração
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.exit(1) # Sair se a preparação falhar
# --- DEBUG LOG FIM --- 

import argparse
import json
import time
from datetime import datetime, timezone
import tiktoken
import tempfile
import uuid
import shutil # Adicionado para limpeza de diretório
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
# --- Comentado para Debug (locais) ---
# from agents.annotator_agent import AnnotatorAgent
from agents.annotator_agent import AnnotatorAgent, ChunkOut
# --- Comentado para Debug (terceiros suspeitos) ---
# from supabase import create_client, Client, PostgrestAPIResponse
from supabase import create_client, Client, PostgrestAPIResponse
from postgrest.exceptions import APIError as PostgrestAPIError
# --- Fim Comentado ---
from concurrent.futures import ThreadPoolExecutor, as_completed
# --- Comentado para Debug (locais) ---
# from infra.r2r_client import R2RClientWrapper
from infra.r2r_client import R2RClientWrapper
# from ingestion.gdrive_ingest import ingest_all_gdrive_content
# from ingestion.gdrive_ingest import ingest_all_gdrive_content # Removido - Não é mais usado aqui
# from ingestion.local_ingest import ingest_local_directory
# from ingestion.local_ingest import ingest_local_directory # Removido - Não é mais usado aqui
# from ingestion.video_transcription import process_video # Importar a função específica # Removido - Não é mais usado aqui
# --- Fim Comentado ---
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configurar logging GLOBALMENTE
# Mudar level para DEBUG para ver logs mais detalhados
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')
logger = logging.getLogger(__name__) # Definir logger globalmente
print("--- DEBUG: Logging configured ---", file=sys.stderr) # DEBUG LOG

# Placeholder para conexão com Supabase (será implementado depois)
# from supabase import create_client, Client
# supabase_url: str = os.environ.get("SUPABASE_URL")
# supabase_key: str = os.environ.get("SUPABASE_SERVICE_KEY")
# supabase: Client = create_client(supabase_url, supabase_key)

# --- Modificado: Descomentado e Configurado Supabase ---
# Placeholder para conexão com Supabase (será implementado depois)
from supabase import create_client, Client
supabase_url: str = os.environ.get("SUPABASE_URL")
supabase_key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase: Client = None # Inicializar como None
try:
    if supabase_url and supabase_key:
        supabase = create_client(supabase_url, supabase_key)
        logging.info("Supabase client initialized successfully.")
    else:
        logging.warning("Supabase URL or Key not found in environment variables. Supabase integration disabled.")
except Exception as e:
    logging.error(f"Error initializing Supabase client: {e}")
    supabase = None # Garantir que supabase seja None em caso de erro
# --- Fim Modificado ---

# --- Adicionado: Inicialização R2R Client Wrapper ---
try:
    r2r_client = R2RClientWrapper()
    logging.info("R2R Client Wrapper initialized successfully for ETL.")
except ValueError as e:
    logging.error(f"Failed to initialize R2R Client Wrapper: {e}. Check R2R_BASE_URL. R2R uploads disabled.")
    r2r_client = None
except Exception as e:
    logging.error(f"Unexpected error initializing R2R Client Wrapper: {e}. R2R uploads disabled.", exc_info=True)
    r2r_client = None
# --- Fim Adicionado ---

# Inicializar tiktoken (usar encoding para modelos OpenAI mais recentes)
try:
    tokenizer = tiktoken.get_encoding("cl100k_base")
except Exception as e:
    logging.warning(f"Falha ao carregar tokenizer tiktoken 'cl100k_base', usando 'p50k_base' como fallback: {e}")
    try:
        tokenizer = tiktoken.get_encoding("p50k_base")
    except Exception as e2:
         logging.error(f"Falha ao carregar qualquer tokenizer tiktoken: {e2}. Contagem de tokens não funcionará.")
         tokenizer = None

# --- DEBUG LOG ANTES DAS FUNÇÕES ---
print("--- DEBUG: Initializations complete, defining functions... ---", file=sys.stderr)
# --- DEBUG LOG FIM ---

# --- Configuração de Retentativas Tenacity ---

# Erros comuns de rede/API que podem ser temporários
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    PostgrestAPIError, # Erros específicos do Postgrest (usado pelo supabase-py)
    # Adicionar outros erros específicos de API se necessário (ex: R2RError?)
)

# Estratégia de retentativa padrão: Tentar 3 vezes, esperar exponencialmente (max 10s)
default_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS)
)

# --- Fim Configuração Tenacity ---

@default_retry
def _update_chunk_status_supabase(supabase_client: Client, document_id: str, data_to_update: Dict[str, Any], step_name: str):
    """Função auxiliar para atualizar o status de um chunk no Supabase (com retentativas).

    Modificado para enviar apenas os campos que realmente precisam ser atualizados,
    evitando reenviar 'content' ou 'metadata' inteiros para prevenir erros de JSON.
    """
    if not supabase_client or not document_id:
        logger.warning(f"[Supabase Update - {step_name}] Cliente Supabase ou document_id ausente. Abortando atualização.")
        return False

    # Garantir que apenas campos válidos da tabela 'documents' sejam enviados
    # Campos permitidos para atualização via esta função (evita enviar 'content')
    allowed_update_fields = {
        "annotation_status", "annotated_at", "keep", "annotation_tags",
        "indexing_status", "indexed_at", "annotation_reason" # Adicionar 'reason' se existir/usado
        # NUNCA inclua 'content' ou 'metadata' aqui para evitar problemas de tamanho/JSON
    }

    update_payload = {k: v for k, v in data_to_update.items() if k in allowed_update_fields and v is not None}

    if not update_payload: # Não fazer chamada se não houver nada válido para atualizar
        logger.debug(f"[Supabase Update - {step_name}] Nada válido a atualizar para doc_id {document_id}")
        return True # Considerar sucesso se não há o que fazer

    logger.debug(f"[Supabase Update - {step_name}] Tentando atualizar status para doc_id {document_id} com payload: {update_payload}")
    try:
        response = supabase_client.table('documents')\
                                  .update(update_payload)\
                                  .eq('document_id', document_id)\
                                  .execute()

        # Verificar se a atualização foi bem-sucedida (Supabase retorna dados ou status 2xx)
        # Ajuste na verificação de sucesso
        if response.data or (hasattr(response, 'status_code') and 200 <= response.status_code < 300):
            logger.debug(f"[Supabase Update - {step_name}] Status atualizado para doc_id {document_id}")
            return True
        else:
            # Tratar erro específico que pode não lançar exceção mas indicar falha
            error_info = getattr(response, 'error', None) or getattr(response, 'message', 'Unknown error')
            logger.error(f"[Supabase Update - {step_name}] Falha FINAL ao atualizar status para doc_id {document_id} após retentativas: {error_info}, Status: {getattr(response, 'status_code', 'N/A')}")
            return False # Indicar falha
    except (PostgrestAPIError) as e:
        logger.error(f"[Supabase Update - {step_name}] Erro FINAL API Postgrest ao atualizar status para doc_id {document_id} após retentativas: {e}")
        raise # Re-lançar para que a retentativa externa (se houver) possa pegar
    except Exception as e:
        logger.error(f"[Supabase Update - {step_name}] Erro FINAL inesperado ao atualizar status para doc_id {document_id} após retentativas: {e}", exc_info=True)
        raise # Re-lançar

@default_retry
def _run_annotator_with_retry(annotator: AnnotatorAgent, chunk_data: Dict[str, Any], source_name: str) -> Optional[ChunkOut]:
    """Executa o annotator para um ÚNICO chunk com retentativas."""
    document_id = chunk_data.get('metadata', {}).get('document_id', 'ID Desconhecido')
    chunk_index = chunk_data.get('metadata', {}).get('chunk_index', 'N/A')
    logger.info(f"[Anotação Retry] Tentando executar anotação para chunk {chunk_index} de {source_name} (ID: {document_id})...")
    start_time = time.time()
    try:
        # O método run do AnnotatorAgent agora espera um único dict
        # e retorna um único objeto ChunkOut ou None
        result: Optional[ChunkOut] = annotator.run(chunk_data)
        duration = time.time() - start_time
        if result:
            logger.info(f"[Anotação Retry] Execução bem-sucedida em {duration:.2f}s para chunk {chunk_index}.")
        else:
            logger.warning(f"[Anotação Retry] Anotação retornou None para chunk {chunk_index} em {duration:.2f}s.")
        return result # Retorna o objeto ChunkOut ou None
    except Exception as e:
         # Logar o erro que causou a falha final das retentativas
         logger.error(f"[Anotação Retry] Erro FINAL ao executar anotação para chunk {chunk_index} de {source_name} (ID: {document_id}) após retentativas: {e}", exc_info=True)
         raise # Re-lançar para tenacity

@default_retry
def _upload_single_chunk_to_r2r_with_retry(r2r_client_instance: R2RClientWrapper, chunk_content: str, document_id: str, metadata: Dict[str, Any]):
    """Faz upload para R2R de um chunk (conteúdo direto) com retentativas."""
    logger.debug(f"[R2R Upload Retry] Tentando upload para doc_id: {document_id}")
    temp_file_path = None
    try:
        # Cria arquivo temporário para o conteúdo do chunk
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".txt", encoding='utf-8') as temp_file:
            temp_file.write(chunk_content)
            temp_file_path = temp_file.name
        logging.debug(f"Conteúdo do chunk {document_id} salvo em arquivo temporário: {temp_file_path}")

        result = r2r_client_instance.upload_file(
            file_path=temp_file_path,
            document_id=document_id, # Usar o ID do chunk como ID do documento no R2R
            metadata=metadata
        )
        logger.debug(f"[R2R Upload Retry] Resultado para doc_id {document_id}: {result}")
        # Limpar arquivo temporário APENAS em caso de sucesso
        if temp_file_path and os.path.exists(temp_file_path):
             try:
                  os.remove(temp_file_path)
                  logging.debug(f"Arquivo temporário {temp_file_path} removido após sucesso.")
             except OSError as e_remove:
                  logging.warning(f"Não foi possível remover o arquivo temporário {temp_file_path} após sucesso: {e_remove}")
        return result # Retorna o dicionário de resultado

    except Exception as e:
         logger.error(f"[R2R Upload Retry] Erro FINAL no upload para doc_id {document_id} após retentativas: {e}", exc_info=False)
         # Limpar arquivo temporário em caso de ERRO também
         if temp_file_path and os.path.exists(temp_file_path):
              try:
                   os.remove(temp_file_path)
                   logging.debug(f"Arquivo temporário {temp_file_path} removido após falha.")
              except OSError as e_remove:
                   logging.warning(f"Não foi possível remover o arquivo temporário {temp_file_path} após falha: {e_remove}")
         raise # Re-lançar para que a retentativa externa funcione

def process_single_chunk(
    chunk_data: Dict[str, Any],
    annotator: AnnotatorAgent,
    r2r_client_instance: R2RClientWrapper,
    supabase_client: Client,
    skip_annotation: bool = False,
    skip_indexing: bool = False,
) -> bool:
    """
    Processa um único chunk: anota (opcional), atualiza status no Supabase,
    indexa no R2R (opcional), atualiza status de indexação no Supabase.

    Retorna True se o processamento (ou tentativa) do chunk foi concluído
    sem erros críticos que impeçam o fluxo principal (ex: falha grave no DB).
    Retorna False se ocorrer um erro crítico.
    """
    document_id = chunk_data.get('document_id', 'ID_Ausente') # Obter ID direto do chunk_data
    source_name = chunk_data.get('metadata', {}).get('source_name', 'Nome Desconhecido')
    chunk_index = chunk_data.get('metadata', {}).get('chunk_index', 'N/A')
    logging.debug(f"[process_single_chunk START] ID: {document_id}, Index: {chunk_index}, Source: {source_name}")

    # --- Inicialização de variáveis de status ---
    annotation_status = chunk_data.get('annotation_status', 'pending') # Pega status atual se já existe
    annotation_tags = chunk_data.get('annotation_tags', [])
    keep_chunk = chunk_data.get('keep', True) # Default para True (mas será sobrescrito pela anotação)
    annotated_at = chunk_data.get('annotated_at')
    indexing_status = chunk_data.get('indexing_status', 'pending')
    indexed_at = chunk_data.get('indexed_at')
    annotation_reason = chunk_data.get('annotation_reason') # Campo opcional

    # Pular se já processado (status final) ? Não, a busca inicial já pega só 'pending'
    # if annotation_status in ['done', 'success', 'failed', 'error', 'skipped']:
    #     logging.debug(f"Chunk {document_id} já tem status final de anotação '{annotation_status}', pulando.")
    #     return True # Considera sucesso pular algo já feito

    try:
        # --- Etapa 1: Anotação (se não pulada) ---
        if not skip_annotation:
            if annotator:
                try:
                    annotated_result: Optional[ChunkOut] = _run_annotator_with_retry(annotator, chunk_data, source_name)

                    if annotated_result:
                        # Atualizar variáveis com resultado da anotação
                        annotation_tags = annotated_result.tags
                        keep_chunk = annotated_result.keep
                        annotation_reason = annotated_result.reason # Captura a razão
                        annotation_status = 'done' # Sucesso na anotação
                        annotated_at = datetime.now(timezone.utc).isoformat()
                        logger.info(f"Chunk {document_id} (Idx: {chunk_index}) anotado. Keep: {keep_chunk}, Tags: {annotation_tags}, Reason: {annotation_reason}")

                    else:
                        # Caso _run_annotator_with_retry retorne None (ex: falha interna do agente)
                        logger.warning(f"Anotação retornou None para chunk {document_id} (Idx: {chunk_index}). Marcando como erro.")
                        annotation_status = 'error'
                        keep_chunk = False # Não manter em caso de erro
                        annotated_at = datetime.now(timezone.utc).isoformat()
                        annotation_reason = "Annotator returned None"

                except Exception as e_annotate:
                    # Captura exceção final após retries de _run_annotator_with_retry
                    logger.error(f"Erro FINAL na anotação do chunk {document_id} (Idx: {chunk_index}): {e_annotate}", exc_info=False) # exc_info=False para não poluir tanto
                    annotation_status = 'error'
                    keep_chunk = False # Não manter se a anotação falhou
                    annotated_at = datetime.now(timezone.utc).isoformat()
                    annotation_reason = f"Annotation failed after retries: {e_annotate}"

                # ---- ATUALIZAÇÃO NO SUPABASE PÓS-ANOTAÇÃO (SUCESSO OU FALHA) ----
                annotation_update_payload = {
                    "annotation_status": annotation_status,
                    "annotated_at": annotated_at,
                    "keep": keep_chunk,
                    "annotation_tags": annotation_tags,
                    "annotation_reason": annotation_reason # Inclui a razão
                }
                try:
                    if not _update_chunk_status_supabase(supabase_client, document_id, annotation_update_payload, "update annotation status"):
                        logger.error(f"FALHA CRÍTICA ao atualizar status da anotação para chunk {document_id}. Dados podem ficar inconsistentes.")
                        # Decidir se isso deve parar o processo do chunk? Por ora, continua para indexação.
                except Exception as e_update_anno:
                    logger.error(f"Exceção CRÍTICA ao tentar atualizar status da anotação para chunk {document_id}: {e_update_anno}", exc_info=True)
                    # Continuar mesmo assim? Por ora, sim.

            else: # Se annotator não foi inicializado
                logging.warning(f"AnnotatorAgent não inicializado, pulando anotação para chunk {document_id}.")
                annotation_status = 'skipped'
                # Manter keep_chunk com seu valor inicial (default True ou do DB)
                # Não precisa atualizar DB aqui, pois status já é 'skipped' ou será atualizado pela indexação
        else: # Se skip_annotation=True
            logging.info(f"Anotação pulada via flag para chunk {document_id}.")
            annotation_status = 'skipped'
            # Manter keep_chunk com seu valor inicial

        # --- Etapa 2: Indexação (se não pulada E keep=True RESULTANTE DA ANOTAÇÃO) ---
        indexing_status = 'skipped' # Default se keep=False ou skip_indexing=True
        indexed_at = None

        if not skip_indexing and keep_chunk: # USA O VALOR DE keep_chunk DEFINIDO ACIMA
            logging.debug(f"[process_single_chunk] Indexando chunk {document_id} (Idx: {chunk_index}) no R2R...")
            if r2r_client_instance:
                try:
                    # A função de upload agora lida com o arquivo temporário
                    _upload_single_chunk_to_r2r_with_retry(
                        r2r_client_instance,
                        chunk_data["content"], # Passa o conteúdo diretamente
                        document_id,
                        chunk_data["metadata"] # Passa os metadados atuais
                    )
                    indexing_status = 'done' # Sucesso na indexação
                    indexed_at = datetime.now(timezone.utc).isoformat()
                    logger.info(f"Chunk {document_id} (Idx: {chunk_index}) indexado com sucesso no R2R.")

                except Exception as e_r2r:
                    # Captura exceção final de _upload_single_chunk_to_r2r_with_retry
                    logger.error(f"Erro FINAL na indexação R2R do chunk {document_id} (Idx: {chunk_index}): {e_r2r}", exc_info=False)
                    indexing_status = 'error'
                    indexed_at = datetime.now(timezone.utc).isoformat() # Marcar tempo do erro

            else: # Se r2r_client não foi inicializado
                logging.warning(f"R2R Client não inicializado, pulando indexação para chunk {document_id}.")
                indexing_status = 'skipped'

            # ---- ATUALIZAÇÃO NO SUPABASE PÓS-INDEXAÇÃO (SUCESSO, FALHA OU SKIP) ----
            indexing_update_payload = {
                "indexing_status": indexing_status,
                "indexed_at": indexed_at
                # Não precisa enviar outros campos aqui, só o resultado da indexação
            }
            try:
                if not _update_chunk_status_supabase(supabase_client, document_id, indexing_update_payload, f"update indexing status ({indexing_status})"):
                     logger.error(f"FALHA CRÍTICA ao atualizar status da indexação ({indexing_status}) para chunk {document_id}.")
            except Exception as e_update_idx:
                 logger.error(f"Exceção CRÍTICA ao tentar atualizar status da indexação ({indexing_status}) para chunk {document_id}: {e_update_idx}", exc_info=True)

        elif not keep_chunk:
            logging.info(f"Chunk {document_id} (Idx: {chunk_index}) marcado como keep=False, indexação pulada.")
            indexing_status = 'skipped' # Garante status correto
            # Tentativa opcional de atualizar o status 'skipped' no DB
            skipped_update_payload = {"indexing_status": indexing_status}
            try:
                _update_chunk_status_supabase(supabase_client, document_id, skipped_update_payload, "update indexing status (skipped)")
            except Exception: # Ignorar falha aqui, é menos crítico
                pass
        else: # Se skip_indexing=True
             logging.info(f"Indexação pulada via flag para chunk {document_id}.")
             indexing_status = 'skipped'
             # Opcional: atualizar status skipped no DB

        # Se chegamos aqui sem exceções fatais, consideramos sucesso no processamento geral do chunk
        logging.debug(f"[process_single_chunk END] ID: {document_id} (Idx: {chunk_index}) - Retornando True")
        return True

    # --- Captura de Erro Geral Inesperado ---
    except Exception as e_main:
        logger.error(f"[process_single_chunk END] Erro GERAL inesperado processando chunk {document_id} (Idx: {chunk_index}): {e_main}", exc_info=True)
        # Tentar marcar como erro no Supabase como último recurso
        try:
            error_update = {
                "annotation_status": "error",
                "indexing_status": "error",
                "keep": False # Default em caso de erro geral
            }
            _update_chunk_status_supabase(supabase_client, document_id, error_update, "update general error status")
        except Exception as e_update_err:
            logger.error(f"Falha CRÍTICA ao tentar atualizar status de erro geral para chunk {document_id} após erro principal: {e_update_err}")

        return False # Indica falha no processamento deste chunk

def run_pipeline(
    # Argumentos da função como definidos anteriormente, mas removendo os não usados
    # REMOVIDO: source: str,
    # REMOVIDO: local_dir: str,
    # REMOVIDO: dry_run: bool,
    # REMOVIDO: dry_run_limit: Optional[int],
    skip_annotation: bool,
    skip_indexing: bool,
    batch_size: int = 100, # Usado para buscar chunks do Supabase
    max_workers_pipeline: int = 5
):
    """
    Orquestra o pipeline ETL: busca chunks PENDENTES no Supabase,
    anota e indexa usando ThreadPoolExecutor.
    """
    logging.info(f"Iniciando pipeline ETL (Modo Consulta Supabase)... Skip Annotation: {skip_annotation}, Skip Indexing: {skip_indexing}")
    start_time_pipeline = time.time()

    if not supabase:
        logging.error("Cliente Supabase não inicializado. Pipeline não pode continuar.")
        return

    # Inicializar AnnotatorAgent (se necessário)
    annotator = None
    if not skip_annotation:
        try:
            annotator = AnnotatorAgent()
            logging.info("AnnotatorAgent inicializado com sucesso.")
        except Exception as e_annotator_init:
            logging.error(f"Erro ao inicializar AnnotatorAgent: {e_annotator_init}. Anotação será pulada.", exc_info=True)
            skip_annotation = True # Forçar pulo da anotação se falhar

    # --- Lógica de Busca de Chunks Pendentes no Supabase ---
    all_chunks_to_process: List[Dict[str, Any]] = []
    try:
        logger.info(f"Buscando chunks com annotation_status='pending' no Supabase (batch_size={batch_size})...")
        # Busca direta, sem retry aqui. O retry está nas operações internas.
        response = supabase.table("documents")\
            .select("document_id, content, metadata, annotation_status, indexing_status, keep, annotation_tags, annotated_at, indexed_at")\
            .eq("annotation_status", "pending")\
            .limit(batch_size)\
            .execute()

        if response.data:
            all_chunks_to_process = response.data
            logger.info(f"Encontrados {len(all_chunks_to_process)} chunks pendentes no Supabase para este lote.")
        else:
            logger.info("Nenhum chunk com annotation_status='pending' encontrado no Supabase. Pipeline concluído para este ciclo.")
            # Se não há chunks, termina a execução deste ciclo. O Railway reiniciará se configurado.
            end_time_pipeline = time.time()
            logging.info(f"Pipeline ETL (Modo Consulta Supabase) concluído (sem chunks pendentes) em {end_time_pipeline - start_time_pipeline:.2f} segundos.")
            return # Termina a execução normal deste ciclo

    except PostgrestAPIError as api_error:
        logger.error(f"Erro da API Postgrest ao buscar chunks pendentes no Supabase: {api_error}", exc_info=True)
        return # Não continuar se não conseguir buscar chunks
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar chunks pendentes no Supabase: {e}", exc_info=True)
        return # Não continuar se não conseguir buscar chunks

    # --- Processamento em Paralelo dos Chunks ---
    total_chunks_in_batch = len(all_chunks_to_process)
    chunks_processed_count = 0
    successful_chunks = 0
    failed_chunks = 0
    start_time_batch = time.time() # Tempo de início do processamento do lote

    with ThreadPoolExecutor(max_workers=max_workers_pipeline) as executor:
        # Mapeia future para document_id para logging
        futures = {
            executor.submit(
                process_single_chunk,
                chunk_data=chunk,
                annotator=annotator,
                r2r_client_instance=r2r_client,
                supabase_client=supabase,
                skip_annotation=skip_annotation,
                skip_indexing=skip_indexing
            ): chunk.get('document_id', f'Unknown_ID_at_submit_{i}')
            for i, chunk in enumerate(all_chunks_to_process)
        }

        for future in as_completed(futures):
            chunks_processed_count += 1
            chunk_id_from_map = futures[future] # Pega o ID que mapeamos
            progress = (chunks_processed_count / total_chunks_in_batch) * 100 if total_chunks_in_batch > 0 else 0

            try:
                # future.result() retorna o booleano de process_single_chunk
                # ou lança a exceção se ocorreu erro não capturado internamente
                success = future.result()
                if success:
                    successful_chunks += 1
                    # Log menos verboso aqui, o log detalhado está em process_single_chunk
                    # logger.debug(f"[run_pipeline loop] Chunk {chunk_id_from_map} completou com sucesso. Progresso: {progress:.2f}%")
                else:
                    failed_chunks += 1
                    logging.warning(f"[run_pipeline loop] Processamento do chunk {chunk_id_from_map} FALHOU (retornou False). Progresso: {progress:.2f}%")
            except Exception as exc:
                 failed_chunks += 1
                 # Logar a exceção que veio do future (pode ser de retry ou outra)
                 logging.error(f"[run_pipeline loop] Exceção ao processar chunk {chunk_id_from_map}: {exc}. Progresso: {progress:.2f}%", exc_info=False) # exc_info=False aqui

    end_time_batch = time.time()
    logger.info(f"Processamento do lote de {total_chunks_in_batch} chunks concluído em {end_time_batch - start_time_batch:.2f} segundos. Sucesso: {successful_chunks}, Falha: {failed_chunks}.")
    end_time_pipeline = time.time()
    logging.info(f"Ciclo completo do pipeline ETL (Modo Consulta Supabase) concluído em {end_time_pipeline - start_time_pipeline:.2f} segundos.")


def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL para RAG - Modo Consulta Supabase")
    parser.add_argument("--skip-annotation", action="store_true", help="Pular a etapa de anotação com CrewAI.")
    parser.add_argument("--skip-indexing", action="store_true", help="Pular a etapa de indexação no R2R Cloud.")
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("ETL_BATCH_SIZE", 100)), help="Tamanho do lote para buscar chunks do Supabase.") # Ler do env var
    parser.add_argument("--max-workers", type=int, default=int(os.environ.get("ETL_MAX_WORKERS", 5)), help="Número máximo de workers para processar chunks em paralelo.") # Ler do env var

    args = parser.parse_args()

    # Log inicial com os parâmetros usados
    logger.info(f"Iniciando pipeline ETL (Modo Consulta Supabase) com parâmetros: "
                f"skip_annotation={args.skip_annotation}, skip_indexing={args.skip_indexing}, "
                f"batch_size={args.batch_size}, max_workers={args.max_workers}")

    run_pipeline(
        skip_annotation=args.skip_annotation,
        skip_indexing=args.skip_indexing,
        batch_size=args.batch_size,
        max_workers_pipeline=args.max_workers
    )

    logger.info("Execução do script annotate_and_index.py finalizada.") # Log final do script

if __name__ == "__main__":
    main() 