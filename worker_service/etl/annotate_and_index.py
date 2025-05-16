#!/usr/bin/env python3
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

from __future__ import annotations

# --- INÍCIO DO MONKEY PATCH HTTP/1.1 ---
import httpx  # Necessário para o patch
import sys    # Para print flush

print("!!! ATENÇÃO: TENTANDO APLICAR MONKEY PATCH GLOBAL NO HTTpx PARA FORÇAR HTTP/1.1 !!!", flush=True)

_original_httpx_client_init = httpx.Client.__init__
_original_httpx_async_client_init = httpx.AsyncClient.__init__

def _patched_httpx_client_init(self_client, *args, **kwargs):
    print(f"MONKEY_PATCH: httpx.Client.__init__ - Forçando http2=False. Kwargs recebidos: {kwargs}", flush=True)
    kwargs['http2'] = False
    _original_httpx_client_init(self_client, *args, **kwargs)

def _patched_httpx_async_client_init(self_async_client, *args, **kwargs):
    print(f"MONKEY_PATCH: httpx.AsyncClient.__init__ - Forçando http2=False. Kwargs recebidos: {kwargs}", flush=True)
    kwargs['http2'] = False
    _original_httpx_async_client_init(self_async_client, *args, **kwargs)

httpx.Client.__init__ = _patched_httpx_client_init
httpx.AsyncClient.__init__ = _patched_httpx_async_client_init

print("!!! MONKEY PATCH HTTpx APLICADO GLOBALMENTE !!!", flush=True)
# --- FIM DO MONKEY PATCH HTTP/1.1 ---

import argparse
import logging
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import tempfile
import json
import traceback
import asyncio
from collections import defaultdict
from functools import lru_cache
from cachetools import TTLCache

import tiktoken
from dotenv import load_dotenv
from postgrest.exceptions import APIError as PostgrestAPIError
from supabase import Client, create_client
from supabase.lib.client_options import ClientOptions
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from httpx import TimeoutException, ConnectError, RemoteProtocolError
from r2r import DocumentChunk

# Adiciona o diretório raiz do projeto ao PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from agents.annotator_agent import AnnotatorAgent, ChunkOut
from infra.r2r_client import R2RClientWrapper

# ---------------------------------------------------------------------------
# Configuração global
# ---------------------------------------------------------------------------
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
)
logger = logging.getLogger("etl")

try:
    tokenizer = tiktoken.get_encoding("cl100k_base")
except Exception:
    tokenizer = None
    logger.warning("tiktoken indisponível – contagem de tokens desligada.")

# ---------------------------------------------------------------------------
# Conexões externas
# ---------------------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("SUPABASE_URL ou SUPABASE_SERVICE_KEY ausentes. Abortando.")
    sys.exit(1)

# Configurar timeouts e headers
options = ClientOptions(
    postgrest_client_timeout=300  # Timeout de 5 minutos para operações do PostgREST
    # Headers de autenticação removidos daqui.
    # create_client(SUPABASE_URL, SUPABASE_KEY) deve lidar com a service_role_key automaticamente.
)

# --- REVERTENDO: Voltar para a inicialização padrão --- 
supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options)
print("--- SUPABASE CLIENT INITIALIZED (using ClientOptions) ---", flush=True)

# DEBUG: Verificar as opções do cliente PostgREST interno
print("--- BEGINNING SUPABASE CLIENT DEBUG ---", flush=True)
try:
    if hasattr(supabase_client, 'postgrest') and supabase_client.postgrest:
        print("DEBUG: supabase_client.postgrest object exists.", flush=True)
        
        if hasattr(supabase_client.postgrest, 'session') and supabase_client.postgrest.session:
            internal_httpx_client = supabase_client.postgrest.session
            print(f"DEBUG: Internal httpx client type: {type(internal_httpx_client)}", flush=True)
            if hasattr(internal_httpx_client, 'timeout'):
                print(f"DEBUG: Timeout of internal httpx client: {internal_httpx_client.timeout}", flush=True)
            else:
                if hasattr(supabase_client.postgrest, 'options') and hasattr(supabase_client.postgrest.options, 'timeout'):
                     print(f"DEBUG: Timeout from supabase_client.postgrest.options: {supabase_client.postgrest.options.timeout}", flush=True)

            if hasattr(internal_httpx_client, 'headers'):
                print(f"DEBUG: Headers of internal httpx client: {internal_httpx_client.headers}", flush=True)
            
            print(f"DEBUG: HTTP/2 support status is not directly introspectable via simple attribute here.", flush=True)
        else:
            print("DEBUG: Could not access supabase_client.postgrest.session for httpx client details.", flush=True)
    else:
        print("DEBUG: supabase_client.postgrest object NOT found or not initialized.", flush=True)
except Exception as e_debug:
    print(f"DEBUG: Exception during Supabase client introspection: {str(e_debug)}", flush=True)
print("--- END OF SUPABASE CLIENT DEBUG ---", flush=True)

try:
    r2r_client = R2RClientWrapper()
    logger.info("R2R Client inicializado.")
except Exception as exc:
    r2r_client = None
    logger.warning("R2R indisponível: %s", exc)

# ---------------------------------------------------------------------------
# Funções utilitárias
# ---------------------------------------------------------------------------

RETRYABLE_EXCEPTIONS = (ConnectionError, TimeoutError, PostgrestAPIError, RemoteProtocolError)

def tenacity_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    )

def _sanitize_metadata(item_to_sanitize: Any) -> Any:
    """
    Sanitiza recursivamente um item para garantir que seja serializável em JSON
    e que valores não hasheáveis sejam convertidos em string.
    Particularmente útil para metadados que podem conter slices ou dicts aninhados.
    """
    if isinstance(item_to_sanitize, dict):
        return {k: _sanitize_metadata(v_item) for k, v_item in item_to_sanitize.items()}
    elif isinstance(item_to_sanitize, list):
        return [_sanitize_metadata(v_item) for v_item in item_to_sanitize]
    elif isinstance(item_to_sanitize, slice):
        return str(item_to_sanitize)
    if isinstance(item_to_sanitize, (str, int, float, bool, type(None))):
        return item_to_sanitize
    try:
        hash(item_to_sanitize)
        return str(item_to_sanitize)
    except TypeError:
        return str(item_to_sanitize)

# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

@tenacity_retry()
def _update_chunk_status_supabase(doc_id: str, update: Dict[str, Any]):
    allowed = {
        "annotation_status", "annotated_at", "keep", "annotation_tags",
        "indexing_status", "indexed_at", "annotation_reason", "status",
        "r2r_status", "r2r_document_id", "r2r_indexed_at", "r2r_error",
    }
    payload = {k: v for k, v in update.items() if k in allowed}
    
    if not payload:
        logger.debug(f"Nenhum payload válido para atualizar o status do Supabase para doc_id {doc_id}")
        return

    if "r2r_status" in payload:
        if payload["r2r_status"] == "success": # Alterado de "indexed" para "success" para corresponder à lógica de _upload_document_batch_to_r2r
            payload.setdefault("status", "completed") # "completed" significa que passou por R2R com sucesso
        elif payload["r2r_status"].startswith("failed"):
            payload.setdefault("status", "processing_failed")
        elif payload["r2r_status"].startswith("skipped"):
             payload.setdefault("status", "processed_with_r2r_skip")
    elif "annotation_status" in payload:
        if payload["annotation_status"] == "done" and payload.get("keep") is True:
            payload.setdefault("status", "annotated_kept") # Pronto para R2R
        elif payload["annotation_status"] == "done" and payload.get("keep") is False:
            payload.setdefault("status", "annotated_not_kept") # Não vai para R2R
        elif payload["annotation_status"] == "skipped":
            payload.setdefault("status", "annotation_skipped")
        elif payload["annotation_status"] == "annotation_failed": # Unificado "error" e "annotation_failed"
             payload.setdefault("status", "processing_failed")

    logger.debug(f"Atualizando Supabase para doc_id {doc_id} com payload: {payload}")
    try:
        supabase_client.table("documents").update(payload).eq("id", doc_id).execute()
        logger.info(f"Status do Supabase atualizado com sucesso para chunk id {doc_id}")
    except Exception as e:
        logger.error(f"Falha ao atualizar status do Supabase para chunk id {doc_id}: {e}", exc_info=True)
        raise

# Cache para evitar múltiplas buscas do mesmo chunk no Supabase
# chunk_cache = TTLCache(maxsize=1000, ttl=300) # Cache para 1000 chunks por 5 minutos # REMOVER CACHE POR AGORA

@tenacity_retry() # Adicionando o decorador de retry aqui também
def ensure_chunk_exists(chunk_id_supabase: str, chunk_data_for_insert: Optional[Dict[str, Any]] = None): # Adicionado argumento para dados de inserção
    """
    Garante que o chunk/documento existe na tabela 'documents' do Supabase.
    Se não existir e chunk_data_for_insert for fornecido, faz insert.
    O ID do chunk no Supabase é o campo 'id'.
    Retorna os dados do chunk se existir ou for criado, caso contrário None.
    """
    logger.debug(f"[ensure_chunk_exists] Chamado para ID: {chunk_id_supabase}")
    
    # Verifica se o chunk existe
    resp_select = supabase_client.table("documents").select("id").eq("id", chunk_id_supabase).limit(1).execute()
    
    if resp_select.data:
        logger.debug(f"[ensure_chunk_exists] Chunk {chunk_id_supabase} já existe no Supabase.")
        # Retorna um dict simples indicando existência, os dados completos serão do fetch_pending_chunks...
        return {"id": chunk_id_supabase, "exists": True}
    elif chunk_data_for_insert:
        logger.info(f"[ensure_chunk_exists] Chunk {chunk_id_supabase} não encontrado. Tentando inserir.")
        insert_data = {
            "id": chunk_id_supabase,
            "document_id": chunk_data_for_insert.get("document_id"), # Corrigido: source_document_id -> document_id
            # "source_file_name": chunk_data_for_insert.get("source_file_name"), # Presumindo que está em metadata
            # "source_type": chunk_data_for_insert.get("source_type"), # Presumindo que está em metadata
            # "user_id": chunk_data_for_insert.get("user_id"), # Presumindo que está em metadata ou não é mais usado diretamente
            "content": chunk_data_for_insert.get("content"), # Corrigido: chunk_content -> content
            "token_count": chunk_data_for_insert.get("token_count"),
            "chunk_index": chunk_data_for_insert.get("chunk_index"),
            "metadata": _sanitize_metadata(chunk_data_for_insert.get("metadata", {})),
            "status": chunk_data_for_insert.get("status", "pending_annotation"), 
            "annotation_status": chunk_data_for_insert.get("annotation_status"),
            "r2r_status": chunk_data_for_insert.get("r2r_status"),
            "annotation_tags": chunk_data_for_insert.get("annotation_tags"), # Adicionado
            "keep": chunk_data_for_insert.get("keep") # Adicionado
        }
        insert_data_cleaned = {k: v for k, v in insert_data.items() if v is not None}
        
        try:
            resp_insert = supabase_client.table("documents").insert(insert_data_cleaned).execute()
            logger.info(f"[ensure_chunk_exists] Chunk {chunk_id_supabase} inserido no Supabase.")
            if resp_insert.data:
                return resp_insert.data[0] 
            else:
                resp_after_insert = supabase_client.table("documents").select("id").eq("id", chunk_id_supabase).limit(1).execute()
                if resp_after_insert.data:
                    return {"id": chunk_id_supabase, "exists": True, "inserted_now": True} # Indica que foi inserido
                else:
                    logger.error(f"[ensure_chunk_exists] Chunk {chunk_id_supabase} não encontrado mesmo após tentativa de inserção.")
                    return None 
        except Exception as e_insert:
            logger.error(f"[ensure_chunk_exists] Falha ao inserir chunk {chunk_id_supabase}: {e_insert}", exc_info=True)
            raise
    else:
        logger.warning(f"[ensure_chunk_exists] Chunk {chunk_id_supabase} não encontrado e sem dados para inserção.")
        return None

# ---------------------------------------------------------------------------
# Chunk‑level helpers
# ---------------------------------------------------------------------------

@tenacity_retry()
def _run_annotation(annotator: AnnotatorAgent, chunk_input_dict: Dict[str, Any]) -> Optional[ChunkOut]:
    # chunk_input_dict deve conter 'text_content', 'id' (supabase_id), 'metadata'
    logger.debug(f"Chunk {chunk_input_dict.get('id', 'ID Desconhecido')} entrando em _run_annotation com temp_id (se existir): {chunk_input_dict.get('temp_id')}.")
    
    # O AnnotatorAgent internamente usa o 'id' do input_dict como 'temp_id' se 'temp_id' não estiver presente.
    # Então, garantir que o 'id' passado seja o supabase_id.
    if "id" not in chunk_input_dict:
         # Isso não deveria acontecer se process_single_chunk montar o dict corretamente.
        logger.error("_run_annotation: 'id' (supabase_id) faltando no chunk_input_dict. Isso é um erro.")
        return None # Não pode prosseguir sem um ID para o anotador.

    try:
        # O AnnotatorAgent.run espera um dicionário que se assemelhe a um ChunkInput
        # O 'id' no chunk_input_dict será usado como temp_id pelo agent
        annotation_output = annotator.run(chunk_input_dict) # Passa o dict diretamente
        if annotation_output:
            logger.debug(f"Chunk {chunk_input_dict.get('id')} anotado com sucesso via _run_annotation. Keep={annotation_output.keep}")
            return annotation_output
        else:
            logger.warning(f"Chunk {chunk_input_dict.get('id')} anotado retornou None. Marcando como erro de anotação.")
            return None
    except Exception as e:
        logger.error(f"Erro durante _run_annotation para {chunk_input_dict.get('id')}: {e}\nStack trace: {traceback.format_exc()}", exc_info=True)
        raise

@tenacity_retry()
async def _upload_document_batch_to_r2r(
    document_id_from_source: str, # Este é o ID do documento fonte (ex: GDrive ID ou Supabase document_id)
    list_of_supabase_chunk_dicts: List[Dict[str, Any]],
    # document_metadata_for_r2r_parent: Dict[str, Any], # Metadados para o Documento R2R PAI
    supabase_chunk_ids_in_batch: List[str] # IDs dos chunks Supabase que compõem este lote R2R
):
    if not r2r_client:
        logger.warning("R2R client não está disponível. Pulando upload para R2R.")
        for supabase_chunk_id in supabase_chunk_ids_in_batch:
            _update_chunk_status_supabase(supabase_chunk_id, {"r2r_status": "failed_client_unavailable", "status": "processing_failed", "r2r_error": "R2R client not available"})
        return # Não retorna valor, a atualização de status é o efeito colateral

    logger.info(f"Preparando para enviar {len(list_of_supabase_chunk_dicts)} chunks para R2R para o documento R2R PAI originado de '{document_id_from_source}'.")

    r2r_document_chunks_to_send: List[DocumentChunk] = []
    for supabase_chunk_dict in list_of_supabase_chunk_dicts:
        chunk_text = supabase_chunk_dict.get("content", "") # Corrigido
        supabase_id = str(supabase_chunk_dict.get("id"))

        if not chunk_text or chunk_text.isspace():
            logger.warning(f"Chunk Supabase ID {supabase_id} tem texto vazio. NÃO SERÁ ENVIADO para R2R.")
            _update_chunk_status_supabase(supabase_id, {"r2r_status": "skipped_empty_content", "status": "processing_failed", "r2r_error": "Chunk content is empty"})
            continue

        # Montar metadados para o R2R DocumentChunk
        # Estes metadados SÃO ESPECÍFICOS DO CHUNK enviado ao R2R.
        r2r_chunk_metadata = _sanitize_metadata(supabase_chunk_dict.get("metadata", {}).copy())
        r2r_chunk_metadata["supabase_chunk_id"] = supabase_id
        r2r_chunk_metadata["annotation_tags"] = _sanitize_metadata(supabase_chunk_dict.get("annotation_tags", [])) # Corrigido
        r2r_chunk_metadata["annotation_reason"] = supabase_chunk_dict.get("annotation_reason")
        # Adicionar o document_id_from_source (ex: GDrive ID) também aos metadados do chunk R2R para rastreabilidade
        r2r_chunk_metadata["document_id_source_system"] = document_id_from_source

        # A função `post_preprocessed_chunks` do R2RClientWrapper espera `document_id_original` como o ID do doc pai no R2R.
        
        # CORREÇÃO: Ajustar a criação do DocumentChunk para corresponder aos campos esperados pelo R2R.
        # Valores mockados para collection_ids e owner_id por enquanto.
        mock_collection_id = uuid.UUID("00000000-0000-0000-0000-000000000000")
        mock_owner_id = uuid.UUID("00000000-0000-0000-0000-000000000001")

        # Tentar converter document_id_from_source para UUID. 
        # Se falhar, pode indicar um problema fundamental na forma como estamos identificando documentos R2R.
        try:
            r2r_parent_document_uuid = uuid.UUID(document_id_from_source)
        except ValueError:
            logger.error(f"Falha ao converter document_id_from_source ('{document_id_from_source}') para UUID. Isso será um problema para o R2R.")
            # O que fazer aqui? Por agora, vamos deixar a exceção original do Pydantic ocorrer se a conversão falhar, 
            # ou podemos pular este lote e marcar como erro.
            # Optando por deixar seguir e ver o erro do Pydantic se ocorrer, para diagnóstico.
            # No entanto, uma abordagem mais robusta seria ter um mapeamento ou geração de UUIDs válidos para R2R.
            r2r_parent_document_uuid = document_id_from_source # Pode causar erro no DocumentChunk se não for UUID

        r2r_document_chunks_to_send.append(
            DocumentChunk(
                id=uuid.uuid4(),  # Campo 'id' do R2R DocumentChunk
                document_id=r2r_parent_document_uuid, # Campo 'document_id' do R2R DocumentChunk (ID do doc pai)
                collection_ids=[mock_collection_id], # Campo 'collection_ids' obrigatório
                owner_id=mock_owner_id, # Campo 'owner_id' obrigatório
                data=chunk_text,  # Campo 'data' do R2R DocumentChunk (anteriormente 'text')
                metadata=r2r_chunk_metadata
                # 'source_id' não é um parâmetro direto do construtor DocumentChunk do R2R,
                # mas o conceito é coberto por 'document_id' que é o ID do documento pai.
            )
        )

    if not r2r_document_chunks_to_send:
        logger.warning(f"Nenhum chunk válido para enviar ao R2R para o documento fonte ID '{document_id_from_source}' após a filtragem de vazios.")
        for supabase_chunk_id in supabase_chunk_ids_in_batch: # Marcar todos os originais do lote
            _update_chunk_status_supabase(supabase_chunk_id, {"r2r_status": "skipped_no_valid_chunks_in_batch", "status": "processed_with_r2r_skip", "r2r_error": "No valid chunks in the batch to send to R2R"})
        return

    try:
        # O `document_id_original` aqui é o ID do documento R2R "pai".
        # O `document_metadata_for_r2r_parent` seria para este documento pai.
        # A API atual `post_preprocessed_chunks` do R2RClientWrapper não parece aceitar explicitamente metadados para o doc pai.
        # Ela espera que o documento pai já exista ou o cria com base no primeiro chunk, se necessário.
        # Vamos passar document_id_from_source como o `document_id_original` para o R2RClientWrapper.
        logger.info(f"Enviando {len(r2r_document_chunks_to_send)} DocumentChunks para R2R. Documento R2R PAI (source_id nos chunks): '{document_id_from_source}'.")
        
        r2r_response = await r2r_client.async_post_preprocessed_chunks(
            document_id_original=document_id_from_source, # Este é o ID do Documento R2R "pai"
            document_chunks=r2r_document_chunks_to_send
        )
        logger.info(f"Resposta do R2R para o lote do doc_id {document_id_from_source}: {r2r_response}")
        
        # Assumindo que r2r_response é um dict que pode indicar sucesso/falha geral do lote.
        # Idealmente, R2RClientWrapper.post_preprocessed_chunks deveria retornar um status claro.
        # Por agora, se não houver exceção, consideramos sucesso para os chunks enviados.
        for supabase_chunk_dict_sent in list_of_supabase_chunk_dicts: # Iterar sobre os que FORAM considerados para envio
            sup_id = str(supabase_chunk_dict_sent.get("id"))
            # Apenas atualiza aqueles que tinham conteúdo (os vazios já foram marcados)
            if supabase_chunk_dict_sent.get("content", "").strip():
                 _update_chunk_status_supabase(sup_id, {"r2r_status": "success", "status": "completed", "indexed_at": datetime.now(timezone.utc).isoformat(), "r2r_error": None})

    except Exception as e:
        logger.error(f"Erro ao enviar lote para R2R (documento fonte ID '{document_id_from_source}'): {e}")
        logger.error(traceback.format_exc())
        for supabase_chunk_id in supabase_chunk_ids_in_batch: # Marcar todos do lote original como falha
            _update_chunk_status_supabase(supabase_chunk_id, {"r2r_status": "failed_upload", "status": "processing_failed", "r2r_error": str(e)})

# ---------------------------------------------------------------------------
# Processa um único chunk
# ---------------------------------------------------------------------------

def process_single_chunk(
    chunk_initial_data: Dict[str, Any],
    annotator: Optional[AnnotatorAgent],
    skip_annotation: bool,
    args_namespace: argparse.Namespace 
) -> Dict[str, Any]: # Retorna o chunk_initial_data atualizado
    chunk_supabase_id = str(chunk_initial_data.get("id"))
    current_chunk_content = chunk_initial_data.get("content") # Corrigido
    current_tags = chunk_initial_data.get("annotation_tags", []) # Corrigido
    
    if not chunk_supabase_id:
        logger.error("process_single_chunk: Chunk inicial sem ID. Pulando.")
        # Retornar o dado original com um status de erro implícito ou explícito?
        # Por agora, apenas loga e o fluxo principal pode pular.
        # Melhor seria ter um campo de erro no dict retornado.
        chunk_initial_data["_processing_error"] = "Missing Supabase ID"
        return chunk_initial_data # Retorna o dict original, que pode estar incompleto

    logger.info(f"Iniciando processamento (anotação) do chunk Supabase ID: {chunk_supabase_id}")

    # Não precisamos de ensure_chunk_exists aqui se fetch_pending_chunks_from_supabase já retorna dados completos.
    # A lógica de ensure_chunk_exists era mais para o pipeline de ingestão.
    # Aqui, assumimos que chunk_initial_data é o estado atual do Supabase.

    current_annotation_status = chunk_initial_data.get("annotation_status")
    
    # Definir valores padrão para o resultado da anotação
    annotation_status_val = current_annotation_status # Mantém o status atual se não anotar
    annotation_successful_val = chunk_initial_data.get("annotation_successful", False) # Mantém se já foi sucesso
    annotation_error_val = chunk_initial_data.get("annotation_error")
    keep_this_chunk = chunk_initial_data.get("keep", False) # Default para False se não anotado
    annotation_reason_val = chunk_initial_data.get("annotation_reason")
    # current_tags já foi definido acima

    if not skip_annotation and current_annotation_status in {None, "pending", "annotation_failed"}: # Unificado "error" e "annotation_failed"
        logger.debug(f"Chunk {chunk_supabase_id}: Tentando anotação (Status atual: {current_annotation_status}).")
        if not annotator:
            logger.warning(f"Chunk {chunk_supabase_id}: Annotator ausente – pulando anotação.")
            annotation_status_val = "skipped"
            # Se pulou, manter 'keep' como estava ou default para True?
            # Por segurança, se pulou anotação, default 'keep' para True para não perder dados para R2R
            keep_this_chunk = chunk_initial_data.get("keep") if chunk_initial_data.get("keep") is not None else True
            annotation_reason_val = "Annotation skipped due to missing annotator service."
        elif not current_chunk_content or current_chunk_content.isspace():
            logger.warning(f"Chunk {chunk_supabase_id}: Conteúdo vazio ou apenas espaços. Pulando anotação, marcando para NÃO MANTER.")
            annotation_status_val = "skipped_empty_content"
            keep_this_chunk = False
            annotation_reason_val = "Annotation skipped due to empty content."
        else:
            try:
                annotation_input_dict = {
                    "content": current_chunk_content,
                    "id": chunk_supabase_id, # Passa o supabase_id para o anotador
                    "temp_id": chunk_supabase_id, # temp_id para o AnnotatorAgent
                    "metadata": chunk_initial_data.get("metadata", {})
                }
                annotation_result = _run_annotation(annotator, annotation_input_dict)
                if annotation_result:
                    logger.info(f"Chunk {chunk_supabase_id}: Anotação bem-sucedida. Keep={annotation_result.keep}, Tags={annotation_result.tags}")
                    annotation_status_val = "done"
                    annotation_successful_val = True
                    keep_this_chunk = annotation_result.keep
                    # Merge tags: Adiciona novas tags do anotador às existentes, evitando duplicatas.
                    new_tags_from_annotator = annotation_result.tags if annotation_result.tags else []
                    if isinstance(current_tags, list):
                        for tag in new_tags_from_annotator:
                            if tag not in current_tags:
                                current_tags.append(tag)
                    else: # Se current_tags não era uma lista (ex: None), apenas atribui as novas
                        current_tags = new_tags_from_annotator
                    annotation_reason_val = annotation_result.reason
                    annotation_error_val = None # Limpa erro anterior
                else:
                    logger.warning(f"Chunk {chunk_supabase_id}: Anotação retornou None.")
                    annotation_status_val = "annotation_failed"
                    annotation_successful_val = False
                    keep_this_chunk = False # Default para não manter se anotação falhou
                    annotation_error_val = "Annotator returned None"
                    annotation_reason_val = annotation_error_val
            except Exception as exc_annotate:
                logger.error(f"Chunk {chunk_supabase_id}: Erro FINAL durante anotação: {exc_annotate}", exc_info=True)
                annotation_status_val = "annotation_failed"
                annotation_successful_val = False
                keep_this_chunk = False
                annotation_error_val = f"Annotation exception: {str(exc_annotate)}"
                annotation_reason_val = annotation_error_val
    elif skip_annotation and current_annotation_status in {None, "pending", "annotation_failed"}:
        logger.debug(f"Chunk {chunk_supabase_id}: Anotação pulada por flag. Marcando como 'skipped'.")
        annotation_status_val = "skipped"
        keep_this_chunk = chunk_initial_data.get("keep") if chunk_initial_data.get("keep") is not None else True # Default keep=True se pula anotação
        annotation_reason_val = "Annotation skipped by --skip_annotation flag."
    else:
        logger.debug(f"Chunk {chunk_supabase_id}: Anotação não necessária (Status: {current_annotation_status}, SkipFlag: {skip_annotation}) ou já processada.")
        if current_annotation_status == "done": # Se já estava 'done'
            annotation_successful_val = True # Assume que foi sucesso
            keep_this_chunk = chunk_initial_data.get("keep", False) # Respeita o valor de 'keep' existente

    # Monta o dicionário de resultado, incluindo o chunk original e os resultados da anotação
    chunk_initial_data["keep"] = keep_this_chunk
    chunk_initial_data["annotation_tags"] = _sanitize_metadata(current_tags) # Corrigido e sanitizado
    chunk_initial_data["annotation_status"] = annotation_status_val
    chunk_initial_data["annotation_successful"] = annotation_successful_val
    chunk_initial_data["annotation_error"] = annotation_error_val
    chunk_initial_data["annotation_reason"] = annotation_reason_val
    if annotation_status_val != current_annotation_status : # Se o status da anotação mudou
        chunk_initial_data["annotated_at"] = datetime.now(timezone.utc).isoformat()

    # Atualiza o status no Supabase com os resultados da fase de anotação
    _update_chunk_status_supabase(chunk_supabase_id, {
        "annotation_status": chunk_initial_data["annotation_status"],
        "annotated_at": chunk_initial_data.get("annotated_at"), # Pode ser None se não mudou
        "keep": chunk_initial_data["keep"],
        "annotation_tags": chunk_initial_data["annotation_tags"],
        "annotation_reason": chunk_initial_data["annotation_reason"],
        # "status" será definido por _update_chunk_status_supabase baseado em annotation_status
    })
    
    logger.info(f"Processamento (fase de anotação) do chunk {chunk_supabase_id} concluído. Keep={chunk_initial_data['keep']}, Status Anotação={chunk_initial_data['annotation_status']}")
    return chunk_initial_data

# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

async def run_pipeline(
    supabase_client: Client,
    r2r_client_wrapper: R2RClientWrapper, # Não usado diretamente, mas o global r2r_client é
    gdrive_service: Any, 
    args: argparse.Namespace
):
    logger.info(f"Executando ETL com argumentos: {args}")
    
    annotator_service = None
    if not args.skip_annotation:
        try:
            annotator_service = AnnotatorAgent() 
            logger.info("AnnotatorAgent instanciado com sucesso.")
        except Exception as e_annotator:
            logger.error(f"Falha ao inicializar AnnotatorAgent: {e_annotator}", exc_info=True)
            logger.error("A anotação será ignorada.")
    else:
        logger.info("Anotação pulada conforme argumento --skip_annotation.")

    if annotator_service is None and not args.skip_annotation:
        logger.warning("AnnotatorAgent NÃO foi instanciado. Anotação será pulada.")

    source_id_to_r2r_doc_id_map: Dict[str, str] = {} # Mapeia ID da fonte original para ID do Documento R2R "pai"

    total_chunks_processed_in_run: int = 0
    total_chunks_successfully_annotated_in_run: int = 0
    total_chunks_submitted_to_r2r_in_run: int = 0
    total_errors_in_annotation_phase_run: int = 0 # Renomeado para clareza
    # total_r2r_submission_errors_in_run: int = 0 # Removido, _upload_document_batch_to_r2r lida com status
    total_batches_processed: int = 0
    overall_start_time = time.time()
    gdrive_file_id_cache = TTLCache(maxsize=1000, ttl=3600) # Usado por fetch_pending_chunks...
    
    while True:
        batch_start_time = time.time()
        total_batches_processed += 1
        logger.info(f"--- Iniciando Lote {total_batches_processed} ---")

        # 1. Buscar chunks pendentes do Supabase
        # fetch_pending_chunks_from_supabase já usa os nomes de coluna corretos internamente
        chunks_to_process_this_batch = await fetch_pending_chunks_from_supabase(
            supabase_client,
            limit=args.batch_size,
            document_id_to_reprocess=args.source_doc_id_to_reprocess, # Passa o nome correto do arg
            gdrive_file_id_cache=gdrive_file_id_cache,
            reprocess_supabase_annotations=args.reprocess_supabase_annotations
        )

        if not chunks_to_process_this_batch:
            logger.info("Nenhum chunk pendente encontrado para o lote. Finalizando o processamento.")
            break

        logger.info(f"Lote {total_batches_processed}: Encontrados {len(chunks_to_process_this_batch)} chunks para processar (fase de anotação).")

        # 2. Fase de Anotação (paralela)
        annotated_chunks_for_r2r: List[Dict[str, Any]] = []
        errors_in_this_batch_annotation = 0

        with ThreadPoolExecutor(max_workers=args.max_workers, thread_name_prefix="AnnotWorker") as executor:
            future_to_chunk_id = {
                executor.submit(process_single_chunk, chunk_data, annotator_service, args.skip_annotation, args): str(chunk_data.get("id"))
                for chunk_data in chunks_to_process_this_batch if isinstance(chunk_data, dict) and 'id' in chunk_data
            }
            
            for future in as_completed(future_to_chunk_id):
                supabase_id_processed = future_to_chunk_id[future]
                total_chunks_processed_in_run +=1
                try:
                    processed_chunk_dict = future.result() # Retorna o chunk_initial_data atualizado
                    if processed_chunk_dict.get("annotation_successful", False):
                        total_chunks_successfully_annotated_in_run += 1
                    if processed_chunk_dict.get("_processing_error"): # Verifica erro interno de process_single_chunk
                        logger.error(f"Erro explícito de process_single_chunk para {supabase_id_processed}: {processed_chunk_dict['_processing_error']}")
                        errors_in_this_batch_annotation +=1
                    
                    # Adiciona à lista para R2R se keep=True e não houve erro explícito
                    if processed_chunk_dict.get("keep") is True and not processed_chunk_dict.get("_processing_error"):
                        annotated_chunks_for_r2r.append(processed_chunk_dict)
                    elif not processed_chunk_dict.get("keep") is True : # Se keep é False ou None
                         logger.info(f"Chunk Supabase ID {supabase_id_processed} não será enviado para R2R (keep={processed_chunk_dict.get('keep')}).")
                    
                except Exception as e_thread:
                    logger.error(f"Erro INESPERADO ao processar chunk Supabase ID {supabase_id_processed} na thread de anotação: {e_thread}", exc_info=True)
                    errors_in_this_batch_annotation +=1
        
        total_errors_in_annotation_phase_run += errors_in_this_batch_annotation
        logger.info(f"Lote {total_batches_processed} - Fase de Anotação Concluída: {len(chunks_to_process_this_batch)} tentados, {total_chunks_successfully_annotated_in_run} acumulado sucesso, {errors_in_this_batch_annotation} erros neste lote.")

        # 3. Fase de Indexação R2R (agrupado por documento original)
        if not args.skip_r2r_indexing and annotated_chunks_for_r2r:
            logger.info(f"Lote {total_batches_processed}: Iniciando fase de indexação R2R para {len(annotated_chunks_for_r2r)} chunks pós-anotação (keep=True).")
            
            # Agrupa chunks pelo document_id (que é o ID do documento fonte no Supabase)
            # Este document_id será usado para gerar/obter o ID do Documento R2R "Pai"
            chunks_grouped_by_source_doc_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for chunk_dict_for_r2r in annotated_chunks_for_r2r:
                # Usa o 'document_id' do Supabase (que é o ID do documento fonte) para agrupar
                source_doc_id = str(chunk_dict_for_r2r.get("document_id", "")) # Corrigido
                if not source_doc_id:
                    logger.warning(f"Chunk Supabase ID {chunk_dict_for_r2r.get('id')} não tem 'document_id'. Usando Supabase ID como agrupador R2R.")
                    source_doc_id = str(chunk_dict_for_r2r.get("id"))
                chunks_grouped_by_source_doc_id[source_doc_id].append(chunk_dict_for_r2r)

            for r2r_parent_doc_source_id, chunks_for_this_r2r_doc in chunks_grouped_by_source_doc_id.items():
                num_chunks_for_this_parent = len(chunks_for_this_r2r_doc)
                logger.info(f"Lote {total_batches_processed}: Enviando {num_chunks_for_this_parent} chunks para R2R. Documento R2R PAI originado de source_id '{r2r_parent_doc_source_id}'.")
                
                supabase_ids_for_this_r2r_doc = [str(ch.get("id")) for ch in chunks_for_this_r2r_doc]

                await _upload_document_batch_to_r2r(
                    document_id_from_source=r2r_parent_doc_source_id, # ID do documento fonte para R2R nomear/agrupar
                    list_of_supabase_chunk_dicts=chunks_for_this_r2r_doc,
                    supabase_chunk_ids_in_batch=supabase_ids_for_this_r2r_doc
                )
                total_chunks_submitted_to_r2r_in_run += num_chunks_for_this_parent # Contar os que foram para a função de upload
        elif args.skip_r2r_indexing:
            logger.info(f"Lote {total_batches_processed}: Indexação R2R pulada por flag.")
        else:
            logger.info(f"Lote {total_batches_processed}: Nenhum chunk elegível (keep=True) para R2R neste lote.")
        
        batch_duration = time.time() - batch_start_time
        logger.info(f"--- Fim do Lote {total_batches_processed} (Duração: {batch_duration:.2f}s) ---")
        logger.info(f"  Resumo Lote Anotação: {len(chunks_to_process_this_batch)} tentados, {errors_in_this_batch_annotation} erros.")
        logger.info(f"Progresso Acumulado (após Lote {total_batches_processed}):")
        logger.info(f"  Total Chunks Processados (anotação): {total_chunks_processed_in_run}")
        logger.info(f"  Total Anotações OK: {total_chunks_successfully_annotated_in_run}")
        logger.info(f"  Total Erros (fase anotação): {total_errors_in_annotation_phase_run}")
        logger.info(f"  Total Chunks Tentados para R2R: {total_chunks_submitted_to_r2r_in_run}")
        
    overall_duration = time.time() - overall_start_time
    logger.info("--- Processamento de Todos os Lotes Concluído ---")
    actual_batches_with_data = total_batches_processed -1 if not chunks_to_process_this_batch and total_batches_processed > 0 else total_batches_processed
    logger.info(f"  Total de Lotes Tentados: {actual_batches_with_data}")
    logger.info(f"  Total de Chunks Processados (anotação): {total_chunks_processed_in_run}")
    logger.info(f"  Total de Chunks Anotados com Sucesso: {total_chunks_successfully_annotated_in_run}")
    logger.info(f"  Total de Erros na Fase de Anotação: {total_errors_in_annotation_phase_run}")
    logger.info(f"  Total de Chunks Submetidos ao R2R (tentativas): {total_chunks_submitted_to_r2r_in_run}")
    logger.info(f"  Duração Total da Execução: {overall_duration:.2f} segundos.")
    logger.info("Pipeline finalizado.")

async def fetch_pending_chunks_from_supabase(
    client: Client,
    limit: int = 10,
    document_id_to_reprocess: Optional[str] = None, # ID do documento Supabase para reprocessar
    gdrive_file_id_cache: Optional[TTLCache] = None, # Não usado diretamente nesta função, mas pode ser útil em `run_pipeline`
    reprocess_supabase_annotations: bool = False,
) -> List[Dict[str, Any]]:
    logger.info(f"Buscando chunks pendentes: limit={limit}, document_id_to_reprocess={document_id_to_reprocess}, reprocess_supabase_annotations={reprocess_supabase_annotations}")
    
    # Colunas a serem selecionadas. Usar nomes corretos.
    select_columns = (
        "id, document_id, content, metadata, annotation_tags, keep, " # Corrigido: chunk_content -> content, original_document_id -> document_id, tags -> annotation_tags
        "annotation_reason, token_count, created_at, updated_at, "
        "annotation_status, status, " # Removido indexing_status, r2r_indexed_at (usar r2r_status)
        "r2r_document_id, r2r_status, r2r_error, chunk_index, annotated_at" # Adicionado annotated_at para consistência
    )
    query = client.table("documents").select(select_columns, count="exact") # Adicionado count="exact"

    if document_id_to_reprocess:
        # Filtra pelo document_id do Supabase.
        # Se document_id_to_reprocess é um GDrive ID, precisaria de um passo anterior para mapeá-lo para document_ids do Supabase.
        # Assumindo aqui que document_id_to_reprocess é o Supabase document_id.
        query = query.eq("document_id", document_id_to_reprocess) 
        logger.info(f"Buscando todos os chunks para reprocessamento do Supabase document_id: {document_id_to_reprocess}")
        # Não filtra por status "reprocessed_skipped_all" aqui, pois o reprocessamento pode querer pegar tudo.
        # A lógica de pular ou não o reprocessamento fica em process_single_chunk.
    else:
        # Construindo a query OR
        # Condições para buscar chunks que precisam de atenção (anotação ou indexação)
        # ou que falharam em etapas anteriores.
        # CORREÇÃO: Remover vírgulas do final de cada string de filtro individual.
        filters = (
            "status.eq.pending_annotation", 
            "status.eq.pending_indexing",
            "annotation_status.is.null", 
            "annotation_status.eq.pending", 
            "annotation_status.eq.annotation_failed", 
            "r2r_status.is.null", 
            "r2r_status.eq.pending", 
            "r2r_status.like.failed%" 
        )
        
        if reprocess_supabase_annotations:
            logger.warning(f"REPROCESS_SUPABASE_ANNOTATIONS ativado. Buscando chunks com annotation_status != 'done' e keep != False, ignorando status de R2R.")
            # Adicionando condições para re-anotação
            filters += ("annotation_status.ne.done", "keep.ne.False")
        
        query = query.or_(",".join(filters)) # A junção com vírgula aqui é a forma correta de passar para o .or_()

    query = query.order("updated_at", desc=False).limit(limit) # Processar os mais antigos primeiro

    try:
        response = await asyncio.to_thread(query.execute) 
    except PostgrestAPIError as e:
        logger.error(f"Erro PostgrestAPIError ao buscar chunks pendentes: {e.message} (Code: {e.code}, Details: {e.details}, Hint: {e.hint})")
        return []
    except Exception as e:
        logger.error(f"Erro inesperado ao buscar chunks pendentes: {e}", exc_info=True)
        return []

    if response.data:
        logger.info(f"Encontrados {response.count if response.count is not None else len(response.data)} chunks no Supabase que correspondem aos critérios (retornando {len(response.data)}).")
        # Lógica de gdrive_file_id_cache removida daqui, pois não parece ser o local correto para popular/usar.
        # Se necessário, deve ser tratado no nível de ingestão ou antes de chamar esta função.
        return response.data
    else:
        logger.info("Nenhum chunk pendente encontrado no Supabase com os critérios atuais.")
        return []

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logger.info("--- SCRIPT VERSION: 2024-05-16 12:00 PM ---") # Adicionando log de versão
    start_time_main = time.time()
    parser = argparse.ArgumentParser(description="Pipeline ETL para Google Drive, Supabase e R2R.")
    parser.add_argument("--skip_gdrive_ingest", action="store_true", help="Pular ingestão do GDrive.")
    parser.add_argument("--skip_annotation", action="store_true", help="Pular anotação. Chunks keep=True por padrão.")
    parser.add_argument("--skip_r2r_indexing", action="store_true", help="Pular indexação R2R.")
    parser.add_argument(
        "--source-doc-id-to-reprocess", type=str, default=None,
        help="ID do documento Supabase (campo 'document_id') para reprocessar todos os seus chunks."
    )
    parser.add_argument(
        "--reprocess-supabase-annotations", action="store_true",
        help="Força re-anotação de chunks do Supabase (usado com --source-doc-id-to-reprocess)."
    )
    parser.add_argument("--batch_size", type=int, default=os.getenv("ETL_BATCH_SIZE", 20), help="Batch size para Supabase.") # Default menor para depuração
    parser.add_argument("--max_workers", type=int, default=8, help="Max workers para anotação.")
    parser.add_argument("--no-cache-crewai", action="store_true", default=False, help="Desabilitar cache CrewAI.")
    args = parser.parse_args()

    try:
        logger.info("Executando ETL com argumentos: %s", args)
        asyncio.run(run_pipeline(
            supabase_client=supabase_client, 
            r2r_client_wrapper=r2r_client, # r2r_client é o wrapper global   
            gdrive_service=None, # gdrive_service não é usado diretamente no pipeline de chunks
            args=args
        ))
    except KeyboardInterrupt:
        logger.info("Processo interrompido pelo usuário.")
    except Exception as e_main:
        logger.error(f"Erro fatal no script principal: {e_main}", exc_info=True)
    finally:
        if r2r_client and hasattr(r2r_client, 'close') and callable(r2r_client.close): 
            try:
                logger.info("Tentando fechar a sessão do cliente R2R...")
                r2r_client.close() 
                logger.info("Sessão do cliente R2R fechada.")
            except Exception as e_close:
                logger.warning(f"Erro ao fechar a sessão do cliente R2R: {e_close}")
        
        end_time_main = time.time()
        total_duration_main = end_time_main - start_time_main
        logger.info(f"Execução do script finalizada. Duração total: {total_duration_main:.2f} segundos.")

if __name__ == "__main__":
    main() 