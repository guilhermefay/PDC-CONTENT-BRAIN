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
import httpx

# Adiciona o diretório raiz do projeto ao PYTHONPATH
# Isso garante que módulos como 'agents', 'infra', etc., sejam encontrados
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Agora podemos importar os módulos locais
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
    postgrest_client_timeout=300,  # Timeout de 5 minutos para operações do PostgREST
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
                # ... (lógica de fallback para timeout, com flush=True) ...
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

RETRYABLE_EXCEPTIONS = (ConnectionError, TimeoutError, PostgrestAPIError)

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
        clean_dict: Dict[str, Any] = {}
        for k, v_item in item_to_sanitize.items():
            clean_dict[k] = _sanitize_metadata(v_item) # Sanitiza o valor recursivamente
        return clean_dict
    elif isinstance(item_to_sanitize, list):
        clean_list: List[Any] = []
        for v_item in item_to_sanitize:
            clean_list.append(_sanitize_metadata(v_item)) # Sanitiza o item da lista recursivamente
        return clean_list
    elif isinstance(item_to_sanitize, slice):
        return str(item_to_sanitize)
    else:
        # Para todos os outros tipos, verifica se é serializável em JSON.
        # Tipos básicos (str, int, float, bool, None) são ok.
        # Para outros objetos, a conversão para string é mais segura.
        if isinstance(item_to_sanitize, (str, int, float, bool, type(None))):
            return item_to_sanitize
        else:
            # Tenta converter para string se não for um tipo JSON básico ou se falhar o hash (indicando objeto complexo).
            try:
                # Esta chamada de hash é apenas para forçar um TypeError se o objeto for complexo e não hasheável.
                # Não usamos o resultado do hash.
                hash(item_to_sanitize)
                # Se for hasheável mas não um tipo JSON básico (ex: datetime), converte para str.
                return str(item_to_sanitize)
            except TypeError:
                # Não hasheável, converte para string.
                return str(item_to_sanitize)

# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

@tenacity_retry()
def _update_chunk_status_supabase(doc_id: str, update: Dict[str, Any]):
    allowed = {
        "annotation_status",
        "annotated_at",
        "keep",
        "annotation_tags",
        "indexing_status",
        "indexed_at",
        "annotation_reason",
    }
    payload = {k: v for k, v in update.items() if k in allowed and v is not None}
    if not payload:
        logger.debug(f"Nenhum payload válido para atualizar o status do Supabase para doc_id {doc_id}")
        return

    logger.debug(f"Atualizando Supabase para doc_id {doc_id} com payload: {payload}")
    try:
        supabase_client.table("documents").update(payload).eq("document_id", doc_id).execute()
        logger.info(f"Status do Supabase atualizado com sucesso para doc_id {doc_id}")
    except Exception as e:
        logger.error(f"Falha ao atualizar status do Supabase para doc_id {doc_id}: {e}", exc_info=True)
        # Re-lançar a exceção para que a retentativa do tenacity funcione
        raise

# ---------------------------------------------------------------------------
# Chunk‑level helpers
# ---------------------------------------------------------------------------

@tenacity_retry()
def _run_annotation(annotator: AnnotatorAgent, chunk: Dict[str, Any]) -> Optional[ChunkOut]:
    logger.debug(f"Executando anotação para o chunk {chunk.get('document_id', 'ID Desconhecido')}")
    if 'metadata' in chunk and chunk['metadata']:
        chunk['metadata'] = _sanitize_metadata(chunk['metadata'])
    try:
        result = annotator.run(chunk)
        logger.debug(f"Resultado da anotação para {chunk.get('document_id', 'ID Desconhecido')}: Keep={result.keep if result else None}")
        return result
    except Exception as e:
        logger.error(f"Erro durante _run_annotation para {chunk.get('document_id', 'ID Desconhecido')}: {e}\nStack trace: {traceback.format_exc()}\nContexto: {json.dumps({k: chunk.get(k) for k in ['document_id','annotation_status','indexing_status','keep','metadata']}, default=str)}", exc_info=True)
        raise

@tenacity_retry()
async def _upload_chunk_r2r(chunk: Dict[str, Any]):
    # Renomear doc_id para current_chunk_specific_id para clareza
    current_chunk_specific_id = chunk.get("document_id", "temp_chunk_id_" + str(uuid.uuid4()))
    logger.info(f"[UPLOAD_API] Iniciando tentativa de envio de chunk {current_chunk_specific_id} para API R2R.")
    
    if not r2r_client:
        logger.warning("R2R client (wrapper) não está disponível. Pulando envio de chunks para API.")
        return None # Retornar None para indicar que não houve tentativa ou falha

    try:
        # Os chunks já estão no formato List[Dict[str, Any]] com 'content'?
        # O payload da API espera: {"document_id": ..., "chunks": [{"content": "..."}], "metadata": ...}
        # Se 'chunk' aqui representa um *único* chunk que precisa ser enviado,
        # precisamos envolvê-lo em uma lista para o método post_preprocessed_chunks_to_api_service.
        
        chunk_content_item = {"content": chunk.get("text_content", chunk.get("content", ""))} 
        if not chunk_content_item["content"]:
            logger.error(f"[UPLOAD_API] Chunk {current_chunk_specific_id} não possui campo 'text_content' ou 'content'. Pulando envio.")
            return None

        # Obter o ID do documento original dos metadados
        # Este é o ID que agrupa todos os chunks de um mesmo arquivo de origem.
        original_doc_id_from_metadata = chunk.get("metadata", {}).get("original_document_id")

        if not original_doc_id_from_metadata:
            logger.error(f"[UPLOAD_API] 'original_document_id' não encontrado nos metadados do chunk {current_chunk_specific_id}. Não é possível enviar para API R2R.")
            return None # Crucial para agrupamento correto no R2R

        logger.info(f"[UPLOAD_API] Documento original para o chunk {current_chunk_specific_id} é {original_doc_id_from_metadata}. Preparando para enviar via R2R API.")

        chunks_to_send = [chunk_content_item]
        metadata_to_send = _sanitize_metadata(chunk.get("metadata", {}))
        # Garantir que o ID específico do chunk esteja nos metadados
        metadata_to_send.setdefault("chunk_specific_id", current_chunk_specific_id)
        # O 'original_document_id' já deve estar em metadata_to_send se veio de chunk.get("metadata", {})
        # 'source' também deve vir dos metadados originais do chunk.
        metadata_to_send.setdefault("source", chunk.get("metadata", {}).get("source_name", "unknown"))

        response_data = await r2r_client.post_preprocessed_chunks_to_api_service(
            document_id=original_doc_id_from_metadata, # <<< USA O ID ORIGINAL AQUI
            chunks_data=chunks_to_send,
            metadata=metadata_to_send
        )

        if response_data and response_data.get("success"):
            logger.info(f"[UPLOAD_API] Envio de chunks para {current_chunk_specific_id} bem-sucedido. Resposta da API: {response_data.get('response')}")
            return response_data # Retorna o dict de sucesso com a resposta da API
        else:
            error_msg = response_data.get("error", "Erro desconhecido ao enviar chunks para API") if response_data else "Nenhuma resposta do R2RClientWrapper"
            logger.error(f"[UPLOAD_API] Erro ao enviar chunks para {current_chunk_specific_id} para API: {error_msg}")
            # Não levantar exceção aqui para que o tenacity possa tentar novamente se for um erro de rede
            # Se for um erro de lógica/payload, as tentativas não ajudarão, mas o log registrará.
            return None # Indicar falha

    except Exception as e:
        logger.error(f"[UPLOAD_API] Exceção inesperada ao enviar chunks para {current_chunk_specific_id} para API: {e}", exc_info=True)
        # Logar o contexto do chunk para depuração
        context_log = {k: chunk.get(k) for k in ['document_id', 'annotation_status', 'indexing_status', 'keep', 'metadata'] if k in chunk}
        logger.error(f"Contexto do chunk no momento do erro: {json.dumps(context_log, default=str)}")
        return None # Indicar falha

def ensure_chunk_exists(chunk):
    """
    Garante que o chunk/documento existe na tabela 'documents'.
    Se não existir, faz insert.
    """
    doc_id = chunk.get("document_id")
    # Busca por document_id
    resp = supabase_client.table("documents").select("document_id").eq("document_id", doc_id).limit(1).execute()
    if not resp.data:
        # Não existe, faz insert
        logger.info(f"[INSERT] Criando novo chunk/documento no Supabase: {doc_id}")
        # Remove campos não persistíveis se necessário
        insert_chunk = {k: v for k, v in chunk.items() if k != "chunk_index"} # chunk_index só em metadata
        supabase_client.table("documents").insert(insert_chunk).execute()
    else:
        logger.debug(f"[EXISTE] Chunk/documento já existe no Supabase: {doc_id}")

# ---------------------------------------------------------------------------
# Processa um único chunk
# ---------------------------------------------------------------------------

def process_single_chunk(
    chunk: Dict[str, Any],
    annotator: Optional[AnnotatorAgent],
    skip_annotation: bool,
    skip_indexing: bool,
):
    """
    Processa um único chunk: anota (opcional) e indexa (opcional).
    Garante que o chunk exista na tabela antes de updates.

    Args:
        chunk: Dicionário com os dados do chunk a ser processado
        annotator: Instância do AnnotatorAgent ou None
        skip_annotation: Flag para pular etapa de anotação
        skip_indexing: Flag para pular etapa de indexação
    """
    # --- Garantia sênior: chunk/documento sempre existe ---
    ensure_chunk_exists(chunk)
    doc_id = chunk.get("document_id", f"id_ausente_{uuid.uuid4()}") # Usar get com fallback
    logger.info(f"Iniciando processamento do chunk: {doc_id}")
    current_annotation_status = chunk.get("annotation_status")
    current_indexing_status = chunk.get("indexing_status")
    keep_chunk = chunk.get("keep") # Pode ser None, True ou False
    # ---------------- Anotação (Com updates reativados e try/except corrigido) ----------------
    if not skip_annotation and current_annotation_status in {None, "pending", "error"}:
        logger.debug(f"Chunk {doc_id}: Tentando anotação (Status atual: {current_annotation_status}).")
        if not annotator:
            logger.warning(f"Chunk {doc_id}: Annotator ausente – pulando anotação.")
            update = {"annotation_status": "skipped", "annotated_at": datetime.now(timezone.utc).isoformat()}
            _update_chunk_status_supabase(doc_id, update)
            current_annotation_status = "skipped"
        else:
            try:
                logger.info(f"VERIFICANDO TIPO DE 'chunk' ANTES DE annotator.run: {type(chunk).__name__}")
                if not isinstance(chunk, dict):
                    logger.error(f"ERRO CRÍTICO: 'chunk' NÃO é um dicionário antes de chamar annotator.run! Valor: {chunk}")
                    raise TypeError(f"'chunk' should be a dict, but got {type(chunk).__name__}")
                result = _run_annotation(annotator, chunk)
                if result:
                    logger.info(f"Chunk {doc_id}: Anotação bem-sucedida. Keep={result.keep}, Tags={result.tags}")
                    update = {
                        "annotation_status": "done",
                        "annotated_at": datetime.now(timezone.utc).isoformat(),
                        "keep": result.keep,
                        "annotation_tags": result.tags,
                    }
                    _update_chunk_status_supabase(doc_id, update)
                    keep_chunk = result.keep
                    current_annotation_status = "done"
                else:
                    logger.warning(f"Chunk {doc_id}: Anotação retornou None. Marcando como erro.")
                    update = {"annotation_status": "error", "annotated_at": datetime.now(timezone.utc).isoformat(), "keep": False}
                    _update_chunk_status_supabase(doc_id, update)
                    keep_chunk = False
                    current_annotation_status = "error"
            except Exception as exc:
                logger.error(f"Chunk {doc_id}: Erro FINAL durante anotação: {exc}\nStack trace: {traceback.format_exc()}\nContexto: {json.dumps({k: chunk.get(k) for k in ['document_id','annotation_status','indexing_status','keep','metadata']}, default=str)}", exc_info=True)
                update = {"annotation_status": "error", "annotated_at": datetime.now(timezone.utc).isoformat(), "keep": False}
                _update_chunk_status_supabase(doc_id, update)
                keep_chunk = False
                current_annotation_status = "error"
    elif skip_annotation:
        logger.debug(f"Chunk {doc_id}: Anotação pulada por flag.")
    else:
        logger.debug(f"Chunk {doc_id}: Anotação não necessária (Status: {current_annotation_status}).")
    # ---------------- Indexação (Com updates reativados) ----------------
    if (
        not skip_indexing
        and keep_chunk is True
        and current_indexing_status in {None, "pending", "error"}
    ):
        logger.debug(f"Chunk {doc_id}: Tentando indexação (Keep={keep_chunk}, Status atual: {current_indexing_status}).")
        try:
            # MODIFICADO: Usar asyncio.run para chamar a função async
            upload_result = asyncio.run(_upload_chunk_r2r(chunk))
            
            # Verificar o resultado do upload
            if upload_result and upload_result.get("success"):
                logger.info(f"Chunk {doc_id}: Indexação (envio para API) bem-sucedida.")
                update = {
                    "indexing_status": "done",
                    "indexed_at": datetime.now(timezone.utc).isoformat(),
                }
                _update_chunk_status_supabase(doc_id, update)
                current_indexing_status = "done"
            else:
                logger.error(f"Chunk {doc_id}: Falha na indexação (envio para API). Resultado: {upload_result}")
                update = {"indexing_status": "error", "indexed_at": datetime.now(timezone.utc).isoformat()}
                _update_chunk_status_supabase(doc_id, update)
                current_indexing_status = "error"
        except Exception as exc:
            logger.error(f"Chunk {doc_id}: Erro FINAL durante indexação (chamada asyncio.run): {exc}\nStack trace: {traceback.format_exc()}\nContexto: {json.dumps({k: chunk.get(k) for k in ['document_id','annotation_status','indexing_status','keep','metadata']}, default=str)}", exc_info=True)
            update = {"indexing_status": "error", "indexed_at": datetime.now(timezone.utc).isoformat()}
            _update_chunk_status_supabase(doc_id, update)
            current_indexing_status = "error"
    
    # Bloco 2: Indexação pulada por flag
    elif skip_indexing:
        logger.debug(f"Chunk {doc_id}: Indexação pulada por flag.")
        # Opcional: Marcar como skipped no DB se o status atual for 'pending' ou 'error'
        if current_indexing_status in {None, "pending", "error"}:
            update = {"indexing_status": "skipped"}
            _update_chunk_status_supabase(doc_id, update)
    
    # Bloco 3: Indexação não necessária porque keep=False
    elif keep_chunk is not True:
        logger.debug(f"Chunk {doc_id}: Indexação não necessária (Keep={keep_chunk}).")
        # Opcional: Marcar como skipped no DB se o status atual for 'pending' ou 'error'
        if current_indexing_status in {None, "pending", "error"}:
            update = {"indexing_status": "skipped"}
            _update_chunk_status_supabase(doc_id, update)
    
    # Bloco 4: Indexação não necessária pelo status atual
    else:
        logger.debug(f"Chunk {doc_id}: Indexação não necessária (Status: {current_indexing_status}).")

    logger.info(f"Processamento do chunk {doc_id} concluído.")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_pipeline(batch_size: int, max_workers: int, skip_annotation: bool, skip_indexing: bool):
    start_time = time.time()
    logger.info(f"Iniciando pipeline ETL... Batch={batch_size}, Workers={max_workers}, SkipAnnotation={skip_annotation}, SkipIndexing={skip_indexing}")

    annotator = None
    if not skip_annotation:
        try:
            annotator = AnnotatorAgent()
            logger.info("AnnotatorAgent inicializado.")
        except Exception as e_annotator:
            logger.error(f"Falha ao inicializar AnnotatorAgent: {e_annotator}. Anotação será pulada.", exc_info=True)
            skip_annotation = True # Força o skip se não conseguir inicializar

    try:
        logger.info(f"Buscando até {batch_size} chunks pendentes ou com erro...")
        query = supabase_client.table("documents").select("*") # Começa a query

        # Aplica o filtro OR
        query = query.or_(
            "annotation_status.eq.pending," 
            "annotation_status.eq.error," 
            "indexing_status.eq.pending," 
            "indexing_status.eq.error," 
            "annotation_status.is.null," 
            "indexing_status.is.null"
        )
        
        # Limita e executa
        resp = query.limit(batch_size).execute()
        
        chunks: List[Dict[str, Any]] = resp.data or []
    except Exception as e_fetch:
        logger.error(f"Erro ao buscar chunks do Supabase: {e_fetch}", exc_info=True)
        return # Aborta o pipeline se não conseguir buscar chunks

    if not chunks:
        logger.info("Nenhum chunk encontrado para processamento.")
        return

    logger.info(f"Processando {len(chunks)} chunks encontrados...")
    processed_count = 0
    failed_chunks = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # Mapeia future para chunk para facilitar o log de erros
        future_to_chunk = {pool.submit(process_single_chunk, c, annotator, skip_annotation, skip_indexing): c for c in chunks}

        for future in as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            doc_id = chunk.get("document_id", "ID Desconhecido")
            try:
                future.result()  # Pega o resultado (ou re-lança exceção se houve)
                processed_count += 1
                logger.debug(f"Chunk {doc_id} processado com sucesso pelo worker.")
            except Exception as exc:
                logger.error(f"Erro no worker ao processar chunk {doc_id}: {exc}", exc_info=True)
                failed_chunks.append(doc_id)

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Pipeline concluído em {duration:.2f} segundos.")
    logger.info(f"Total de chunks processados (ou tentados): {len(chunks)}")
    logger.info(f"Sucessos (thread concluída sem exceção): {processed_count}")
    logger.info(f"Falhas (exceção no worker): {len(failed_chunks)}")
    if failed_chunks:
        logger.warning(f"IDs dos chunks que falharam no processamento: {failed_chunks}")

    # Fechar o cliente HTTPX se ele existir e tiver o método close_http_client
    if r2r_client and hasattr(r2r_client, 'close_http_client') and callable(r2r_client.close_http_client):
        try:
            logger.info("Fechando o cliente HTTPX do R2RClientWrapper...")
            asyncio.run(r2r_client.close_http_client()) # Executar a função async de fechamento
        except Exception as e_close:
            logger.error(f"Erro ao fechar o cliente HTTPX: {e_close}", exc_info=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL RAG Supabase → CrewAI → R2R")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("ETL_BATCH_SIZE", 100)), help="Número de chunks a processar por lote.")
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("ETL_MAX_WORKERS", 2)), help="Número máximo de threads paralelas.")
    parser.add_argument("--skip-annotation", action="store_true", help="Pular etapa de anotação.")
    parser.add_argument("--skip-indexing", action="store_true", help="Pular etapa de indexação.")
    args = parser.parse_args()

    logger.info("Executando ETL com argumentos: %s", args)
    run_pipeline(
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        skip_annotation=args.skip_annotation,
        skip_indexing=args.skip_indexing,
    )
    logger.info("Execução do script finalizada.")


if __name__ == "__main__":
    main() 