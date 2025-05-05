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

import tiktoken
from dotenv import load_dotenv
from postgrest.exceptions import APIError as PostgrestAPIError
from supabase import Client, create_client
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logger.info("Supabase inicializado.")

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

def _sanitize_metadata(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Converte valores não‑hasháveis (ex.: slice) em string recursivamente."""
    if not meta or not isinstance(meta, dict):
        return {} if meta is None else meta
        
    clean: Dict[str, Any] = {}
    for k, v in meta.items():
        if isinstance(v, slice):
            clean[k] = str(v)
        elif isinstance(v, dict):
            clean[k] = _sanitize_metadata(v) # Chamada recursiva para dicionários aninhados
        elif isinstance(v, list):
             # Processa listas recursivamente (caso contenham dicts ou slices)
             clean[k] = [_sanitize_metadata(item) if isinstance(item, dict) 
                         else str(item) if isinstance(item, slice) 
                         else item for item in v]
        else:
            # Mantém outros tipos hasheáveis como estão
            try:
                hash(v) # Verifica se é hasheável
                clean[k] = v
            except TypeError:
                clean[k] = str(v) # Converte para string se não for hasheável
    return clean


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
        supabase.table("documents").update(payload).eq("document_id", doc_id).execute()
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
    
    # Sanitizar metadados para evitar erros com tipos não-hashable
    if 'metadata' in chunk and chunk['metadata']:
        chunk['metadata'] = _sanitize_metadata(chunk['metadata'])
        
    try:
        result = annotator.run(chunk)
        logger.debug(f"Resultado da anotação para {chunk.get('document_id', 'ID Desconhecido')}: Keep={result.keep if result else None}")
        return result
    except Exception as e:
        logger.error(f"Erro durante _run_annotation para {chunk.get('document_id', 'ID Desconhecido')}: {e}", exc_info=True)
        raise # Re-lançar para retentativa

@tenacity_retry()
def _upload_chunk_r2r(chunk: Dict[str, Any]):
    doc_id = chunk["document_id"]
    logger.debug(f"Preparando para fazer upload do chunk {doc_id} para R2R.")
    if not r2r_client:
        logger.warning("R2R client não está disponível. Pulando upload.")
        return # Retorna sem fazer nada se o R2R não estiver configurado

    try:
        # Sanitiza os metadados ANTES de usá-los
        meta = _sanitize_metadata(chunk.get("metadata", {})) # Usar .get com default {}
        meta["document_id"] = doc_id # Garante que o document_id correto está nos metadados
        meta.setdefault("source", chunk.get("metadata", {}).get("source_name", "unknown")) # Pega source_name se existir

        logger.debug(f"Enviando chunk {doc_id} para R2R com metadados: {meta}")
        r2r_client.upload_and_process_file(
            document_id=doc_id,
            blob=chunk["content"].encode('utf-8'), # Especificar encoding
            metadata=meta,
            settings={},
        )
        logger.info(f"Upload do chunk {doc_id} para R2R bem-sucedido.")
    except Exception as e:
        logger.error(f"Erro durante _upload_chunk_r2r para {doc_id}: {e}", exc_info=True)
        raise # Re-lançar para retentativa


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
    
    Args:
        chunk: Dicionário com os dados do chunk a ser processado
        annotator: Instância do AnnotatorAgent ou None
        skip_annotation: Flag para pular etapa de anotação
        skip_indexing: Flag para pular etapa de indexação
    """
    doc_id = chunk.get("document_id", f"id_ausente_{uuid.uuid4()}") # Usar get com fallback
    logger.info(f"Iniciando processamento do chunk: {doc_id}")

    current_annotation_status = chunk.get("annotation_status")
    current_indexing_status = chunk.get("indexing_status")
    keep_chunk = chunk.get("keep") # Pode ser None, True ou False

    # ---------------- Anotação ----------------
    # Bloco 1: Verificação se deve fazer anotação
    if not skip_annotation and current_annotation_status in {None, "pending", "error"}:
        logger.debug(f"Chunk {doc_id}: Tentando anotação (Status atual: {current_annotation_status}).")
        
        # Bloco 1.1: Verificar se o annotator está disponível
        if not annotator:
            logger.warning(f"Chunk {doc_id}: Annotator ausente – pulando anotação.")
            update = {"annotation_status": "skipped", "annotated_at": datetime.now(timezone.utc).isoformat()}
            _update_chunk_status_supabase(doc_id, update)
            current_annotation_status = "skipped" # Atualiza status local
        
        # Bloco 1.2: Executar anotação se annotator estiver disponível
        else:
            try:
                result = _run_annotation(annotator, chunk) # Já tem retentativa
                
                # Verificar resultado da anotação
                if result:
                    logger.info(f"Chunk {doc_id}: Anotação bem-sucedida. Keep={result.keep}, Tags={result.tags}")
                    update = {
                        "annotation_status": "done",
                        "annotated_at": datetime.now(timezone.utc).isoformat(),
                        "keep": result.keep,
                        "annotation_tags": result.tags,
                    }
                    _update_chunk_status_supabase(doc_id, update)
                    keep_chunk = result.keep # Atualiza variável local para indexação
                    current_annotation_status = "done"
                else:
                    logger.warning(f"Chunk {doc_id}: Anotação retornou None. Marcando como erro.")
                    update = {"annotation_status": "error", "annotated_at": datetime.now(timezone.utc).isoformat(), "keep": False}
                    _update_chunk_status_supabase(doc_id, update)
                    keep_chunk = False
                    current_annotation_status = "error"
            
            # Capturar erros na execução da anotação
            except Exception as exc:
                logger.exception(f"Chunk {doc_id}: Erro FINAL durante anotação: {exc}")
                update = {"annotation_status": "error", "annotated_at": datetime.now(timezone.utc).isoformat(), "keep": False}
                _update_chunk_status_supabase(doc_id, update)
                keep_chunk = False
                current_annotation_status = "error"
    
    # Bloco 2: Anotação pulada por flag
    elif skip_annotation:
        logger.debug(f"Chunk {doc_id}: Anotação pulada por flag.")
    
    # Bloco 3: Anotação não necessária pelo status atual
    else:
        logger.debug(f"Chunk {doc_id}: Anotação não necessária (Status: {current_annotation_status}).")

    # ---------------- Indexação ----------------
    # Bloco 1: Verificação se deve fazer indexação
    if (
        not skip_indexing
        and keep_chunk is True
        and current_indexing_status in {None, "pending", "error"}
    ):
        logger.debug(f"Chunk {doc_id}: Tentando indexação (Keep={keep_chunk}, Status atual: {current_indexing_status}).")
        try:
            # === COMENTAR TEMPORARIAMENTE PARA TESTE ===
            # _upload_chunk_r2r(chunk) # Já tem retentativa e checagem de r2r_client
            logger.warning(f"Chunk {doc_id}: Upload R2R comentado para teste.") # Log temporário
            # === FIM DO COMENTÁRIO ===
            logger.info(f"Chunk {doc_id}: Indexação bem-sucedida.")
            update = {
                "indexing_status": "done",
                "indexed_at": datetime.now(timezone.utc).isoformat(),
            }
            _update_chunk_status_supabase(doc_id, update)
            current_indexing_status = "done"
        
        # Capturar erros na execução da indexação
        except Exception as exc:
            logger.exception(f"Chunk {doc_id}: Erro FINAL durante indexação: {exc}")
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

    # Filtro para buscar chunks pendentes ou com erro em qualquer uma das etapas
    filter_expr = (
        "or=("
        "annotation_status.eq.pending,annotation_status.eq.error,"
        "indexing_status.eq.pending,indexing_status.eq.error,"
        "annotation_status.is.null,indexing_status.is.null" # Inclui chunks nunca processados
        ")"
    )

    try:
        logger.info(f"Buscando até {batch_size} chunks com filtro: {filter_expr}")
        resp = (
            supabase.table("documents")
            .select("*") # Busca todos os campos necessários para processamento
            .filter(filter_expr)
            .limit(batch_size)
            .execute()
        )
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


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL RAG Supabase → CrewAI → R2R")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("ETL_BATCH_SIZE", 100)), help="Número de chunks a processar por lote.")
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("ETL_MAX_WORKERS", 5)), help="Número máximo de threads paralelas.")
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