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
from agents.annotator_agent import AnnotatorAgent
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
from ingestion.gdrive_ingest import ingest_all_gdrive_content
# from ingestion.local_ingest import ingest_local_directory
from ingestion.local_ingest import ingest_local_directory
from ingestion.video_transcription import process_video # Importar a função específica
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

def count_tokens(text: str) -> int:
    """Conta tokens usando o tokenizer tiktoken inicializado.

    Args:
        text (str): O texto a ser tokenizado.

    Returns:
        int: O número de tokens no texto. Retorna a contagem de caracteres
             se o tokenizer não estiver disponível.
    """
    if not tokenizer:
        logging.warning("Tokenizer tiktoken não disponível, retornando contagem de caracteres como fallback.")
        return len(text)
    return len(tokenizer.encode(text))

def read_files_from_directory(directory_path: str) -> List[Dict[str, Any]]:
    """Lê arquivos .txt de um diretório e retorna uma lista de dicionários.

    Cada dicionário contém `filename`, `content` e `metadata` básico.

    Args:
        directory_path (str): O caminho para o diretório contendo os arquivos .txt.

    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários, onde cada um representa
                               um arquivo lido. Retorna lista vazia se o diretório
                               não for encontrado ou ocorrer um erro.
    """
    all_files_data = []
    try:
        for filename in os.listdir(directory_path):
            if filename.endswith(".txt"):
                file_path = os.path.join(directory_path, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        # Adiciona metadados iniciais aqui
                        all_files_data.append({
                            "filename": filename,
                            "content": content,
                            "metadata": { # Estrutura de metadados
                                "source_name": filename,
                                "origin": "file" # Default, pode ser sobrescrito por ingestão real
                            }
                        })
                        logging.info(f"Successfully read file: {filename}")
                except Exception as e:
                    logging.error(f"Error reading file {filename}: {e}")
    except FileNotFoundError:
        logging.error(f"Directory not found: {directory_path}")
    except Exception as e:
        logging.error(f"Error listing directory {directory_path}: {e}")
    return all_files_data

def split_content_into_chunks(text: str, initial_metadata: Dict[str, Any], max_chunk_tokens: int = 800) -> List[Dict[str, Any]]:
    """Divide o texto em chunks menores baseados na contagem de tokens.

    Tenta manter parágrafos e sentenças intactos, mas pode quebrar sentenças
    longas se necessário. Adiciona metadados a cada chunk, incluindo
    o `chunk_index`.

    Args:
        text (str): O texto completo a ser dividido.
        initial_metadata (Dict[str, Any]): Metadados originais do documento fonte,
                                           que serão copiados para cada chunk.
        max_chunk_tokens (int): O número máximo aproximado de tokens por chunk.
                                Defaults to 800.

    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários, onde cada um representa
                               um chunk com `content` e `metadata` atualizado.
    """
    if not tokenizer:
         logging.error("Tokenizer não disponível, não é possível fazer chunking por tokens.")
         return []
    if not text or not isinstance(text, str):
        logging.warning(f"Conteúdo inválido ou vazio recebido para chunking com metadados: {initial_metadata}")
        return []

    chunks_data = []
    current_chunk = []
    current_token_count = 0
    chunk_index_counter = 0

    # Tentar dividir por parágrafos primeiro, depois por sentenças se necessário
    sentences = text.split('\n\n') # Considerar parágrafos como unidades iniciais
    if len(sentences) <= 1:
        # Se não houver parágrafos claros, tentar por sentenças (simplista)
        sentences = text.replace('. ', '.\n').replace('! ', '!\n').replace('? ', '?\n').split('\n')

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        sentence_token_count = count_tokens(sentence)

        # Se a sentença sozinha já excede o limite, quebrá-la (embora não ideal)
        if sentence_token_count > max_chunk_tokens:
            logging.warning(f"Sentença excedeu o limite de tokens ({sentence_token_count}/{max_chunk_tokens}) e será quebrada: {sentence[:100]}...")
            # Quebra simples por palavra - pode ser melhorado
            words = sentence.split()
            temp_chunk = []
            temp_count = 0
            for word in words:
                word_count = count_tokens(word + ' ')
                if temp_count + word_count <= max_chunk_tokens:
                    temp_chunk.append(word)
                    temp_count += word_count
                else:
                    # Salva o chunk parcial da sentença longa
                    if temp_chunk:
                        current_chunk_text = " ".join(temp_chunk)
                        metadata = initial_metadata.copy()
                        metadata["chunk_index"] = chunk_index_counter
                        chunks_data.append({"content": current_chunk_text, "metadata": metadata})
                        chunk_index_counter += 1
                    # Começa novo chunk com a palavra atual
                    temp_chunk = [word]
                    temp_count = word_count
            # Salva o restante do chunk da sentença longa
            if temp_chunk:
                current_chunk_text = " ".join(temp_chunk)
                metadata = initial_metadata.copy()
                metadata["chunk_index"] = chunk_index_counter
                chunks_data.append({"content": current_chunk_text, "metadata": metadata})
                chunk_index_counter += 1
            continue # Pula para a próxima sentença

        # Se adicionar a sentença exceder o limite do chunk atual
        if current_token_count + sentence_token_count > max_chunk_tokens:
            # Salvar o chunk atual se ele não estiver vazio
            if current_chunk:
                current_chunk_text = "\n\n".join(current_chunk)
                metadata = initial_metadata.copy()
                metadata["chunk_index"] = chunk_index_counter
                chunks_data.append({"content": current_chunk_text, "metadata": metadata})
                chunk_index_counter += 1
            # Começar novo chunk com a sentença atual
            current_chunk = [sentence]
            current_token_count = sentence_token_count
        else:
            # Adicionar a sentença ao chunk atual
            current_chunk.append(sentence)
            current_token_count += sentence_token_count

    # Adicionar o último chunk restante
    if current_chunk:
        current_chunk_text = "\n\n".join(current_chunk)
        metadata = initial_metadata.copy()
        metadata["chunk_index"] = chunk_index_counter
        chunks_data.append({"content": current_chunk_text, "metadata": metadata})

    logging.debug(f"Chunking para {initial_metadata.get('source_name', 'N/A')} resultou em {len(chunks_data)} chunks.")
    return [c for c in chunks_data if c.get("content")]

def process_video_data(video_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Processa os resultados da ingestão de vídeo, chama a transcrição e formata
    os dados com o texto da transcrição e os metadados originais.
    """
    logger.info(f"Iniciando processamento e transcrição para {len(video_results)} vídeo(s) ingerido(s)...")
    formatted_results = []
    processed_count = 0
    failed_count = 0

    for video_data in video_results:
        original_metadata = video_data.get("metadata", {})
        video_path = video_data.get("path")
        video_name = original_metadata.get("gdrive_name", "Nome Desconhecido")
        gdrive_id = original_metadata.get("gdrive_id")

        if not video_path or not os.path.exists(video_path):
            logger.error(f"Caminho do vídeo inválido ou não encontrado para {video_name} (ID: {gdrive_id}). Pulando transcrição.")
            failed_count += 1
            continue

        # Chamar a função de transcrição (que tenta AssemblyAI/WhisperX)
        # Nota: Isso pode ser demorado dependendo do vídeo e método
        transcription_result = process_video(video_path)

        if transcription_result and transcription_result.get("text"):
            transcription_text = transcription_result.get("text", "").strip()
            transcriber_metadata = transcription_result.get("metadata", {})
            if transcription_text:
                # Combinar metadados originais com metadados da transcrição
                combined_metadata = original_metadata.copy() # Começa com os metadados do GDrive
                combined_metadata.update(transcriber_metadata) # Adiciona metadados do transcritor
                combined_metadata["origin"] = "video_transcription" # Indica a origem

                # Criar o dicionário final no formato esperado
                formatted_results.append({
                    "filename": video_name, # Usar nome original como filename?
                    "content": transcription_text,
                    "metadata": combined_metadata
                })
                logger.info(f"Transcrição bem-sucedida e formatada para {video_name} (ID: {gdrive_id})")
                processed_count += 1
            else:
                logger.warning(f"Transcrição para {video_name} (ID: {gdrive_id}) resultou em texto vazio. Descartando.")
                failed_count += 1
        else:
            logger.error(f"Falha na transcrição para {video_name} (ID: {gdrive_id}). Descartando.")
            failed_count += 1

    logger.info(f"Processamento de dados de vídeo concluído. Sucesso: {processed_count}, Falhas/Vazios: {failed_count}.")
    return formatted_results

def process_local_data(local_files_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """(REMOVIDO - Fonte local não é mais suportada)
    Processa dados de arquivos lidos localmente.

    Atualmente, esta função é um placeholder e apenas repassa os dados.
    Pode ser estendida no futuro para pré-processamento específico de arquivos locais.

    Args:
        local_files_data (List[Dict[str, Any]]): Lista de dicionários
            representando arquivos lidos do diretório local.

    Returns:
        List[Dict[str, Any]]: A mesma lista de entrada.
    """
    # Por enquanto, apenas retorna os dados como estão
    logger.info(f"Processando {len(local_files_data)} arquivos locais (nenhuma transformação extra aplicada).")
    return local_files_data

@default_retry
def _update_chunk_status_supabase(supabase_client: Client, document_id: str, data_to_update: Dict[str, Any], step_name: str):
    """Função auxiliar para atualizar o status de um chunk no Supabase (com retentativas)."""
    if not supabase_client or not document_id:
        return False
    
    logger.debug(f"[Supabase Update - {step_name}] Tentando atualizar status para doc_id {document_id}")
    try:
        response = supabase_client.table('documents')\
                                  .update(data_to_update)\
                                  .eq('document_id', document_id)\
                                  .execute()
        if hasattr(response, 'error') and response.error:
             # Se ainda der erro após retentativas, logar como erro final
             logger.error(f"[Supabase Update - {step_name}] Erro FINAL ao atualizar status para doc_id {document_id} após retentativas: {response.error}")
             return False
        logger.debug(f"[Supabase Update - {step_name}] Status atualizado para doc_id {document_id}")
        return True
    except (PostgrestAPIError) as e:
        # Logar erro específico da API que causou a falha final das retentativas
        logger.error(f"[Supabase Update - {step_name}] Erro FINAL API ao atualizar status para doc_id {document_id} após retentativas: {e}")
        raise # Re-lançar para que tenacity saiba que falhou
    except Exception as e:
        logger.error(f"[Supabase Update - {step_name}] Erro FINAL inesperado ao atualizar status para doc_id {document_id} após retentativas: {e}", exc_info=True)
        raise # Re-lançar para que tenacity saiba que falhou

@default_retry
def _run_annotator_with_retry(annotator: AnnotatorAgent, chunks: List[Dict[str, Any]], source_name: str) -> List[Dict[str, Any]]:
    """Executa o annotator com retentativas."""
    logger.info(f"[Anotação Retry] Tentando executar anotação para {len(chunks)} chunks de {source_name}...")
    start_time = time.time()
    try:
        result = annotator.run(chunks)
        duration = time.time() - start_time
        logger.info(f"[Anotação Retry] Execução bem-sucedida em {duration:.2f}s.")
        return result
    except Exception as e:
         # Logar o erro que causou a falha final das retentativas
         logger.error(f"[Anotação Retry] Erro FINAL ao executar anotação para {source_name} após retentativas: {e}", exc_info=True)
         raise # Re-lançar para tenacity

@default_retry
def _upload_single_chunk_to_r2r_with_retry(r2r_client: R2RClientWrapper, file_path: str, document_id: str, metadata: Dict[str, Any]):
    """Faz upload para R2R com retentativas."""
    logger.debug(f"[R2R Upload Retry] Tentando upload para doc_id: {document_id}")
    try:
        result = r2r_client.upload_file(
            file_path=file_path,
            document_id=document_id,
            metadata=metadata
        )
        logger.debug(f"[R2R Upload Retry] Resultado para doc_id {document_id}: {result}")
        return result # Retorna o dicionário de resultado
    except Exception as e:
         logger.error(f"[R2R Upload Retry] Erro FINAL no upload para doc_id {document_id} após retentativas: {e}", exc_info=False)
         raise # Re-lançar

@default_retry
def _mark_file_processed_supabase(supabase_client: Client, file_id: str, source_name: str):
    """Marca o arquivo como processado no Supabase (com retentativas)."""
    if not supabase_client or not file_id:
        return

    logger.info(f"[Mark Processed Retry] Tentando marcar {source_name} (ID: {file_id}) como processado...")
    try:
        response = supabase_client.table('processed_files').insert({"file_id": file_id}).execute()
        if response.data or (hasattr(response, 'status_code') and 200 <= response.status_code < 300):
             logging.info(f"[Mark Processed Retry] Marcação bem-sucedida para {file_id}.")
        else:
             is_duplicate = False
             if hasattr(response, 'error') and response.error:
                  if hasattr(response.error, 'code') and response.error.code == '23505':
                       is_duplicate = True
                       logging.info(f"[Mark Processed Retry] {file_id} já estava marcado (Unique constraint). OK.")
             if not is_duplicate:
                  logger.error(f"[Mark Processed Retry] Falha FINAL ao marcar {file_id} após retentativas. Resposta: {getattr(response, 'data', 'N/A')}, Status: {getattr(response, 'status_code', 'N/A')}, Erro: {getattr(response, 'error', 'N/A')}")
                  # Levantar exceção aqui para sinalizar falha? Ou apenas logar? Por enquanto, logar.
                  # raise PostgrestAPIError("Failed to mark file as processed after retries") # Exemplo se quiséssemos falhar
    except (PostgrestAPIError) as e:
         if hasattr(e, 'code') and e.code == '23505':
              logger.info(f"[Mark Processed Retry] {file_id} já estava marcado (Unique constraint - APIError). OK.")
         else:
              logger.error(f"[Mark Processed Retry] Erro FINAL API ao marcar {file_id} após retentativas: {e}")
              # raise # Re-lançar se quisermos que a falha aqui seja crítica
    except Exception as e:
        logger.error(f"[Mark Processed Retry] Erro FINAL inesperado ao marcar {file_id} após retentativas: {e}", exc_info=True)
        # raise # Re-lançar se quisermos que a falha aqui seja crítica

def process_single_chunk(
    chunk_data: Dict[str, Any],
    annotator: AnnotatorAgent,
    r2r_client_instance: R2RClientWrapper,
    supabase_client: Client,
    skip_annotation: bool = False,
    skip_indexing: bool = False,
    # max_workers_r2r_upload: int = 5 # Não mais necessário aqui, paralelismo é externo
) -> bool: # Retorna True se o processamento do chunk (anotação/indexação) foi bem-sucedido
    """
    Processa um ÚNICO chunk (anotação e/ou indexação) baseado no seu status atual.

    Args:
        chunk_data (Dict[str, Any]): Dicionário do chunk vindo do Supabase,
                                     contendo pelo menos 'document_id', 'content', 'metadata',
                                     'annotation_status', 'indexing_status'.
        annotator (AnnotatorAgent): Instância do agente de anotação.
        r2r_client_instance (R2RClientWrapper): Instância do cliente R2R.
        supabase_client (Client): Instância do cliente Supabase.
        skip_annotation (bool): Pular etapa de anotação globalmente.
        skip_indexing (bool): Pular etapa de indexação globalmente.

    Returns:
        bool: True se as etapas aplicáveis ao chunk foram concluídas com sucesso,
              False caso contrário.
    """
    document_id = chunk_data.get("document_id")
    content = chunk_data.get("content", "")
    metadata = chunk_data.get("metadata", {})
    annotation_status = chunk_data.get("annotation_status", "pending")
    indexing_status = chunk_data.get("indexing_status", "pending")
    source_name = metadata.get("source_name", "desconhecido") # Para logs

    if not document_id or not content:
        logger.warning(f"Chunk inválido recebido (sem ID ou conteúdo). Pulando: {chunk_data}")
        return False # Não pode processar

    logger.debug(f"Iniciando processamento do chunk {document_id} de {source_name}...")

    # --- Etapa de Anotação ---
    annotation_completed_successfully = False
    annotated_chunk_data = chunk_data # Usar dados originais se pular ou falhar

    if annotation_status == 'pending' and not skip_annotation and annotator:
        logger.info(f"[Anotação] Processando chunk {document_id}...")
        try:
            # Anotar um chunk de cada vez
            annotation_results = _run_annotator_with_retry(annotator, [chunk_data], source_name)
            if annotation_results:
                annotated_chunk_data = annotation_results[0] # Pega o chunk anotado
                annotation_error = annotated_chunk_data.get("annotation_error")
                status_to_set = "failed" if annotation_error else "success"
                annotation_completed_successfully = (status_to_set == "success")
                logger.info(f"[Anotação] Chunk {document_id} concluído. Status: {status_to_set}")
            else:
                logger.error(f"[Anotação] Agente não retornou resultados para chunk {document_id}.")
                status_to_set = "failed"
                annotation_completed_successfully = False

            # Atualizar status no Supabase
            timestamp = datetime.now(timezone.utc).isoformat()
            update_payload = {
                "annotation_status": status_to_set,
                "annotated_at": timestamp,
                # Atualizar metadados com tags/keep/reason da anotação
                "metadata": annotated_chunk_data.get("metadata", metadata) # Usa metadata atualizado
            }
            if not _update_chunk_status_supabase(supabase_client, document_id, update_payload, "anotação"):
                 logger.error(f"[Anotação] Falha ao atualizar status no Supabase para chunk {document_id}.")
                 annotation_completed_successfully = False # Marcar falha se update falhar

        except Exception as e:
             logger.error(f"[Anotação] Erro inesperado ao anotar chunk {document_id}: {e}", exc_info=True)
             # Atualizar status como falha
             timestamp = datetime.now(timezone.utc).isoformat()
             _update_chunk_status_supabase(supabase_client, document_id, {"annotation_status": "failed", "annotated_at": timestamp}, "anotação-erro")
             annotation_completed_successfully = False

    elif annotation_status == 'pending' and (skip_annotation or not annotator):
         logger.info(f"[Anotação] Pulando para chunk {document_id} (skip={skip_annotation}, annotator_exists={bool(annotator)}). Marcando como skipped.")
         timestamp = datetime.now(timezone.utc).isoformat()
         # Atualizar status como skipped
         _update_chunk_status_supabase(supabase_client, document_id, {"annotation_status": "skipped", "annotated_at": timestamp}, "anotação-skip")
         annotation_completed_successfully = True # Pular é considerado sucesso para o fluxo
         # Manter metadata original, mas garantir que 'keep' seja True por default se pulou
         if "keep" not in metadata:
             metadata["keep"] = True
             annotated_chunk_data["metadata"] = metadata # Atualiza dados para etapa de indexação

    elif annotation_status != 'pending':
         logger.debug(f"[Anotação] Já processada ou pulada para chunk {document_id} (status: {annotation_status}).")
         annotation_completed_successfully = (annotation_status == 'success' or annotation_status == 'skipped')
         annotated_chunk_data = chunk_data # Usar dados existentes do banco

    # --- Etapa de Indexação R2R ---
    indexing_completed_successfully = False
    # Condições para indexar: Anotação bem sucedida OU pulada E Indexação está pendente E não é para pular globalmente E R2R existe
    should_attempt_indexing = (annotation_completed_successfully and
                               indexing_status == 'pending' and
                               not skip_indexing and
                               r2r_client_instance and
                               annotated_chunk_data.get("metadata", {}).get("keep", False)) # E keep=True

    if should_attempt_indexing:
        logger.info(f"[Indexação R2R] Processando chunk {document_id}...")
        temp_file_path = None
        try:
            # Preparar arquivo temporário
            chunk_content = annotated_chunk_data.get('content', '')
            chunk_metadata = annotated_chunk_data.get('metadata', {})
            chunk_index = chunk_metadata.get("chunk_index", -1)
            temp_file_name = f"r2r_upload_{document_id}_idx{chunk_index}.txt"
            with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8', suffix='.txt', prefix=temp_file_name) as temp_file:
                temp_file.write(chunk_content)
                temp_file_path = temp_file.name

            # Chamar upload R2R com retry
            upload_result = _upload_single_chunk_to_r2r_with_retry(r2r_client_instance, temp_file_path, document_id, chunk_metadata)
            if upload_result: # Função retorna True em sucesso, False em falha após retries
                logger.info(f"[Indexação R2R] Chunk {document_id} indexado com sucesso.")
                status_to_set = "success"
                indexing_completed_successfully = True
            else:
                 logger.error(f"[Indexação R2R] Falha ao indexar chunk {document_id} após retentativas.")
                 status_to_set = "failed"
                 indexing_completed_successfully = False

        except Exception as e:
             logger.error(f"[Indexação R2R] Erro inesperado ao indexar chunk {document_id}: {e}", exc_info=True)
             status_to_set = "failed"
             indexing_completed_successfully = False
        finally:
            # Limpar arquivo temporário
            if temp_file_path and os.path.exists(temp_file_path):
                try: os.remove(temp_file_path)
                except OSError: pass

        # Atualizar status no Supabase
        timestamp = datetime.now(timezone.utc).isoformat()
        if not _update_chunk_status_supabase(supabase_client, document_id, {"indexing_status": status_to_set, "indexed_at": timestamp}, "indexação"):
            indexing_completed_successfully = False # Falha no update também conta como falha

    elif indexing_status == 'pending' and (skip_indexing or not r2r_client_instance or not annotated_chunk_data.get("metadata", {}).get("keep", False)):
         reason_skip = ("global skip" if skip_indexing else
                       "no R2R client" if not r2r_client_instance else
                       "keep=False" if not annotated_chunk_data.get("metadata", {}).get("keep", False) else
                       "annotation failed" if not annotation_completed_successfully else "unknown")
         logger.info(f"[Indexação R2R] Pulando para chunk {document_id} (Razão: {reason_skip}). Marcando como skipped.")
         timestamp = datetime.now(timezone.utc).isoformat()
         _update_chunk_status_supabase(supabase_client, document_id, {"indexing_status": "skipped", "indexed_at": timestamp}, "indexação-skip")
         indexing_completed_successfully = True # Pular é considerado sucesso

    elif indexing_status != 'pending':
         logger.debug(f"[Indexação R2R] Já processada ou pulada para chunk {document_id} (status: {indexing_status}).")
         indexing_completed_successfully = (indexing_status == 'success' or indexing_status == 'skipped')

    # Retorna True se ambas as etapas aplicáveis foram bem-sucedidas (ou puladas com sucesso)
    final_success = annotation_completed_successfully and indexing_completed_successfully
    logger.debug(f"Processamento do chunk {document_id} finalizado. Sucesso geral: {final_success}")
    return final_success

def run_pipeline(
    source: str, # Pode ser removido ou ignorado se só usamos Supabase
    local_dir: str, # Pode ser removido ou ignorado
    dry_run: bool, # Provavelmente não mais aplicável neste novo fluxo
    dry_run_limit: Optional[int], # Provavelmente não mais aplicável
    skip_annotation: bool,
    skip_indexing: bool,
    # max_workers_r2r_upload: int = 5 # Movido para fora, se necessário
    batch_size: int = 100, # Tamanho do lote para buscar chunks do Supabase
    max_workers_pipeline: int = 5 # Workers para processar chunks em paralelo
):
    """
    Executa a pipeline ETL completa buscando chunks pendentes do Supabase.

    1. Consulta Supabase por chunks pendentes de anotação.
    2. Processa esses chunks em paralelo.
    3. Consulta Supabase por chunks pendentes de indexação (que passaram pela anotação).
    4. Processa esses chunks em paralelo.
    """
    start_time = time.time()
    logger.info(f"--- Iniciando Pipeline ETL (Modo Consulta Supabase) --- SkipAnnotation: {skip_annotation}, SkipIndex: {skip_indexing}")

    if not supabase:
        logger.critical("Cliente Supabase não inicializado. Pipeline não pode continuar.")
        return

    # Inicializar agente CrewAI se necessário
    annotator_agent = None
    if not skip_annotation:
        try:
            annotator_agent = AnnotatorAgent()
            logger.info("AnnotatorAgent inicializado com sucesso.")
        except Exception as e:
            logger.error(f"Falha ao inicializar AnnotatorAgent: {e}. Anotação será pulada.", exc_info=True)
            skip_annotation = True

    # Inicializar cliente R2R se necessário
    r2r_client_instance = None
    if not skip_indexing:
         try:
             r2r_client_instance = R2RClientWrapper()
             logger.info("R2R Client Wrapper inicializado com sucesso.")
         except Exception as e:
             logger.error(f"Falha ao inicializar R2R Client Wrapper: {e}. Indexação R2R será pulada.", exc_info=True)
             skip_indexing = True

    total_processed_annotation = 0
    total_failed_annotation = 0
    total_processed_indexing = 0
    total_failed_indexing = 0

    # --- Processar Chunks Pendentes de Anotação ---
    logger.info("--- Iniciando Fase de Anotação (buscando chunks pendentes) ---")
    while True:
        logger.debug(f"Buscando lote de até {batch_size} chunks com annotation_status='pending'...")
        try:
            response = supabase.table('documents')\
                             .select("document_id, content, metadata, annotation_status, indexing_status")\
                             .eq('annotation_status', 'pending')\
                             .limit(batch_size)\
                             .execute()

            if not response.data:
                logger.info("Nenhum chunk pendente de anotação encontrado neste lote.")
                break # Sai do loop de anotação

            chunks_to_annotate = response.data
            logger.info(f"Encontrados {len(chunks_to_annotate)} chunks para anotar neste lote.")

            with ThreadPoolExecutor(max_workers=max_workers_pipeline) as executor:
                future_to_doc_id = {executor.submit(process_single_chunk, chunk, annotator_agent, r2r_client_instance, supabase, skip_annotation, True): # Força skip_indexing nesta fase
                                    chunk.get("document_id") for chunk in chunks_to_annotate}

                for future in as_completed(future_to_doc_id):
                    doc_id = future_to_doc_id[future]
                    try:
                        success = future.result()
                        if success:
                            total_processed_annotation += 1
                        else:
                            total_failed_annotation += 1
                    except Exception as exc:
                        logger.error(f'Erro no worker de anotação para chunk {doc_id}: {exc}', exc_info=True)
                        total_failed_annotation += 1

            logger.info(f"Lote de anotação processado. Total até agora: {total_processed_annotation} sucesso, {total_failed_annotation} falhas.")
            # Implementar alguma lógica para evitar loop infinito se um chunk sempre falhar?
            # Por ora, assume que falhas serão marcadas e não re-selecionadas.
            if len(chunks_to_annotate) < batch_size:
                 logger.info("Último lote de anotação processado.")
                 break # Provavelmente não há mais chunks pendentes

        except Exception as e:
            logger.error(f"Erro ao buscar ou processar lote de anotação: {e}", exc_info=True)
            time.sleep(5) # Esperar antes de tentar buscar novo lote

    logger.info(f"--- Fase de Anotação Concluída: {total_processed_annotation} sucesso, {total_failed_annotation} falhas ---")

    # --- Processar Chunks Pendentes de Indexação ---
    logger.info("--- Iniciando Fase de Indexação (buscando chunks pendentes e anotados/pulados) ---")
    while True:
        logger.debug(f"Buscando lote de até {batch_size} chunks com indexing_status='pending' E annotation_status!='pending'...")
        try:
            response = supabase.table('documents')\
                             .select("document_id, content, metadata, annotation_status, indexing_status")\
                             .neq('annotation_status', 'pending')\
                             .eq('indexing_status', 'pending')\
                             .limit(batch_size)\
                             .execute()

            if not response.data:
                logger.info("Nenhum chunk pendente de indexação encontrado neste lote.")
                break # Sai do loop de indexação

            chunks_to_index = response.data
            logger.info(f"Encontrados {len(chunks_to_index)} chunks para indexar neste lote.")

            with ThreadPoolExecutor(max_workers=max_workers_pipeline) as executor:
                future_to_doc_id = {executor.submit(process_single_chunk, chunk, annotator_agent, r2r_client_instance, supabase, True, skip_indexing): # Força skip_annotation nesta fase
                                    chunk.get("document_id") for chunk in chunks_to_index}

                for future in as_completed(future_to_doc_id):
                    doc_id = future_to_doc_id[future]
                    try:
                        success = future.result()
                        if success:
                            total_processed_indexing += 1
                        else:
                            total_failed_indexing += 1
                    except Exception as exc:
                        logger.error(f'Erro no worker de indexação para chunk {doc_id}: {exc}', exc_info=True)
                        total_failed_indexing += 1

            logger.info(f"Lote de indexação processado. Total até agora: {total_processed_indexing} sucesso, {total_failed_indexing} falhas.")
            if len(chunks_to_index) < batch_size:
                 logger.info("Último lote de indexação processado.")
                 break

        except Exception as e:
            logger.error(f"Erro ao buscar ou processar lote de indexação: {e}", exc_info=True)
            time.sleep(5)

    logger.info(f"--- Fase de Indexação Concluída: {total_processed_indexing} sucesso, {total_failed_indexing} falhas ---")

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"--- Pipeline ETL (Modo Consulta Supabase) Concluída --- Duração Total: {duration:.2f} segundos")
    logger.info(f"Resumo Anotação: {total_processed_annotation} sucesso, {total_failed_annotation} falhas.")
    logger.info(f"Resumo Indexação: {total_processed_indexing} sucesso, {total_failed_indexing} falhas.")

def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL para RAG - Modo Consulta Supabase")
    # Remover argumentos de ingestão que não são mais usados por run_pipeline
    # parser.add_argument("--source", choices=['gdrive', 'local'], default='gdrive', help="Fonte dos dados (gdrive ou local).")
    # parser.add_argument("--local-dir", type=str, default="/app/local_data", help="Diretório para ingestão local.")
    # parser.add_argument("--dry-run", action="store_true", help="Executar em modo dry-run.")
    # parser.add_argument("--dry-run-limit", type=int, default=None, help="Limitar número de arquivos no dry-run.")
    parser.add_argument("--skip-annotation", action="store_true", help="Pular a etapa de anotação com CrewAI.")
    parser.add_argument("--skip-indexing", action="store_true", help="Pular a etapa de indexação no R2R Cloud.")
    parser.add_argument("--batch-size", type=int, default=100, help="Tamanho do lote para buscar chunks do Supabase.")
    parser.add_argument("--max-workers", type=int, default=5, help="Número máximo de workers para processar chunks em paralelo.")

    args = parser.parse_args()

    logger.info("Iniciando pipeline ETL (Modo Consulta Supabase)...")

    run_pipeline(
        source='supabase', # Fonte agora é implícita
        local_dir='', # Não usado
        dry_run=False, # Não aplicável
        dry_run_limit=None, # Não aplicável
        skip_annotation=args.skip_annotation,
        skip_indexing=args.skip_indexing,
        batch_size=args.batch_size,
        max_workers_pipeline=args.max_workers
    )

    logger.info("Pipeline ETL (Modo Consulta Supabase) finalizada.")

if __name__ == "__main__":
    main() 