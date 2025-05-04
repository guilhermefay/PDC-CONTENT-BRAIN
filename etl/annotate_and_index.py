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
        # Adicionar log antes da execução
        logging.debug(f"[_mark_file_processed_supabase] Tentando inserir file_id: {file_id}, source_name: {source_name}")
        response: PostgrestAPIResponse = supabase_client.table('processed_files').insert({"file_id": file_id}).execute()
        if response.data or (hasattr(response, 'status_code') and 200 <= response.status_code < 300):
             logging.info(f"Arquivo '{source_name}' (ID: {file_id}) marcado como processado no Supabase.")
             # Adicionar log de sucesso
             logging.debug(f"[_mark_file_processed_supabase] Inserção bem-sucedida para file_id: {file_id}")
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
              # Adicionar log de erro específico
              logger.error(f"[_mark_file_processed_supabase] Erro Postgrest ao marcar file_id {file_id}: {e}")
              # raise # Re-lançar se quisermos que a falha aqui seja crítica
    except Exception as e:
        # Adicionar log de erro genérico
        logger.error(f"[_mark_file_processed_supabase] Erro inesperado ao marcar file_id {file_id}: {e}", exc_info=True)
        # raise # Re-lançar se quisermos que a falha aqui seja crítica

def process_single_chunk(
    chunk_data: Dict[str, Any],
    annotator: AnnotatorAgent,
    r2r_client_instance: R2RClientWrapper,
    supabase_client: Client,
    skip_annotation: bool = False,
    skip_indexing: bool = False,
) -> bool:
    document_id = chunk_data.get('metadata', {}).get('document_id', 'ID Desconhecido')
    source_name = chunk_data.get('metadata', {}).get('source_name', 'Nome Desconhecido')
    logging.debug(f"[process_single_chunk START] ID: {document_id}, Source: {source_name}") # Log de início

    annotation_tags = []
    keep_chunk = True # Default para manter o chunk se a anotação for pulada
    annotation_status = 'skipped' # Default status
    annotated_at = None # Default timestamp

    try:
        # Etapa 1: Anotação (se não pulada)
        if not skip_annotation:
            logging.debug(f"[process_single_chunk] Anotando chunk {document_id}...")
            if annotator:
                try:
                    # Usar a função de retry para a chamada do anotador
                    annotated_chunks = _run_annotator_with_retry(annotator, [chunk_data], source_name) # Passar como lista
                    if annotated_chunks:
                        annotated_chunk = annotated_chunks[0] # Pegar o primeiro (e único) resultado
                        annotation_tags = annotated_chunk.get('metadata', {}).get('annotation_tags', [])
                        keep_chunk = annotated_chunk.get('metadata', {}).get('keep', True)
                        annotation_status = 'success'
                        annotated_at = datetime.now(timezone.utc).isoformat()
                        logging.info(f"Chunk {document_id} anotado. Tags: {annotation_tags}, Keep: {keep_chunk}")
                    else:
                        logging.warning(f"Anotação para chunk {document_id} retornou vazio ou falhou após retries.")
                        keep_chunk = False # Considerar falha se anotação falhar
                        annotation_status = 'failed'
                except Exception as e_annotate:
                    logging.error(f"Erro durante a anotação do chunk {document_id}: {e_annotate}", exc_info=True)
                    keep_chunk = False # Falha na anotação impede processamento posterior
                    annotation_status = 'failed'
            else:
                logging.warning(f"AnnotatorAgent não inicializado, pulando anotação para chunk {document_id}.")
                # Mantém status 'skipped' e keep_chunk=True (default)
        else:
            logging.info(f"Anotação pulada para chunk {document_id}.")
            # Mantém status 'skipped' e keep_chunk=True (default)

        # Atualizar metadados do chunk original com os resultados da anotação/skip
        chunk_data['metadata']['annotation_tags'] = annotation_tags
        chunk_data['metadata']['keep'] = keep_chunk
        chunk_data['metadata']['annotation_status'] = annotation_status
        chunk_data['metadata']['indexing_status'] = 'pending' # Será atualizado depois
        chunk_data['metadata']['annotated_at'] = annotated_at

        # Etapa 2: Sempre salvar/atualizar no Supabase (mesmo se keep=False)
        logging.debug(f"[process_single_chunk] Atualizando chunk {document_id} no Supabase (tabela documents)...")
        db_update_data = {
            "content": chunk_data.get("content"),
            "metadata": chunk_data.get("metadata"), # Inclui status e timestamp da anotação
            "annotation_tags": annotation_tags # Salvar tags separadamente também
        }
        try:
            _update_chunk_status_supabase(supabase_client, document_id, db_update_data, "save/update chunk")
            logging.info(f"Chunk {document_id} salvo/atualizado no Supabase.")
        except Exception as e_supabase_save:
            logging.error(f"Falha ao salvar/atualizar chunk {document_id} no Supabase: {e_supabase_save}", exc_info=True)
            logging.debug(f"[process_single_chunk END] ID: {document_id} - Retornando False (falha ao salvar no DB)")
            return False # Falha crítica se não conseguir salvar no DB

        # Etapa 3: Indexação no R2R (se não pulada E keep_chunk=True)
        indexing_final_status = 'pending' # Status padrão
        if not skip_indexing and keep_chunk:
            logging.debug(f"[process_single_chunk] Indexando chunk {document_id} no R2R...")
            if r2r_client_instance:
                temp_file_path = None # Inicializar fora do with
                try:
                    # Salvar conteúdo do chunk em arquivo temporário para upload
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".txt", encoding='utf-8') as temp_file:
                        temp_file.write(chunk_data["content"])
                        temp_file_path = temp_file.name
                    logging.debug(f"Chunk {document_id} salvo em arquivo temporário: {temp_file_path}")

                    _upload_single_chunk_to_r2r_with_retry(r2r_client_instance, temp_file_path, document_id, chunk_data["metadata"])
                    logging.info(f"Chunk {document_id} enviado para indexação no R2R.")
                    indexing_final_status = 'success'

                except Exception as e_r2r:
                    logging.error(f"Falha ao indexar chunk {document_id} no R2R após retries: {e_r2r}", exc_info=True)
                    indexing_final_status = 'failed'
                    # Não retornar False aqui ainda, vamos tentar atualizar o status no Supabase primeiro
                finally:
                    # Limpar arquivo temporário após tentativa de upload (sucesso ou falha)
                    if temp_file_path and os.path.exists(temp_file_path):
                        try:
                            os.remove(temp_file_path)
                            logging.debug(f"Arquivo temporário {temp_file_path} removido.")
                        except OSError as e_remove:
                            logging.warning(f"Não foi possível remover o arquivo temporário {temp_file_path}: {e_remove}")
            else:
                logging.warning(f"R2R Client não inicializado, pulando indexação para chunk {document_id}.")
                indexing_final_status = 'skipped'
        elif skip_indexing:
            logging.info(f"Indexação pulada para chunk {document_id} via flag.")
            indexing_final_status = 'skipped'
        elif not keep_chunk:
            logging.info(f"Chunk {document_id} marcado com keep=False, indexação pulada.")
            indexing_final_status = 'skipped'

        # Atualizar status final de indexação no Supabase (se não for pending)
        if indexing_final_status != 'pending':
            index_status_update = {
                "indexing_status": indexing_final_status,
                "indexed_at": datetime.now(timezone.utc).isoformat() if indexing_final_status == 'success' else None,
                "metadata": chunk_data['metadata'] # Reenviar metadados (inclui status da anotação)
            }
            try:
                _update_chunk_status_supabase(supabase_client, document_id, index_status_update, f"update indexing status ({indexing_final_status})")
                logging.info(f"Status de indexação ({indexing_final_status}) para chunk {document_id} atualizado no Supabase.")
            except Exception as e_supabase_index_status:
                 logging.error(f"Falha ao atualizar status de indexação ({indexing_final_status}) para chunk {document_id} no Supabase: {e_supabase_index_status}", exc_info=True)
                 # Considerar isso uma falha do chunk? Se falhou no R2R e falhou ao marcar falha no DB?
                 # Por enquanto, apenas logar o erro.

        # Retornar True se a indexação foi bem-sucedida ou pulada sem erros críticos anteriores
        # Retornar False se a indexação falhou (e tentamos marcar no DB)
        if indexing_final_status == 'failed':
            logging.debug(f"[process_single_chunk END] ID: {document_id} - Retornando False (falha na indexação R2R)")
            return False
        else:
            logging.debug(f"[process_single_chunk END] ID: {document_id} - Retornando True")
            return True

    # Except geral para pegar erros inesperados no fluxo principal da função
    except Exception as e:
        logging.error(f"[process_single_chunk END] Erro inesperado processando chunk {document_id}: {e}", exc_info=True)
        # Tentar marcar o chunk como falha no Supabase?
        try:
            fail_status_update = {
                "annotation_status": "failed",
                "indexing_status": "failed",
                "metadata": chunk_data.get('metadata', {}) # Tentar pegar metadados se disponíveis
            }
            if document_id != 'ID Desconhecido': # Evitar tentar atualizar se não tiver ID
                _update_chunk_status_supabase(supabase_client, document_id, fail_status_update, "update status on general failure")
        except Exception as e_update_fail:
            logging.error(f"Falha adicional ao tentar marcar chunk {document_id} como falha no DB após erro geral: {e_update_fail}")
        
        logging.debug(f"[process_single_chunk END] ID: {document_id} - Retornando False (exceção geral)")
        return False

def run_pipeline(
    # Argumentos da função como definidos anteriormente, mas removendo os não usados
    # REMOVIDO: source: str,
    local_dir: str, # Mantido, mas não usado
    dry_run: bool, # Mantido, mas não usado
    dry_run_limit: Optional[int], # Mantido, mas não usado
    skip_annotation: bool,
    skip_indexing: bool,
    batch_size: int = 100, # Usado para buscar chunks do Supabase
    max_workers_pipeline: int = 5
):
    """
    Orquestra o pipeline ETL: busca chunks PENDENTES no Supabase,
    anota e indexa.
    """
    # Log de Info atualizado
    logging.info(f"Iniciando pipeline ETL (Modo Consulta Supabase)... Skip Annotation: {skip_annotation}, Skip Indexing: {skip_indexing}")
    start_time = time.time() # Marcar tempo de início do processamento dos chunks

    if not supabase:
        logging.error("Cliente Supabase não inicializado. Não é possível buscar chunks pendentes.")
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
    all_chunks_to_process = []
    try:
        logging.info(f"Buscando chunks pendentes para anotação/indexação no Supabase (batch_size={batch_size})...")
        # Busca chunks com annotation_status = 'pending'
        response = default_retry(
            lambda: supabase.table("documents")\
            .select("document_id, content, metadata, annotation_status, indexing_status, keep")\
            .eq("annotation_status", "pending")\
            .limit(batch_size)\
            .execute()
        )

        if response.data:
            for item in response.data:
                metadata = item.get('metadata', {})
                if isinstance(metadata, str):
                    try:
                        metadata = json.loads(metadata)
                    except json.JSONDecodeError:
                        logging.warning(f"Falha ao decodificar metadata JSON para chunk {item.get('document_id')}. Metadata: {metadata}")
                        metadata = {}
                
                chunk_data = {
                    "document_id": item.get('document_id'),
                    "content": item.get('content'),
                    "metadata": metadata,
                    "current_annotation_status": item.get('annotation_status'),
                    "current_indexing_status": item.get('indexing_status'),
                    "current_keep_status": item.get('keep')
                }
                all_chunks_to_process.append(chunk_data)
            logging.info(f"{len(all_chunks_to_process)} chunks com status 'pending' encontrados para processar.")
        else:
            logging.info("Nenhum chunk com status 'pending' encontrado no Supabase.")
            end_time_no_chunks = time.time()
            logging.info(f"Pipeline ETL concluído (sem chunks pendentes) em {end_time_no_chunks - start_time:.2f} segundos.")
            return

    except PostgrestAPIError as e_select:
        logging.error(f"Erro na API do Supabase ao buscar chunks pendentes: {e_select}", exc_info=True)
        return # Parar se não conseguir buscar
    except Exception as e_fetch:
        logging.error(f"Erro inesperado ao buscar chunks pendentes no Supabase: {e_fetch}", exc_info=True)
        return # Parar se não conseguir buscar

    # --- Processamento em Paralelo dos Chunks (EXISTENTE) ---
    total_chunks = len(all_chunks_to_process)
    chunks_processed_count = 0
    successful_chunks = 0
    failed_chunks = 0

    with ThreadPoolExecutor(max_workers=max_workers_pipeline) as executor:
        futures = {
            executor.submit(process_single_chunk, chunk, annotator, r2r_client, supabase, skip_annotation, skip_indexing): chunk.get('document_id', 'ID_Desconhecido')
            for chunk in all_chunks_to_process
        }

        for future in as_completed(futures):
            chunks_processed_count += 1
            chunk_id = futures[future]
            progress = (chunks_processed_count / total_chunks) * 100 if total_chunks > 0 else 0

            try:
                success = future.result()
                logging.debug(f"[run_pipeline loop] Chunk {chunk_id} completou. Resultado: {success}. Progresso: {progress:.2f}%")
                if success:
                    successful_chunks += 1
                else:
                    failed_chunks += 1
                    logging.warning(f"Processamento do chunk {chunk_id} FALHOU ou retornou False.")
            except Exception as exc:
                 failed_chunks += 1
                 logging.error(f"[run_pipeline loop] Exceção ao processar chunk {chunk_id}: {exc}. Progresso: {progress:.2f}%", exc_info=True)

    logging.info(f"Processamento de todos os {total_chunks} chunks buscados concluído. Sucesso: {successful_chunks}, Falha: {failed_chunks}.")

    end_time = time.time()
    logging.info(f"Pipeline ETL (Modo Consulta Supabase) concluído em {end_time - start_time:.2f} segundos.")

def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL para RAG - Modo Consulta Supabase")
    parser.add_argument("--skip-annotation", action="store_true", help="Pular a etapa de anotação com CrewAI.")
    parser.add_argument("--skip-indexing", action="store_true", help="Pular a etapa de indexação no R2R Cloud.")
    parser.add_argument("--batch-size", type=int, default=100, help="Tamanho do lote para buscar chunks do Supabase.")
    parser.add_argument("--max-workers", type=int, default=5, help="Número máximo de workers para processar chunks em paralelo.")

    args = parser.parse_args()
    logger.info("Iniciando pipeline ETL (Modo Consulta Supabase)...")

    run_pipeline(
        local_dir='', # Não usado
        dry_run=False, # Não usado
        dry_run_limit=None, # Não usado
        skip_annotation=args.skip_annotation,
        skip_indexing=args.skip_indexing,
        batch_size=args.batch_size,
        max_workers_pipeline=args.max_workers
    )

    logger.info("Pipeline ETL (Modo Consulta Supabase) finalizada.")

if __name__ == "__main__":
    main() 