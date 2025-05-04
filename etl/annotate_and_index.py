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
# from ingestion.video_transcription import process_all_videos_in_directory
from ingestion.video_transcription import process_all_videos_in_directory
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
    """(REMOVIDO - Lógica agora integrada em gdrive_ingest)
    Converte o resultado da transcrição de vídeo para o formato esperado [{'content': str, 'metadata': dict}].
    """
    # Função mantida vazia ou pode ser removida completamente
    logger.warning("Função process_video_data não é mais usada diretamente no ETL.")
    return []

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
def _insert_initial_chunks_supabase(supabase_client: Client, batch: List[Dict[str, Any]], source_name: str) -> bool:
    """Insere o lote inicial de chunks no Supabase (com retentativas)."""
    if not supabase_client or not batch:
        logger.warning(f"[Supabase Insert Initial] Cliente não disponível ou lote vazio para {source_name}. Pulando inserção.")
        return False # Ou True se lote vazio for considerado sucesso? False parece mais seguro.

    logger.info(f"[Supabase Insert Initial] Tentando inserir {len(batch)} chunks em lote inicial para {source_name}...")
    start_initial_insert = time.time()
    try:
        # Aplicar retry aqui na chamada
        response: PostgrestAPIResponse = default_retry(supabase_client.table('documents').insert(batch).execute)()
        initial_insert_time = time.time() - start_initial_insert
        if response.data or (hasattr(response, 'status_code') and 200 <= response.status_code < 300):
            logging.info(f"[Supabase Insert Initial] Inserção inicial bem-sucedida para {source_name} em {initial_insert_time:.2f} segundos.")
            return True
        else:
             logger.error(f"[Supabase Insert Initial] Falha FINAL na inserção inicial para {source_name} após retentativas. Resposta: {getattr(response, 'data', 'N/A')}, Status: {getattr(response, 'status_code', 'N/A')}")
             return False
    except (PostgrestAPIError) as e:
        logger.error(f"[Supabase Insert Initial] Erro FINAL API na inserção inicial para {source_name} após retentativas: {e}", exc_info=True)
        return False # Falha crítica
    except Exception as general_exception:
        logger.error(f"[Supabase Insert Initial] Erro FINAL inesperado na inserção inicial para {source_name} após retentativas", exc_info=True)
        return False # Falha crítica

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

def process_single_source_document(
    source_data: Dict[str, Any],
    annotator: AnnotatorAgent,
    r2r_client_instance: R2RClientWrapper,
    supabase_client: Client,
    skip_annotation: bool = False,
    skip_indexing: bool = False,
    max_workers_r2r_upload: int = 5
) -> Optional[int]:
    """
    Processa um único documento fonte (arquivo de texto, transcrição de vídeo).

    Este é o coração do processamento paralelo. Ele executa as seguintes etapas
    para um único item de dados de entrada:
    1. Verifica no Supabase se o `file_id` (nos metadados) já foi processado.
    2. Se não processado, divide o conteúdo em chunks.
    3. **Salva TODOS os chunks no Supabase com status inicial 'pending'.**
    4. (Opcional) Envia os chunks para o `AnnotatorAgent`.
    5. **(Refatorar em 40.3) Atualiza o status da anotação no Supabase.**
    6. (Opcional) Envia os chunks marcados como `keep=True` para o R2R para indexação.
    7. **(Refatorar em 40.3) Atualiza o status da indexação no Supabase.**
    8. Se o processamento geral do arquivo for bem-sucedido, marca o `file_id` como processado no Supabase.

    Args:
        source_data (Dict[str, Any]): Dicionário contendo 'content' e 'metadata' da fonte.
                                      'metadata' deve idealmente conter 'file_id'.
        annotator (AnnotatorAgent): Instância do agente de anotação.
        r2r_client_instance (R2RClientWrapper): Instância do cliente R2R.
        supabase_client (Client): Instância do cliente Supabase.
        skip_annotation (bool): Se True, pula a etapa de anotação.
        skip_indexing (bool): Se True, pula o armazenamento no Supabase e indexação no R2R.
        max_workers_r2r_upload (int): Número máximo de threads para uploads paralelos ao R2R.

    Returns:
        Optional[int]: O número de chunks enviados com sucesso para o R2R,
                       ou 0 se o arquivo foi pulado (já processado),
                       ou None se ocorreu um erro crítico no processamento do arquivo.
    """
    source_name = source_data.get('filename', source_data.get('metadata', {}).get('source_name', 'unknown_source'))
    source_content = source_data.get('content')
    initial_metadata = source_data.get('metadata', {'source_name': source_name, 'origin': 'unknown'})
    source_id = initial_metadata.get('gdrive_id') or initial_metadata.get('file_id') # Buscar por 'gdrive_id' ou 'file_id'

    if not source_id:
        # Este erro ainda pode ocorrer se metadados estiverem ausentes por algum motivo
        logging.error(f"[Check Processed] Impossível verificar status para {source_name}: 'gdrive_id' ou 'file_id' não encontrado nos metadados.")
    elif supabase_client:
        # --- Adicionado: Verificar se já foi processado --- 
        logging.debug(f"[Check Processed] Verificando status para {source_name} (ID: {source_id})...")
        try:
            response = supabase_client.table('processed_files').select('file_id', count='exact').eq('file_id', source_id).execute()
            # A API retorna uma lista em response.data, e count está no atributo count
            if response.count > 0:
                logging.info(f"[Check Processed] Arquivo {source_name} (ID: {source_id}) já processado anteriormente. Pulando.")
                return 0 # Retornar 0 chunks para indicar que foi pulado
            else:
                logging.debug(f"[Check Processed] Arquivo {source_name} (ID: {source_id}) não processado anteriormente.")
        except PostgrestAPIError as e:
            logging.error(f"[Check Processed] Erro API ao verificar status para {source_id} no Supabase: {e}")
            # Logar detalhes extras se possível
            if hasattr(e, 'json') and callable(e.json):
                try: logger.error(f"  - JSON do Erro: {e.json()}")
                except Exception: pass
            # Continuar processamento mesmo com erro na verificação? Decisão: Sim, por enquanto.
        except Exception as e:
            logging.error(f"[Check Processed] Erro inesperado ao verificar status para {source_id}: {e}", exc_info=True)
            # Continuar processamento mesmo com erro na verificação? Decisão: Sim, por enquanto.
        # --- Fim Adicionado --- 

    if not source_content:
        logging.warning(f"Conteúdo vazio para {source_name}. Pulando.")
        return 0 # Não é um erro, apenas nada a fazer.

    logging.info(f"[Chunking] Iniciando para {source_name}...")
    start_chunking = time.time()
    all_chunks = split_content_into_chunks(source_content, initial_metadata)
    chunking_time = time.time() - start_chunking
    logging.info(f"[Chunking] Concluído para {source_name} em {chunking_time:.2f} segundos. {len(all_chunks)} chunks criados.")

    if not all_chunks:
        logging.warning(f"Nenhum chunk criado para {source_name}. Pulando etapas subsequentes.")
        return 0

    # --- Salvar Chunks Primeiro (com retentativas) ---
    initial_supabase_batch = []
    if supabase_client:
        logging.info(f"[Supabase Insert] Preparando {len(all_chunks)} chunks para inserção inicial...")
        start_prep_insert = time.time()
        for chunk in all_chunks:
            # Gerar e adicionar document_id (UUID) a cada chunk ANTES de salvar
            doc_id = str(uuid.uuid4())
            chunk["document_id"] = doc_id # Adiciona ao dicionário do chunk para uso posterior

            supabase_data = {
                "document_id": doc_id,
                "content": chunk.get("content"),
                "metadata": chunk.get("metadata"),
                "token_count": count_tokens(chunk.get("content", "")),
                # Adicionar status iniciais
                "annotation_status": "pending",
                "indexing_status": "pending",
                # Não definir annotation_tags, annotation_keep, etc. aqui
            }
            initial_supabase_batch.append(supabase_data)

        prep_insert_time = time.time() - start_prep_insert
        logging.debug(f"[Supabase Insert] Preparação do lote inicial levou {prep_insert_time:.2f}s")

        if initial_supabase_batch:
            # Chamar a função com retentativas
            insert_ok = _insert_initial_chunks_supabase(supabase_client, initial_supabase_batch, source_name)
            if not insert_ok:
                 return None # Erro crítico, não continuar

    elif not skip_indexing: # Se skip_indexing=False mas não temos cliente Supabase
         logging.error(f"Supabase client não está disponível, mas skip_indexing é False. Impossível prosseguir com armazenamento/status. Pulando {source_name}.")
         return None # Erro de configuração/estado

    # --- Anotação (com retentativas) ---
    processed_chunks = []
    annotation_succeeded = True
    current_time_utc = datetime.now(timezone.utc)

    if skip_annotation:
        logging.info(f"[Anotação] Pulando para {source_name} conforme solicitado.")
        processed_chunks = [{**chunk, 'tags': [], 'keep': True, 'reason': 'Annotation Skipped'} for chunk in all_chunks]
        # Atualizar status no Supabase para 'skipped'
        for chunk in processed_chunks:
            _update_chunk_status_supabase(
                supabase_client,
                chunk.get("document_id"),
                {"annotation_status": "skipped", "annotated_at": current_time_utc.isoformat()},
                "Annotation Skip"
            )
        annotation_succeeded = True # Pular é considerado um 'sucesso' para o fluxo
    elif not annotator:
         logging.warning(f"AnnotatorAgent não disponível. Pulando anotação para {source_name}.")
         processed_chunks = [{**chunk, 'tags': [], 'keep': True, 'reason': 'Annotator Not Available'} for chunk in all_chunks]
         # Atualizar status no Supabase para 'skipped' ou 'failed'? Usar 'skipped'
         for chunk in processed_chunks:
              _update_chunk_status_supabase(
                supabase_client,
                chunk.get("document_id"),
                {"annotation_status": "skipped", "annotated_at": current_time_utc.isoformat()},
                "Annotation Skip (No Agent)"
            )
         annotation_succeeded = True
    else:
        logging.info(f"[Anotação] Iniciando para {len(all_chunks)} chunks de {source_name}...")
        try:
            # Chamar a função com retentativas
            annotated_chunks_results = _run_annotator_with_retry(annotator, all_chunks, source_name)
            annotation_time = time.time() - start_annotation
            logging.info(f"[Anotação] Concluída para {source_name} em {annotation_time:.2f} segundos. {len(annotated_chunks_results)} chunks processados pela anotação.")
            processed_chunks = annotated_chunks_results

            # Atualizar Supabase com resultados da anotação (sucesso)
            update_annotation_success_count = 0
            current_time_utc = datetime.now(timezone.utc) # Atualizar timestamp
            for chunk in processed_chunks:
                update_payload = {
                    "annotation_status": "success",
                    "annotated_at": current_time_utc.isoformat(),
                    "annotation_tags": chunk.get("tags"),
                    "annotation_keep": chunk.get("keep"),
                    "annotation_reason": chunk.get("reason")
                }
                if _update_chunk_status_supabase(supabase_client, chunk.get("document_id"), update_payload, "Annotation Success"):
                    update_annotation_success_count += 1
            logging.info(f"[Supabase Update - Annotation Success] {update_annotation_success_count}/{len(processed_chunks)} chunks atualizados.")
            annotation_succeeded = True

        except Exception as e: # Captura exceção final após retentativas
            logging.error(f"Erro FINAL durante a execução do AnnotatorAgent para {source_name} APÓS retentativas: {e}", exc_info=False) # Log mais conciso
            processed_chunks = [{**chunk, 'tags': [], 'keep': False, 'reason': f'Annotation Failed after retries: {e}'} for chunk in all_chunks]
            annotation_succeeded = False
            
            # Atualizar Supabase com status de falha na anotação
            update_annotation_fail_count = 0
            current_time_utc = datetime.now(timezone.utc) # Atualizar timestamp
            for chunk in processed_chunks: # Usar processed_chunks aqui contém a razão da falha
                 update_payload = {
                    "annotation_status": "failed",
                    "annotated_at": current_time_utc.isoformat(),
                    "annotation_reason": chunk.get("reason") # Salvar a razão da falha
                 }
                 if _update_chunk_status_supabase(supabase_client, chunk.get("document_id"), update_payload, "Annotation Failure"):
                     update_annotation_fail_count += 1
            logging.warning(f"[Supabase Update - Annotation Failure] {update_annotation_fail_count}/{len(processed_chunks)} chunks marcados como falha na anotação.")


    # --- Indexação R2R (com retentativas no upload) ---
    chunks_sent_to_r2r_count = 0
    # Determinar sucesso geral do arquivo (storage_successful) apenas no final
    # Usar uma flag específica para o sucesso da indexação
    indexing_step_completed_without_errors = True # Assume sucesso até que falhe

    if skip_indexing:
        logging.info(f"[Indexação R2R] Pulando para {source_name} conforme solicitado.")
        # Atualizar status de indexação para 'skipped' para todos os chunks
        update_indexing_skip_count = 0
        current_time_utc = datetime.now(timezone.utc)
        for chunk in processed_chunks: # Iterar sobre chunks após anotação
            if _update_chunk_status_supabase(
                supabase_client,
                chunk.get("document_id"),
                {"indexing_status": "skipped", "indexed_at": current_time_utc.isoformat()},
                "Indexing Skip"
            ):
                update_indexing_skip_count += 1
        logging.info(f"[Supabase Update - Indexing Skip] {update_indexing_skip_count}/{len(processed_chunks)} chunks marcados como indexação pulada.")
        indexing_step_completed_without_errors = True # Pular é considerado sucesso

    elif not r2r_client_instance:
         logging.warning(f"[Indexação R2R] Cliente R2R não disponível. Pulando indexação para {source_name}.")
         # Atualizar status de indexação para 'skipped'
         update_indexing_skip_count = 0
         current_time_utc = datetime.now(timezone.utc)
         for chunk in processed_chunks:
              if _update_chunk_status_supabase(
                    supabase_client,
                    chunk.get("document_id"),
                    {"indexing_status": "skipped", "indexed_at": current_time_utc.isoformat()},
                    "Indexing Skip (No Client)"
                ):
                   update_indexing_skip_count += 1
         logging.info(f"[Supabase Update - Indexing Skip] {update_indexing_skip_count}/{len(processed_chunks)} chunks marcados como indexação pulada (sem cliente R2R).")
         indexing_step_completed_without_errors = True

    elif not processed_chunks or not annotation_succeeded: # Se anotação falhou, não indexar
         logging.warning(f"[Indexação R2R] Pulando indexação para {source_name} devido à falha na etapa de anotação ou falta de chunks processados.")
         # Atualizar status de indexação para 'skipped' (pois não foi tentado devido a erro anterior)
         update_indexing_skip_count = 0
         current_time_utc = datetime.now(timezone.utc)
         for chunk in processed_chunks:
              if _update_chunk_status_supabase(
                    supabase_client,
                    chunk.get("document_id"),
                    {"indexing_status": "skipped", "indexed_at": current_time_utc.isoformat()},
                    "Indexing Skip (Annotation Failed)"
                ):
                   update_indexing_skip_count += 1
         logging.info(f"[Supabase Update - Indexing Skip] {update_indexing_skip_count}/{len(processed_chunks)} chunks marcados como indexação pulada (falha anotação).")
         # Mantem indexing_step_completed_without_errors = True, pois a *etapa de indexação* em si não falhou
    
    else: # Prosseguir com a indexação
        r2r_upload_tasks = [] 
        chunks_to_skip_indexing = [] # Chunks com keep=False
        for chunk in processed_chunks:
            if chunk.get("keep"): 
                doc_id = chunk.get("document_id")
                if doc_id:
                    r2r_upload_tasks.append((chunk, doc_id))
                else:
                    logging.error(f"[Indexação R2R] Chunk sem document_id encontrado para {source_name}. Pulando.")
            else:
                 chunks_to_skip_indexing.append(chunk.get("document_id")) # Coleta IDs dos chunks a serem pulados

        # Marcar chunks com keep=False como 'skipped' no Supabase
        update_indexing_keepfalse_count = 0
        current_time_utc = datetime.now(timezone.utc)
        for doc_id_to_skip in chunks_to_skip_indexing:
             if _update_chunk_status_supabase(
                   supabase_client,
                   doc_id_to_skip,
                   {"indexing_status": "skipped", "indexed_at": current_time_utc.isoformat()},
                   "Indexing Skip (Keep=False)"
               ):
                  update_indexing_keepfalse_count += 1
        if update_indexing_keepfalse_count > 0:
             logging.info(f"[Supabase Update - Indexing Skip] {update_indexing_keepfalse_count} chunks marcados como indexação pulada (keep=False).")


        if not r2r_upload_tasks:
            logging.info(f"[Indexação R2R] Nenhum chunk marcado para envio ao R2R para {source_name}.")
            # Não precisa atualizar status aqui, já feito acima para keep=False
            indexing_step_completed_without_errors = True
        else:
            logging.info(f"[Indexação R2R] Iniciando upload paralelo de {len(r2r_upload_tasks)} chunks para {source_name}...")
            start_indexing = time.time()

            # Função auxiliar interna AGORA chama a função com retentativas
            def upload_single_chunk_to_r2r(chunk_data, doc_id):
                 chunk_metadata = chunk_data.get("metadata", {})
                 chunk_index = chunk_metadata.get("chunk_index", -1) # Log purpose
                 temp_file_path = None
                 try:
                     with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".txt", encoding='utf-8') as temp_file:
                         temp_file.write(chunk_data.get("content")) # Obter conteúdo aqui
                         temp_file_path = temp_file.name
                         # ... (preparar r2r_metadata) ...
                         r2r_metadata = chunk_metadata.copy()
                         r2r_metadata['tags'] = [str(tag) for tag in chunk_data.get("tags", []) if isinstance(tag, str)]
                         r2r_metadata['reason'] = chunk_data.get("reason", "")
                         r2r_metadata['token_count'] = count_tokens(chunk_data.get("content", ""))
                         r2r_metadata.pop('test_marker', None)
                         
                         # Chamar a função com retentativas
                         upload_result = _upload_single_chunk_to_r2r_with_retry(
                             r2r_client_instance, temp_file_path, doc_id, r2r_metadata
                         )
                         upload_success = upload_result.get("success", False)
                         error_message = upload_result.get("error")
                         return {"doc_id": doc_id, "success": upload_success, "error": error_message}

                 except Exception as e: # Captura falha final da retentativa ou erro na preparação
                      logger.error(f"[R2R Upload Main Thread] Erro FINAL para doc_id: {doc_id} (chunk {chunk_index}): {e}", exc_info=False)
                      return {"doc_id": doc_id, "success": False, "error": str(e)}
                 finally:
                      if temp_file_path and os.path.exists(temp_file_path):
                          try: os.remove(temp_file_path)
                          except Exception: pass
            
            # Coletar resultados
            r2r_results = []
            with ThreadPoolExecutor(max_workers=max_workers_r2r_upload) as executor:
                future_to_doc_id = {executor.submit(upload_single_chunk_to_r2r, chunk, doc_id): doc_id for chunk, doc_id in r2r_upload_tasks}
                for future in as_completed(future_to_doc_id):
                    doc_id_completed = future_to_doc_id[future]
                    try:
                        result_dict = future.result()
                        r2r_results.append(result_dict)
                    except Exception as exc:
                        logging.error(f'[R2R Upload Main] Upload para doc_id {doc_id_completed} gerou exceção: {exc}', exc_info=True)
                        r2r_results.append({"doc_id": doc_id_completed, "success": False, "error": str(exc)})
            
            indexing_time = time.time() - start_indexing
            successful_r2r_uploads = sum(1 for r in r2r_results if r.get("success"))
            failed_r2r_uploads = len(r2r_results) - successful_r2r_uploads
            chunks_sent_to_r2r_count = successful_r2r_uploads
            logging.info(f"[Indexação R2R] Concluída para {source_name} em {indexing_time:.2f}s. {successful_r2r_uploads} S, {failed_r2r_uploads} F.")

            # Atualizar Supabase com resultados de R2R
            update_indexing_count = 0
            current_time_utc = datetime.now(timezone.utc)
            for result in r2r_results:
                doc_id_to_update = result.get("doc_id")
                is_success = result.get("success")
                status_to_set = "success" if is_success else "failed"
                update_payload = {
                    "indexing_status": status_to_set,
                    "indexed_at": current_time_utc.isoformat()
                    # Poderia adicionar a msg de erro em metadata se falhou?
                }
                if _update_chunk_status_supabase(supabase_client, doc_id_to_update, update_payload, "Indexing Result"):
                    update_indexing_count += 1
            logging.info(f"[Supabase Update - Indexing Result] {update_indexing_count}/{len(r2r_results)} chunks tiveram status de indexação atualizado.")

            # Determinar sucesso da etapa de indexação
            if failed_r2r_uploads > 0:
                 indexing_step_completed_without_errors = False
                 logging.warning(f"[Indexação R2R] {failed_r2r_uploads} falhas ocorreram durante o upload para {source_name}.")
            else:
                 indexing_step_completed_without_errors = True

    # --- Marcar Arquivo como Processado (com retentativas) ---
    processamento_geral_ok = annotation_succeeded and indexing_step_completed_without_errors
    if source_id and supabase_client and processamento_geral_ok:
         # Chamar a função com retentativas
         try:
              _mark_file_processed_supabase(supabase_client, source_id, source_name)
         except Exception as mark_exc:
             # Logar que a marcação final falhou, mas não necessariamente falhar o processo inteiro
             logger.error(f"Falha ao marcar {source_name} (ID: {source_id}) como processado, mesmo após retentativas: {mark_exc}", exc_info=True)

    # Retorna a contagem de uploads R2R bem-sucedidos
    # ou None se ocorreu erro crítico na inserção inicial.
    # Se anotação ou indexação falharam mas inserção inicial foi ok, ainda retorna a contagem.
    return chunks_sent_to_r2r_count

def run_pipeline(
    source: str,
    local_dir: str,
    dry_run: bool,
    dry_run_limit: Optional[int],
    skip_annotation: bool,
    skip_indexing: bool,
    max_workers_r2r_upload: int = 5 # Passar para process_single_source_document
):
    """
    Orquestra o pipeline ETL completo.

    1. Ingere dados da fonte especificada (Google Drive ou diretório local).
    2. Processa transcrições de vídeo (se houver).
    3. Processa cada documento/transcrição em paralelo usando ThreadPoolExecutor,
       chamando `process_single_source_document` para cada um.
    4. Limpa o diretório temporário de vídeos.
    """
    all_source_data = []
    temp_video_dir = None
    processed_successfully = True # Flag para rastrear sucesso geral

    # --- Etapa 1: Ingestão (Gdrive ou Local) ---
    if source == 'gdrive':
        logging.info("Iniciando ingestão do Google Drive...")
        start_gdrive = time.time()
        gdrive_data, temp_video_dir = ingest_all_gdrive_content(dry_run=dry_run)
        all_source_data.extend(gdrive_data)
        gdrive_time = time.time() - start_gdrive
        logging.info(f"Ingestão do Google Drive concluída em {gdrive_time:.2f} segundos. {len(gdrive_data)} itens brutos obtidos.")
    elif source == 'local':
        logging.info(f"Iniciando ingestão do diretório local: {local_dir}")
        start_local = time.time()
        local_data = ingest_local_directory(local_dir, dry_run=dry_run)
        all_source_data.extend(local_data)
        local_time = time.time() - start_local
        logging.info(f"Ingestão local concluída em {local_time:.2f} segundos. {len(local_data)} itens obtidos.")
    else:
        logging.error(f"Fonte desconhecida: {source}. Use 'gdrive' ou 'local'.")
        return

    # Aplicar limite de dry-run se especificado
    if dry_run and dry_run_limit is not None and len(all_source_data) > dry_run_limit:
        logging.warning(f"Dry run limitado aos primeiros {dry_run_limit} itens de {len(all_source_data)}. ")
        all_source_data = all_source_data[:dry_run_limit]

    # --- Etapa 2: Processamento de Vídeos (se GDrive foi usado) ---
    video_transcriptions = []
    if temp_video_dir and os.path.exists(temp_video_dir):
        logging.info("Iniciando processamento de transcrição de vídeos...")
        start_video = time.time()
        # Assumindo que process_all_videos_in_directory retorna lista de dicts com 'content' e 'metadata'
        transcription_results = process_all_videos_in_directory(temp_video_dir)
        video_transcriptions.extend(transcription_results)
        video_time = time.time() - start_video
        logging.info(f"Processamento de vídeos concluído em {video_time:.2f} segundos. {len(video_transcriptions)} transcrições obtidas.")
        # Adicionar transcrições aos dados a serem processados
        all_source_data.extend(video_transcriptions)
    elif temp_video_dir:
         logging.warning(f"Diretório temporário de vídeo {temp_video_dir} não encontrado após ingestão. Transcrições puladas.")

    # --- Etapa 3: Processamento Paralelo (Chunking, Anotação, Armazenamento, Indexação) ---
    if not all_source_data:
        logging.warning("Nenhum dado fonte encontrado ou obtido após ingestão/transcrição. Pipeline encerrado.")
    else:
        logging.info(f"Iniciando processamento paralelo de {len(all_source_data)} documentos/transcrições...")
        start_parallel = time.time()

        # Inicializar AnnotatorAgent UMA VEZ aqui se não for pular anotação
        annotator_instance = None
        if not skip_annotation:
            try:
                annotator_instance = AnnotatorAgent()
            except Exception as agent_init_error:
                logging.error(f"Falha ao inicializar AnnotatorAgent: {agent_init_error}. Anotação será pulada.", exc_info=True)
                skip_annotation = True # Forçar pulo da anotação se o agente falhar

        total_chunks_indexed = 0
        files_processed_count = 0
        files_failed_count = 0

        # Determinar número de workers para processamento de documentos
        # Usar um número razoável, talvez baseado em CPU cores ou fixo
        max_workers_docs = int(os.cpu_count() or 4) 
        logging.info(f"Usando {max_workers_docs} workers para processamento paralelo de documentos.")

        with ThreadPoolExecutor(max_workers=max_workers_docs) as executor:
            future_to_source = {executor.submit(
                    process_single_source_document,
                    source_doc,
                annotator_instance,
                r2r_client, # Passar a instância R2R inicializada
                supabase, # Passar a instância Supabase inicializada
                    skip_annotation,
                    skip_indexing,
                max_workers_r2r_upload
            ): source_doc.get('filename', source_doc.get('metadata', {}).get('source_name', 'unknown_source')) for source_doc in all_source_data}

            for future in as_completed(future_to_source):
                source_name_completed = future_to_source[future]
                files_processed_count += 1
                try:
                    result = future.result() # Retorna contagem de chunks R2R ou None em erro
                    if result is not None:
                        total_chunks_indexed += result
                        logging.info(f"Processamento de '{source_name_completed}' concluído. {result} chunks indexados.")
                    else:
                        # result é None, indica erro crítico no processamento deste arquivo
                        files_failed_count += 1
                        logging.error(f"Processamento de '{source_name_completed}' falhou com erro crítico.")
                        processed_successfully = False # Marcar falha geral
                except Exception as exc:
                    files_failed_count += 1
                    logging.error(f'Processamento de "{source_name_completed}" gerou exceção: {exc}', exc_info=True)
                    processed_successfully = False # Marcar falha geral

        parallel_time = time.time() - start_parallel
        logging.info(f"Processamento paralelo concluído em {parallel_time:.2f} segundos.")
        logging.info(f"Resumo: {files_processed_count} arquivos tentados, {files_failed_count} falhas críticas. Total de {total_chunks_indexed} chunks indexados no R2R.")

    # --- Etapa 4: Limpeza --- 
    if temp_video_dir and os.path.exists(temp_video_dir):
        try:
            shutil.rmtree(temp_video_dir)
            logging.info(f"Diretório temporário de vídeos {temp_video_dir} limpo com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao limpar diretório temporário {temp_video_dir}: {e}")

    if not processed_successfully:
        logging.error("Pipeline ETL concluído com uma ou mais falhas críticas.")
        # Considerar lançar uma exceção ou retornar um código de erro aqui?
        # sys.exit(1) # Descomentar para fazer o processo sair com erro
    else:
        logging.info("Pipeline ETL concluído com sucesso.")

def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL para processar conteúdo (Gdrive/Local), anotar (CrewAI) e indexar (R2R/Supabase).")
    parser.add_argument('--source', type=str, required=True, choices=['gdrive', 'local'], help="Fonte dos dados ('gdrive' ou 'local')")
    parser.add_argument('--local-dir', type=str, help="Diretório local para usar se source='local'")
    parser.add_argument('--dry-run', action='store_true', help="Executa sem fazer alterações reais (downloads, anotações, uploads)")
    parser.add_argument('--dry-run-limit', type=int, help="Limita o número de itens processados no dry run")
    parser.add_argument('--skip-annotation', action='store_true', help="Pula a etapa de anotação com CrewAI")
    parser.add_argument('--skip-indexing', action='store_true', help="Pula o armazenamento no Supabase e indexação no R2R")
    parser.add_argument('--max-workers-r2r', type=int, default=5, help="Número máximo de workers para upload paralelo no R2R")

    args = parser.parse_args()

    if args.source == 'local' and not args.local_dir:
        parser.error("--local-dir é obrigatório quando --source='local'")

    logging.info(f"Iniciando pipeline ETL com os seguintes argumentos: {args}")

    run_pipeline(
        source=args.source,
        local_dir=args.local_dir,
        dry_run=args.dry_run,
        dry_run_limit=args.dry_run_limit,
        skip_annotation=args.skip_annotation,
        skip_indexing=args.skip_indexing,
        max_workers_r2r_upload=args.max_workers_r2r
    )

    logging.info("Pipeline ETL finalizado.")

if __name__ == "__main__":
    main() 