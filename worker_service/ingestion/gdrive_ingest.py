# ingestion/gdrive_ingest.py
"""
Módulo para ingestão de conteúdo do Google Drive.

Responsável por:
- Autenticar na API do Google Drive usando credenciais de Service Account.
- Listar arquivos em pastas configuradas (via variáveis de ambiente).
- Baixar arquivos de documentos suportados (TXT, PDF, DOCX) e vídeos.
- Exportar Google Docs para formato DOCX.
- Extrair texto de documentos baixados/exportados usando Docling ou decodificação direta.
- Retornar uma lista de dicionários representando os itens ingeridos (documentos com
  texto extraído e vídeos com caminho para o arquivo baixado), incluindo metadados relevantes.

"""
import os
import io
import json
import argparse
import unicodedata
import logging
import tiktoken
import uuid
import shutil
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from docling.document_converter import DocumentConverter
import tempfile
from typing import Any, List, Dict, Optional, Set
# Removida a importação de ImageProcessor, pois não estava sendo usada e pode não existir no contexto do worker
# from ingestion.image_processor import ImageProcessor 
from supabase import create_client, Client, PostgrestAPIResponse
from postgrest.exceptions import APIError as PostgrestAPIError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import base64
import re
import time
from dateutil.parser import isoparse

# Adicionar importação do pipeline de anotação/indexação
# O caminho aqui deve ser relativo à raiz do PYTHONPATH configurado no Dockerfile.
# Se gdrive_ingest.py está em worker_service/ingestion/
# e annotate_and_index.py está em worker_service/etl/
# então o import deve ser:
from etl.annotate_and_index import run_pipeline as run_annotation_pipeline

# Configurar logger
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

load_dotenv()

supabase_url: str = os.environ.get("SUPABASE_URL")
supabase_key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase_client: Optional[Client] = None
try:
    if supabase_url and supabase_key:
        supabase_client = create_client(supabase_url, supabase_key)
        logger.info("Cliente Supabase inicializado com sucesso em gdrive_ingest.")
    else:
        logger.warning("URL ou Chave Supabase não encontradas. Verificação de arquivos processados desabilitada.")
except Exception as e:
    logger.error(f"Erro ao inicializar cliente Supabase em gdrive_ingest: {e}", exc_info=True)
    supabase_client = None

SUPPORTED_MIME_TYPES = {
    'application/vnd.google-apps.document': 'gdoc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
    'application/pdf': 'pdf',
    'text/plain': 'txt',
    'video/mp4': 'mp4',
    'video/quicktime': 'mov',
    'video/x-msvideo': 'avi',
    'video/x-matroska': 'mkv',
    'video/mpeg': 'mpeg',
    'video/webm': 'webm'
}
DOCUMENT_MIME_TYPES = {
    'application/vnd.google-apps.document',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/pdf',
    'text/plain'
}
VIDEO_MIME_TYPES = {
    'video/mp4',
    'video/quicktime',
    'video/x-msvideo',
    'video/x-matroska',
    'video/mpeg',
    'video/webm'
}
GDRIVE_EXPORT_MIME = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

IGNORED_FILENAMES = {
    '.gitignore', '.env', 'requirements.txt', 'Pipfile', 'Pipfile.lock',
    'poetry.lock', 'package.json', 'package-lock.json', 'README.md',
    'LICENSE', 'docker-compose.yml', 'Dockerfile', '.dockerignore',
    '.qa_piplist.txt', 'test.txt', '.DS_Store',
}
IGNORED_EXTENSIONS = {
    '.py', '.js', '.ts', '.sql', '.sh', '.ipynb', '.json', '.yml',
    '.yaml', '.log', '.csv', '.tsv', '.xml', '.zip', '.gz', '.tar',
    '.exe', '.dll', '.so', '.class', '.jar', '.md',
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp',
    '.heic',
}

OUTPUT_DIR = '/storage/ingest_output' # No Railway, geralmente é /app/storage ou um volume montado

try:
    docling_converter = DocumentConverter()
    logging.info("Docling DocumentConverter inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar Docling DocumentConverter: {e}", exc_info=True)
    docling_converter = None

# image_processor = ImageProcessor() # Removido

processed_folders: Set[str] = set()

try:
    tokenizer = tiktoken.get_encoding("cl100k_base")
except Exception as e:
    logger.warning(f"Falha ao carregar tokenizer tiktoken 'cl100k_base', usando 'p50k_base': {e}")
    try:
        tokenizer = tiktoken.get_encoding("p50k_base")
    except Exception as e2:
         logger.error(f"Falha ao carregar qualquer tokenizer tiktoken: {e2}. Contagem de tokens não funcionará.")
         tokenizer = None

RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    PostgrestAPIError,
)
default_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS)
)

@default_retry
def check_folder_processed(supabase_cli: Client, folder_id: str) -> Optional[datetime]:
    if not supabase_cli:
        logger.warning("Supabase client não disponível para check_folder_processed.")
        return None
    try:
        logger.debug(f"[RPC DEBUG] Chamando check_processed_folder para {folder_id}")
        response = supabase_cli.rpc(
            "check_processed_folder", {"folder_id_param": folder_id}
        ).execute()
        logger.debug(f"[RPC DEBUG] Resposta RPC crua para {folder_id}: {response}")
        timestamp_str = None
        if isinstance(response.data, list) and len(response.data) > 0:
             if isinstance(response.data[0], dict):
                 func_name = "check_processed_folder"
                 if func_name in response.data[0]:
                     timestamp_str = response.data[0][func_name]
                 else:
                     timestamp_str = list(response.data[0].values())[0] if response.data[0] else None
             else:
                 timestamp_str = response.data[0]
        elif not isinstance(response.data, list):
             timestamp_str = response.data
        logger.debug(f"[RPC DEBUG] Timestamp string extraído para {folder_id}: {timestamp_str}")
        if timestamp_str:
            try:
                if isinstance(timestamp_str, str):
                     if timestamp_str.endswith('Z'):
                         timestamp_str = timestamp_str[:-1] + '+00:00'
                     dt = isoparse(timestamp_str)
                     if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                         dt = dt.replace(tzinfo=timezone.utc)
                     else:
                         dt = dt.astimezone(timezone.utc)
                     logger.debug(f"[RPC DEBUG] Timestamp convertido para datetime para {folder_id}: {dt}")
                     return dt
                else:
                    logger.warning(f"[RPC DEBUG] Timestamp para {folder_id} não é string: {timestamp_str}")
            except ValueError as e:
                logger.error(f"Erro ao converter timestamp '{timestamp_str}' para {folder_id}: {e}")
        else:
             logger.debug(f"[RPC DEBUG] Nenhum timestamp retornado para {folder_id}.")
        return None
    except PostgrestAPIError as e:
        logger.error(f"Erro API Supabase RPC check_processed_folder para {folder_id}: {e.message}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado RPC check_processed_folder para {folder_id}: {e}", exc_info=True)
        return None

@default_retry
def mark_folder_processed(supabase_cli: Client, folder_id: str, folder_name: Optional[str]) -> bool:
    if not supabase_cli:
        logger.warning("Supabase client não disponível para mark_folder_processed.")
        return False
    try:
        params = {'folder_id_param': folder_id}
        if folder_name:
            params['folder_name_param'] = folder_name
        response = supabase_cli.rpc('mark_folder_processed', params).execute()
        logger.info(f"Pasta {folder_name or ''} (ID: {folder_id}) marcada/atualizada como processada via RPC.")
        return True
    except PostgrestAPIError as api_error:
        if (
            ("relation \"processed_folders\" does not exist" in str(api_error.message).lower()) or
            ("function public.mark_folder_processed" in str(api_error.message).lower() and "does not exist" in str(api_error.message).lower())
        ):
            logger.error(f"Tabela 'processed_folders' ou RPC 'mark_folder_processed' não existe. Não foi possível marcar {folder_id}.", exc_info=True)
            return False
        logger.error(f"Erro API Supabase RPC mark_folder_processed para {folder_id}: {api_error.message}", exc_info=True)
        return False
    except Exception as mark_err:
        logger.error(f"Erro inesperado RPC mark_folder_processed para {folder_id}: {mark_err}", exc_info=True)
        return False

def count_tokens(text: str) -> int:
    if not tokenizer:
        logger.warning("Tokenizer tiktoken não disponível, retornando contagem de caracteres.")
        return len(text)
    return len(tokenizer.encode(text))

def split_content_into_chunks(text, base_metadata=None, max_chunk_tokens=2048, min_chunk_chars=300, model_name=None, llm_api_key=None):
    import os
    import requests
    import time
    import uuid
    import json as _json

    logger.info(f"[CHUNKING] Iniciando split_content_into_chunks (len={len(text)})...")
    if not text or not isinstance(text, str) or len(text.strip()) < min_chunk_chars:
        logger.warning("Texto vazio ou curto. Retornando chunk único.")
        return [{
            "content": text.strip(),
            "metadata": {**(base_metadata or {}), "chunk_index": 0, "total_chunks_in_doc": 1, "split_type": "single_or_short"}
        }]
    try:
        model = model_name or os.getenv("OPENAI_MODEL", "gpt-4o")
        api_key = llm_api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OPENAI_API_KEY não definido.")
        prompt = (
            f"Você é um assistente de NLP especialista em segmentação de texto para RAG.\n"
            f"Divida o texto abaixo em blocos coesos, cada um com até {max_chunk_tokens} tokens.\n"
            f"Evite dividir frases ou tópicos no meio.\n"
            f"Retorne uma lista JSON de pares [início, fim] (offsets de caractere).\n"
            f"Evite chunks muito curtos (<{min_chunk_chars} caracteres).\n"
            f"Não inclua explicações, apenas a lista JSON.\n"
            f"Texto:\n" + text[:12000]
        )
        logger.info(f"[CHUNKING][IA] Chamando OpenAI (modelo={model})...")
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        data = {
            "model": model,
            "messages": [
                {"role": "system", "content": "Você é um assistente de NLP especialista em chunking."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 512,
            "temperature": 0.1
        }
        response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=data, timeout=90)
        if response.status_code != 200:
            logger.warning(f"[CHUNKING][IA] Falha OpenAI: {response.status_code} {response.text}")
            raise RuntimeError("OpenAI API error")
        result = response.json()
        llm_reply = result["choices"][0]["message"]["content"]
        try:
            offsets = _json.loads(llm_reply)
            if not isinstance(offsets, list) or not all(isinstance(x, list) and len(x) == 2 for x in offsets):
                raise ValueError("Formato de offsets inválido")
        except Exception as parse_err:
            logger.warning(f"[CHUNKING][IA] Falha parsear offsets: {parse_err}. Fallback.")
            offsets = None
        if offsets:
            logger.info(f"[CHUNKING][IA] LLM sugeriu {len(offsets)} splits.")
            chunks = []
            for idx, (start, end) in enumerate(offsets):
                chunk_text = text[start:end].strip()
                if not chunk_text: continue
                chunk_meta = {**(base_metadata or {}),
                              "chunk_index": idx,
                              "total_chunks_in_doc": len(offsets),
                              "split_type": "llm_semantic_gpt4o"}
                chunks.append({"content": chunk_text, "metadata": chunk_meta})
            if chunks: return chunks
    except Exception as e:
        logger.error(f"[CHUNKING][IA] Erro chunking LLM: {e}. Fallback.")
        time.sleep(1)

    logger.info("[CHUNKING][FALLBACK] Usando chunking por parágrafo/tamanho.")
    paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
    chunks = []
    current = ""
    idx = 0
    for p in paragraphs:
        if not current:
            current = p
        elif count_tokens(current + "\n\n" + p) <= max_chunk_tokens:
            current += "\n\n" + p
        else:
            chunk_meta = {**(base_metadata or {}),
                          "chunk_index": idx,
                          "total_chunks_in_doc": None,
                          "split_type": "fallback_paragraph_or_size"}
            chunks.append({"content": current, "metadata": chunk_meta})
            idx += 1
            current = p
    if current:
        chunk_meta = {**(base_metadata or {}),
                      "chunk_index": idx,
                      "total_chunks_in_doc": None,
                      "split_type": "fallback_paragraph_or_size"}
        chunks.append({"content": current, "metadata": chunk_meta})
    total = len(chunks)
    for c in chunks:
        c["metadata"]["total_chunks_in_doc"] = total
    logger.info(f"[CHUNKING][FALLBACK] Gerados {total} chunks por fallback.")
    return chunks

@default_retry
def _insert_initial_chunks_supabase(supabase_cli: Client, batch: List[Dict[str, Any]], source_name: str) -> bool:
    if not supabase_cli or not batch:
        logger.warning(f"Supabase client não disponível ou lote vazio para {source_name}.")
        return False
    records_to_insert = []
    for chunk_data in batch:
        metadata = chunk_data.get('metadata', {})
        if not isinstance(metadata, dict):
            logger.warning(f"Metadados inválidos para chunk em {source_name}: {chunk_data}")
            continue
        records_to_insert.append({
            'document_id': metadata.get('document_id'),
            'content': chunk_data.get('content'),
            'metadata': metadata,
            'annotation_status': 'pending',
            'indexing_status': 'pending'
        })
    if not records_to_insert:
         logger.warning(f"Nenhum registro válido para inserir para {source_name}.")
         return False
    try:
        logger.info(f"Inserindo {len(records_to_insert)} chunks para {source_name}...")
        response: PostgrestAPIResponse = supabase_cli.table('documents').insert(records_to_insert).execute()
        if hasattr(response, 'data') and response.data:
             logger.info(f"Sucesso: {len(response.data)} chunks inseridos para {source_name}.")
             return True
        else:
             error_details = getattr(response, 'error', None) or getattr(response, 'message', 'Detalhes indisponíveis')
             logger.error(f"Falha inserir chunks para {source_name}: {error_details}")
             return False
    except PostgrestAPIError as api_error:
         logger.error(f"Erro API Postgrest inserir chunks para {source_name}: {api_error}", exc_info=True)
         try:
             logger.error(f"Detalhes Postgrest: Code={api_error.code}, Details={api_error.details}, Hint={api_error.hint}, Message={api_error.message}")
         except Exception: pass
         return False
    except Exception as e:
        logger.error(f"Erro inesperado inserir chunks para {source_name}: {e}", exc_info=True)
        return False

def authenticate_gdrive():
    creds = None
    creds_content_b64 = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64')
    if creds_content_b64:
        try:
            logger.info("Autenticando via GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64...")
            creds_content_json = base64.b64decode(creds_content_b64).decode('utf-8')
            credentials_dict = json.loads(creds_content_json)
            creds = service_account.Credentials.from_service_account_info(info=credentials_dict, scopes=SCOPES)
            logger.info("Autenticação via conteúdo JSON (Base64) OK.")
        except Exception as e:
            logger.warning(f"Falha GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64: {e}. Fallback via path...")
            creds = None
    if not creds:
        creds_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        if creds_path and os.path.exists(creds_path):
            try:
                logger.info(f"Autenticando via GOOGLE_SERVICE_ACCOUNT_JSON (path: {creds_path})...")
                creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
                logger.info("Autenticação via path OK.")
            except Exception as e:
                logger.error(f"Falha autenticar via path {creds_path}: {e}")
                creds = None
        else:
            logger.warning("GOOGLE_SERVICE_ACCOUNT_JSON (path) não definido ou inválido.")
    if not creds:
        error_msg = "Falha autenticar GDrive. Nenhuma credencial válida."
        logger.error(error_msg)
        raise ValueError(error_msg)
    try:
        service = build('drive', 'v3', credentials=creds)
        logger.info("Serviço Google Drive API construído.")
        return service
    except HttpError as auth_error:
        logger.error(f"Erro HTTP construção serviço GDrive: {auth_error}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado construção serviço GDrive: {e}")
        raise

def export_and_download_gdoc(service, file_id, export_mime_type):
    logger.debug(f"  -> Exportando GDoc (ID: {file_id}) para {export_mime_type}...")
    try:
        request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        logger.debug(f"  -> Exportação concluída para {file_id}.")
        return fh.getvalue()
    except HttpError as error:
        logger.error(f"   -> Erro HTTP {error.resp.status} export GDoc {file_id}: {error}")
        try: logger.error(f"   -> Conteúdo erro (export): {error.content.decode('utf-8')}")
        except Exception: pass
        return None
    except Exception as e:
        logger.error(f"   -> Erro inesperado export GDoc {file_id}: {e}", exc_info=True)
        return None

def extract_text_from_file(mime_type: str, file_content_bytes: bytes, file_name: str) -> Optional[str]:
    logger.debug(f"   -> Iniciando extração texto para {file_name} ({mime_type})")
    text_content = None
    if not file_content_bytes:
        logger.warning(f"   -> Conteúdo binário vazio para {file_name}")
        return None
    try:
        if mime_type == 'text/plain':
            try:
                text_content = file_content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                 logger.warning(f"    -> Erro decodificar TXT {file_name} UTF-8, tentando latin-1.")
                 try: text_content = file_content_bytes.decode('latin-1')
                 except Exception as decode_err: logger.error(f"    -> Falha decodificar TXT {file_name} latin-1: {decode_err}")
            except Exception as txt_err: logger.error(f"    -> Erro processar TXT {file_name}: {txt_err}")
        elif docling_converter and (mime_type == GDRIVE_EXPORT_MIME or mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' or mime_type == 'application/pdf'):
            temp_file_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{os.path.basename(file_name)}") as temp_input_file:
                    temp_input_file.write(file_content_bytes)
                    temp_file_path = temp_input_file.name
                logger.debug(f"    -> Usando Docling para {temp_file_path} (origem: {file_name})")
                conversion_result = docling_converter.convert(temp_file_path)
                text_content = conversion_result.document.export_to_markdown()
                logger.debug(f"    -> Docling extraiu texto de {file_name}.")
            except Exception as docling_err: logger.error(f"    -> Erro Docling {file_name}: {docling_err}", exc_info=True)
            finally:
                 if temp_file_path and os.path.exists(temp_file_path):
                     try: os.remove(temp_file_path)
                     except Exception as rem_err: logger.warning(f"     -> Falha remover temp {temp_file_path}: {rem_err}")
        elif not docling_converter and mime_type != 'text/plain':
             logger.error(f"   -> Docling não inicializado. Não é possível processar {file_name} ({mime_type}).")
        else:
            logger.warning(f"   -> Tipo MIME não suportado ou Docling não aplicável: {mime_type} para {file_name}")
        if text_content:
            text_content = unicodedata.normalize('NFKC', text_content).strip()
            if text_content:
                 logger.debug(f"   -> Extração texto OK para {file_name}.")
                 return text_content
            else: logger.warning(f"   -> Texto extraído vazio para {file_name}.")
        else: logger.warning(f"   -> Falha extração texto para {file_name}.")
        return None
    except Exception as e:
        logger.error(f"   -> Erro inesperado extração texto para {file_name} ({mime_type}): {e}", exc_info=True)
        return None

def download_file(service, file_id):
    logger.debug(f"  -> Baixando arquivo (ID: {file_id})...")
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        logger.debug(f"  -> Download concluído para {file_id}.")
        return fh.getvalue()
    except HttpError as error:
        logger.error(f"  -> Erro HTTP {error.resp.status} download {file_id}: {error}")
        try: logger.error(f"   -> Conteúdo erro (download): {error.content.decode('utf-8')}")
        except Exception: pass
        return None
    except Exception as e:
        logger.error(f"  -> Erro inesperado download {file_id}: {e}", exc_info=True)
        return None

def is_supported_image(file: Dict[str, Any]) -> bool: # Não usado atualmente
    return False

def ingest_gdrive_folder(service, folder_name: str, folder_id: str, dry_run: bool = False, access_level: Optional[str] = None, current_path: str = "") -> bool:
    folder_path_log = os.path.join(current_path, folder_name)
    logger.info(f"\n[{folder_path_log}] Iniciando ingestão: ID={folder_id}, Access={access_level}")
    if folder_id in processed_folders:
        logger.info(f"[{folder_path_log}] Pulando: Já processada nesta sessão.")
        return True
    last_processed_at: Optional[datetime] = None
    if supabase_client:
        last_processed_at = check_folder_processed(supabase_client, folder_id)
    else:
        logger.debug(f"[{folder_path_log}] Supabase client não disponível, cache persistente off.")
    gdrive_query = f"'{folder_id}' in parents and trashed = false"
    query_description = "Buscando todos os itens"
    if last_processed_at:
        timestamp_iso = last_processed_at.isoformat()
        if timestamp_iso.endswith('+00:00'):
             timestamp_iso = timestamp_iso[:-6] + 'Z'
        elif not timestamp_iso.endswith('Z'):
             logger.warning(f"Timestamp {last_processed_at} não parece UTC. Formatando e adicionando 'Z'.")
             timestamp_iso = last_processed_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        gdrive_query += f" and modifiedTime > '{timestamp_iso}'"
        query_description = f"Buscando itens modificados desde {timestamp_iso}"
        logger.info(f"[{folder_path_log}] Pasta no cache (proc. em {last_processed_at}). {query_description}")
    else:
        logger.info(f"[{folder_path_log}] Pasta não no cache persistente. {query_description}")
    overall_success = True
    page_token = None
    all_items_to_process = []
    folder_modified_since_last_check = False
    logger.info(f"[{folder_path_log}] Executando query GDrive: {gdrive_query}")
    while True:
        try:
            response = service.files().list(
                q=gdrive_query, spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, size, parents, capabilities, webViewLink)',
                pageToken=page_token
            ).execute()
            files_in_current_page = response.get('files', [])
            if files_in_current_page:
                folder_modified_since_last_check = True
                logger.info(f"  [{folder_path_log}] Encontrados {len(files_in_current_page)} itens modif/novos.")
                all_items_to_process.extend(files_in_current_page)
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                logger.debug(f"  [{folder_path_log}] Fim paginação. Total modif/novos: {len(all_items_to_process)}")
                break
        except HttpError as error:
            logger.error(f"[{folder_path_log}] Erro HTTP listar (ID: {folder_id}): {error}")
            overall_success = False; break
        except Exception as e:
            logger.error(f"[{folder_path_log}] Erro inesperado listar (ID: {folder_id}): {e}", exc_info=True)
            overall_success = False; break
    if not overall_success:
        logger.error(f"[{folder_path_log}] Abortando devido a erro na listagem.")
        return False
    if not all_items_to_process:
        logger.info(f"[{folder_path_log}] Nenhum item novo ou modificado.")
    else:
        logger.info(f"[{folder_path_log}] Processando {len(all_items_to_process)} itens novos/modificados...")
        for item in all_items_to_process:
            file_id = item.get('id')
            file_name = item.get('name', 'NomeDesconhecido')
            mime_type = item.get('mimeType')
            capabilities = item.get('capabilities', {})
            can_download = capabilities.get('canDownload', False)
            item_path_log = os.path.join(folder_path_log, file_name)
            is_folder = mime_type == 'application/vnd.google-apps.folder'
            if supabase_client:
                try:
                    @default_retry
                    def check_processed_file_db(cli, f_id):
                        return cli.table('processed_files').select('file_id', count='exact').eq('file_id', f_id).limit(1).execute()
                    supabase_response = check_processed_file_db(supabase_client, file_id)
                    if supabase_response.count > 0:
                        logger.info(f"  -> [Check Arquivo] '{item_path_log}' já em 'processed_files'. Pulando.")
                        continue
                    else:
                        logger.debug(f"  [Check Arquivo] '{item_path_log}' não em 'processed_files'. Processando.")
                except PostgrestAPIError as api_error: logger.error(f"  [Check Arquivo] Erro API Supabase '{item_path_log}': {api_error.message}. Assumindo não processado.", exc_info=False)
                except Exception as check_err: logger.error(f"  [Check Arquivo] Erro inesperado '{item_path_log}': {check_err}. Assumindo não processado.", exc_info=False)
            else:
                logger.debug(f"  Supabase client não disponível, pulando check de ARQUIVO para {item_path_log}.")
            file_ext = os.path.splitext(file_name)[1].lower()
            is_hidden = file_name.startswith('.')
            normalized_file_name = file_name.lower()
            if normalized_file_name in IGNORED_FILENAMES or file_ext in IGNORED_EXTENSIONS or is_hidden:
                logger.info(f"  -> Ignorando irrelevante/config: {item_path_log}")
                continue
            text_content = None
            file_data_for_chunking = None
            if is_folder:
                logger.info(f"  SUBPASTA MODIFICADA/NOVA: {file_name}. Ingestão recursiva...")
                if not dry_run:
                    subfolder_success = ingest_gdrive_folder(service, file_name, file_id, dry_run, access_level, folder_path_log)
                    if not subfolder_success: logger.warning(f"  -> Ingestão recursiva de {file_name} com problemas.")
                continue
            elif mime_type in DOCUMENT_MIME_TYPES:
                logger.info(f"  DOCUMENTO MODIFICADO/NOVO: {item_path_log}")
                if not can_download and mime_type != 'application/vnd.google-apps.document':
                     logger.warning(f"   -> Sem permissão download {item_path_log}. Pulando."); continue
                if dry_run: continue
                file_content_bytes = None
                try:
                    if mime_type == 'application/vnd.google-apps.document':
                        file_content_bytes = export_and_download_gdoc(service, file_id, GDRIVE_EXPORT_MIME)
                        extraction_mime_type = GDRIVE_EXPORT_MIME
                    else:
                        file_content_bytes = download_file(service, file_id)
                        extraction_mime_type = mime_type
                    if file_content_bytes:
                        text_content = extract_text_from_file(extraction_mime_type, file_content_bytes, file_name)
                        if text_content:
                            logger.info(f"   -> Texto extraído de {item_path_log}.")
                            file_data_for_chunking = {"content": text_content, "metadata": create_metadata(item, folder_path_log)}
                        else: logger.warning(f"   -> Falha extrair texto de {item_path_log}. Pulando."); continue
                    else: logger.warning(f"   -> Falha download/export {item_path_log}. Pulando."); continue
                except Exception as doc_proc_err:
                    logger.error(f"   -> Erro processando documento {item_path_log}: {doc_proc_err}", exc_info=True)
                    overall_success = False
            elif mime_type in VIDEO_MIME_TYPES:
                file_size_mb = int(item.get('size', 0)) / (1024 * 1024)
                logger.info(f"  VÍDEO MODIFICADO/NOVO: {item_path_log} ({file_size_mb:.2f} MB)")
                if not can_download:
                     logger.warning(f"   -> Sem permissão download vídeo {item_path_log}. Pulando."); continue
                if dry_run: continue
                downloaded_video_path = None
                try:
                    logger.debug(f"   -> Baixando vídeo (ID: {file_id})...")
                    file_content_bytes = download_file(service, file_id)
                    if file_content_bytes:
                        temp_dir_base = tempfile.gettempdir()
                        run_temp_dir = os.path.join(temp_dir_base, f"gdrive_videos_{uuid.uuid4().hex[:8]}")
                        os.makedirs(run_temp_dir, exist_ok=True)
                        temp_video_suffix = os.path.splitext(file_name)[1] or '.mp4'
                        safe_filename = re.sub(r'[^a-zA-Z0-9_.-]', '_', file_name)
                        temp_video_path = os.path.join(run_temp_dir, f"{uuid.uuid4().hex}{temp_video_suffix}")
                        logger.debug(f"   -> Salvando vídeo {safe_filename} em {temp_video_path}")
                        with open(temp_video_path, 'wb') as temp_video_file: temp_video_file.write(file_content_bytes)
                        downloaded_video_path = temp_video_path
                        logger.info(f"   -> Vídeo {safe_filename} baixado para {downloaded_video_path}")
                        logger.info(f"   -> Iniciando transcrição para {downloaded_video_path}...")
                        # Importação movida para dentro para evitar import circular ou dependência no nível do módulo se video_transcription não for sempre necessário
                        from ingestion.video_transcription import process_video 
                        transcription_result = process_video(downloaded_video_path)
                        if transcription_result and transcription_result.get("text"):
                            logger.info(f"   -> Transcrição de {safe_filename} concluída.")
                            file_data_for_chunking = {"content": transcription_result.get("text"), "metadata": create_metadata(item, folder_path_log)}
                        else: logger.warning(f"   -> Falha/transcrição vazia {safe_filename}. Pulando."); continue
                    else: logger.warning(f"   -> Falha download vídeo {item_path_log}. Pulando."); continue
                except OSError as os_err:
                     if os_err.errno == 28: logger.error(f"   -> ERRO ESPAÇO EM DISCO vídeo {item_path_log} em {downloaded_video_path}: {os_err}", exc_info=False)
                     else: logger.error(f"   -> Erro OS vídeo {item_path_log}: {os_err}", exc_info=True)
                     overall_success = False
                except Exception as video_proc_err:
                     logger.error(f"   -> Erro processando vídeo {item_path_log}: {video_proc_err}", exc_info=True)
                     overall_success = False
                finally:
                     if downloaded_video_path and os.path.exists(downloaded_video_path):
                          try:
                              os.remove(downloaded_video_path)
                              logger.debug(f"   -> Vídeo temp removido: {downloaded_video_path}")
                          except OSError as e: logger.error(f"   -> Erro remover vídeo temp {downloaded_video_path}: {e}")
            # Removido processamento de imagem para simplificar e focar no erro principal.
            # elif is_supported_image(item):
            #     logger.info(f"  IMAGEM SUPORTADA MODIFICADA/NOVA: {item_path_log} (Pulando)")
            #     continue
            else:
                logger.info(f"  -> Ignorando tipo não suportado modificado/novo: {item_path_log} (Tipo: {mime_type})")
                continue
            if file_data_for_chunking and supabase_client:
                content_to_chunk = file_data_for_chunking.get("content")
                metadata_for_chunks = file_data_for_chunking.get("metadata", {})
                source_name_log = metadata_for_chunks.get("source_name", file_id)
                doc_uuid = str(uuid.uuid4())
                metadata_for_chunks['document_id'] = doc_uuid
                if content_to_chunk:
                    logger.info(f"  Chunking para {source_name_log} (Doc ID: {doc_uuid})...")
                    chunks = split_content_into_chunks(
                        content_to_chunk, metadata_for_chunks,
                        max_chunk_tokens=int(os.getenv("MAX_CHUNK_TOKENS", "2048")),
                        min_chunk_chars=int(os.getenv("MIN_CHUNK_CHARS", "300")),
                        model_name=os.getenv("OPENAI_MODEL", "gpt-4o")
                    )
                    if chunks:
                        logger.info(f"  Salvando {len(chunks)} chunks para {source_name_log} (Doc ID: {doc_uuid}) Supabase...")
                        save_success = _insert_initial_chunks_supabase(supabase_client, chunks, source_name_log)
                        if save_success:
                            logger.info(f"  Chunks para {source_name_log} (Doc ID: {doc_uuid}) salvos. Marcando arquivo.")
                            try:
                                @default_retry
                                def mark_file_db_processed(cli, f_id):
                                    return cli.table('processed_files').insert({"file_id": f_id}).execute()
                                mark_response = mark_file_db_processed(supabase_client, file_id)
                                logger.info(f"  -> Arquivo {source_name_log} (ID: {file_id}) marcado em 'processed_files'.")
                            except PostgrestAPIError as mark_api_err:
                                if 'duplicate key value violates unique constraint \"processed_files_pkey\"' in str(mark_api_err.message):
                                    logger.warning(f"  -> Arquivo {source_name_log} (ID: {file_id}) já marcado (concorrência?).")
                                else:
                                    logger.error(f"  -> Falha marcar {source_name_log} (Erro API Supabase): {mark_api_err.message}", exc_info=False)
                                    overall_success = False
                            except Exception as mark_err:
                                logger.error(f"  -> Erro inesperado marcar {source_name_log}: {mark_err}", exc_info=True)
                                overall_success = False
                        else:
                            logger.error(f"  Falha salvar chunks para {source_name_log} (Doc ID: {doc_uuid}). NÃO marcado.")
                            overall_success = False
                    else: logger.warning(f"  Nenhum chunk gerado para {source_name_log} (Doc ID: {doc_uuid}).")
                else: logger.warning(f"  Conteúdo vazio para {source_name_log} antes do chunking.")
            elif not supabase_client:
                logger.warning("  Supabase client não configurado. Pulando save/marcação.")
                overall_success = False
            # Removido bloco DEBUG para garantir que o processamento ocorra.
            # logger.info(f"[DEBUG] Pulando processamento pesado para {item_path_log} (ID: {file_id}).")
            # if supabase_client and not dry_run:
            #     mark_file_db_processed(supabase_client, file_id) # Marcar como processado
            # continue

    should_mark_folder = overall_success and (folder_modified_since_last_check or last_processed_at is None)
    if should_mark_folder and not dry_run and supabase_client:
        logger.info(f"[{folder_path_log}] Tentando marcar/atualizar pasta no cache (ID: {folder_id}).")
        mark_success = mark_folder_processed(supabase_client, folder_id, folder_name)
        if mark_success:
            processed_folders.add(folder_id)
            logger.info(f"[{folder_path_log}] Pasta marcada/atualizada com sucesso no cache.")
        else:
            logger.error(f"[{folder_path_log}] FALHA CRÍTICA marcar/atualizar pasta {folder_id} no cache.")
            overall_success = False
    elif not overall_success:
         logger.warning(f"[{folder_path_log}] Pulando marcação/atualização da pasta devido a erros críticos.")
    logger.info(f"[{folder_path_log}] Ingestão da pasta concluída. Status: {'Sucesso' if overall_success else 'Falha'}")
    return overall_success

def create_metadata(item: Dict[str, Any], current_path: str) -> Dict[str, Any]:
    file_id = item.get("id")
    file_name = item.get("name")
    source_url = f"gdrive://{os.path.join(current_path, file_name)}" if current_path else f"gdrive://{file_name}"
    return {
        "source_name": file_name, "source_url": source_url, "gdrive_id": file_id,
        "mime_type": item.get("mimeType"), "gdrive_parent_id": item.get("parents", [None])[0],
        "created_time": item.get("createdTime"), "modified_time": item.get("modifiedTime"),
        "size_bytes": item.get("size"), "gdrive_webview_link": item.get("webViewLink"),
        "origin": "gdrive"
    }

def ingest_all_gdrive_content(dry_run=False):
    try:
        env_vars_json = json.dumps(dict(os.environ), indent=2, sort_keys=True)
        logger.debug(f"[DEBUG env vars] os.environ antes de get('GDRIVE_ROOT_FOLDER_IDS'):\n{env_vars_json}")
    except Exception as e: logger.error(f"[DEBUG env vars] Erro serializar os.environ: {e}")
    root_folder_ids_str = os.environ.get('GDRIVE_ROOT_FOLDER_IDS')
    if not root_folder_ids_str:
        logger.critical("Variável GDRIVE_ROOT_FOLDER_IDS não definida.")
        return None
    root_folder_ids = [folder_id.strip() for folder_id in root_folder_ids_str.split(',') if folder_id.strip()]
    logger.info(f"Pastas raiz a processar: {root_folder_ids}")
    service = authenticate_gdrive()
    if not service:
        logger.critical("Falha autenticar GDrive. Abortando ingestão.")
        return None
    processed_folders.clear()
    logger.info("Cache de pastas em memória limpo.")
    for folder_id in root_folder_ids:
        try:
            folder_metadata = service.files().get(fileId=folder_id, fields='id, name, capabilities').execute()
            folder_name = folder_metadata.get('name', folder_id)
            logger.info(f"Iniciando processamento pasta raiz: '{folder_name}' (ID: {folder_id})")
            ingest_successful = ingest_gdrive_folder(
                service=service, folder_name=folder_name, folder_id=folder_id,
                dry_run=dry_run, access_level=None, current_path=""
            )
            if not ingest_successful:
                logger.warning(f"Falhas ao processar itens na pasta raiz '{folder_name}' (ID: {folder_id}).")
        except HttpError as error: logger.error(f"Erro HTTP processar pasta raiz {folder_id}: {error}", exc_info=True)
        except Exception as e: logger.error(f"Erro inesperado processar pasta raiz {folder_id}: {e}", exc_info=True)
    logger.info("Processamento de todas as pastas raiz concluído.")
    return None

def main():
    parser = argparse.ArgumentParser(description="Ingestão de conteúdo do Google Drive.")
    parser.add_argument("--dry-run", action="store_true", help="Executa em modo dry-run.")
    # Novos argumentos para controlar o pipeline de anotação
    parser.add_argument("--skip-annotation", action="store_true", help="Pula a etapa de anotação.")
    parser.add_argument("--skip-indexing", action="store_true", help="Pula a etapa de indexação.")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("ANNOTATION_BATCH_SIZE", "50")), help="Tamanho do lote para anotação/indexação.")
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("ANNOTATION_MAX_WORKERS", "4")), help="Número máximo de workers para anotação/indexação.")
    
    args = parser.parse_args()

    logger.info("Iniciando gdrive_ingest.py...")
    if args.dry_run:
        logger.info("*** EXECUTANDO EM MODO DRY-RUN ***")

    ingest_all_gdrive_content(dry_run=args.dry_run)

    logger.info("Iniciando pipeline de anotação e indexação...")
    try:
        run_annotation_pipeline(
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            skip_annotation=args.skip_annotation,
            skip_indexing=args.skip_indexing
        )
        logger.info("Pipeline de anotação e indexação concluído.")
    except Exception as e:
        logger.error(f"Erro ao executar o pipeline de anotação e indexação: {e}", exc_info=True)

    logger.info("Processamento principal concluído. Mantendo o worker ativo...")
    while True:
        time.sleep(300)

if __name__ == "__main__":
    main()
