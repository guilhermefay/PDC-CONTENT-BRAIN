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
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from docling.document_converter import DocumentConverter
import tempfile
from typing import Any, List, Dict, Optional
from ingestion.image_processor import ImageProcessor
from supabase import create_client, Client, PostgrestAPIResponse
from postgrest.exceptions import APIError as PostgrestAPIError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import base64

# Configurar logger
logger = logging.getLogger(__name__)
# Certificar que o logging básico seja configurado apenas uma vez
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

# Carregar variáveis de ambiente do .env se existir
load_dotenv()

# Inicializar Cliente Supabase
supabase_url: str = os.environ.get("SUPABASE_URL")
supabase_key: str = os.environ.get("SUPABASE_SERVICE_KEY")
supabase_client: Optional[Client] = None # Inicializar como None
try:
    if supabase_url and supabase_key:
        supabase_client = create_client(supabase_url, supabase_key)
        logger.info("Cliente Supabase inicializado com sucesso em gdrive_ingest.")
    else:
        logger.warning("URL ou Chave Supabase não encontradas nas variáveis de ambiente. Verificação de arquivos processados desabilitada.")
except Exception as e:
    logger.error(f"Erro ao inicializar cliente Supabase em gdrive_ingest: {e}", exc_info=True)
    supabase_client = None # Garantir que seja None em caso de erro

# Constantes
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
GDRIVE_EXPORT_MIME = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' # Exportar GDoc como DOCX
SCOPES = ['https://www.googleapis.com/auth/drive.readonly'] # Definir escopos aqui

# --- Constantes de Filtragem ---
IGNORED_FILENAMES = {
    '.gitignore', '.env', 'requirements.txt', 'Pipfile', 'Pipfile.lock',
    'poetry.lock', 'package.json', 'package-lock.json', 'README.md',
    'LICENSE', 'docker-compose.yml', 'Dockerfile', '.dockerignore',
    '.qa_piplist.txt', 'test.txt', '.DS_Store',
    # Adicionar outros nomes exatos se necessário
}
IGNORED_EXTENSIONS = {
    '.py', '.js', '.ts', '.sql', '.sh', '.ipynb', '.json', '.yml',
    '.yaml', '.log', '.csv', '.tsv', '.xml', '.zip', '.gz', '.tar',
    '.exe', '.dll', '.so', '.class', '.jar', '.md', # Ignorar Markdown por enquanto
    # Adicionar extensões de imagem comuns
    '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.webp',
    '.heic', # Adicionar HEIC
    # Adicionar outras extensões se necessário
}
# --- Fim Constantes de Filtragem ---

OUTPUT_DIR = '/storage/ingest_output'

try:
    docling_converter = DocumentConverter()
    logging.info("Docling DocumentConverter inicializado com sucesso.")
except Exception as e:
    logging.error(f"Falha ao inicializar Docling DocumentConverter: {e}", exc_info=True)
    docling_converter = None

# Instancia global do ImageProcessor
image_processor = ImageProcessor()

# Contador global para limite de teste
# files_processed_so_far = 0 # <-- COMENTADO

# ### DEBUG - LIMITAR PARA TESTE ###
# Limite global temporário para o número de arquivos a processar no total (todas as pastas)
# Remover ou comentar esta linha para processamento completo
# MAX_FILES_TO_PROCESS_TEST = 5 # <-- COMENTADO
# ### FIM DEBUG ###

# --- INÍCIO: Código copiado/adaptado de etl/annotate_and_index.py ---

# Inicializar tiktoken (usar encoding para modelos OpenAI mais recentes)
try:
    tokenizer = tiktoken.get_encoding("cl100k_base")
except Exception as e:
    logger.warning(f"Falha ao carregar tokenizer tiktoken 'cl100k_base', usando 'p50k_base' como fallback: {e}")
    try:
        tokenizer = tiktoken.get_encoding("p50k_base")
    except Exception as e2:
         logger.error(f"Falha ao carregar qualquer tokenizer tiktoken: {e2}. Contagem de tokens não funcionará.")
         tokenizer = None

# --- Configuração de Retentativas Tenacity ---
RETRYABLE_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    PostgrestAPIError, # Erros específicos do Postgrest
    # Adicionar outros erros de API específicos do GDrive ou Supabase se necessário
)
default_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS)
)
# --- Fim Configuração Tenacity ---

def count_tokens(text: str) -> int:
    """Conta tokens usando o tokenizer tiktoken inicializado."""
    if not tokenizer:
        logger.warning("Tokenizer tiktoken não disponível, retornando contagem de caracteres como fallback.")
        return len(text)
    return len(tokenizer.encode(text))

def split_content_into_chunks(text: str, initial_metadata: Dict[str, Any], max_chunk_tokens: int = 800) -> List[Dict[str, Any]]:
    """Divide o texto em chunks menores baseados na contagem de tokens.

    Tenta manter parágrafos e sentenças intactos, mas pode quebrar sentenças
    longas se necessário. Adiciona metadados a cada chunk, incluindo
    o `chunk_index` e `document_id`.

    Args:
        text (str): O texto completo a ser dividido.
        initial_metadata (Dict[str, Any]): Metadados originais do documento fonte,
                                           que serão copiados para cada chunk.
        max_chunk_tokens (int): O número máximo aproximado de tokens por chunk.

    Returns:
        List[Dict[str, Any]]: Uma lista de dicionários, onde cada um representa
                               um chunk com `content` e `metadata` atualizado.
    """
    if not tokenizer:
         logger.error("Tokenizer não disponível, não é possível fazer chunking por tokens.")
         return []
    if not text or not isinstance(text, str):
        logger.warning(f"Conteúdo inválido ou vazio recebido para chunking com metadados: {initial_metadata.get('source_name', 'Desconhecido')}")
        return []

    chunks_data = []
    # Usar parágrafos como delimitadores iniciais
    paragraphs = [p for p in text.split('\n\n') if p.strip()]
    current_chunk_content = []
    current_chunk_tokens = 0
    chunk_index = 0

    for paragraph in paragraphs:
        paragraph_tokens = count_tokens(paragraph)

        # Se um único parágrafo exceder o limite, divida-o ainda mais (por sentença, etc.) - Simplificado por agora
        if paragraph_tokens > max_chunk_tokens:
            logger.warning(f"Parágrafo muito longo ({paragraph_tokens} tokens) em {initial_metadata.get('source_name', 'Desconhecido')}, adicionando como um chunk único. Considere pré-processamento.")
            # Adiciona o parágrafo grande como um chunk separado, mesmo que exceda
            if current_chunk_content: # Salva o chunk anterior se houver
                doc_id = str(uuid.uuid4())
                metadata_copy = initial_metadata.copy()
                metadata_copy.update({"chunk_index": chunk_index, "document_id": doc_id})
                chunks_data.append({"content": "\n\n".join(current_chunk_content), "metadata": metadata_copy})
                chunk_index += 1
                current_chunk_content = []
                current_chunk_tokens = 0

            # Salva o parágrafo grande
            doc_id_large = str(uuid.uuid4())
            metadata_copy_large = initial_metadata.copy()
            metadata_copy_large.update({"chunk_index": chunk_index, "document_id": doc_id_large})
            chunks_data.append({"content": paragraph, "metadata": metadata_copy_large})
            chunk_index += 1
            continue # Pula para o próximo parágrafo

        # Se adicionar o parágrafo atual exceder o limite, salve o chunk atual e comece um novo
        if current_chunk_tokens + paragraph_tokens > max_chunk_tokens and current_chunk_content:
            doc_id = str(uuid.uuid4())
            metadata_copy = initial_metadata.copy()
            metadata_copy.update({"chunk_index": chunk_index, "document_id": doc_id})
            chunks_data.append({"content": "\n\n".join(current_chunk_content), "metadata": metadata_copy})
            chunk_index += 1
            current_chunk_content = [paragraph] # Começa novo chunk com o parágrafo atual
            current_chunk_tokens = paragraph_tokens
        else:
            # Adiciona o parágrafo ao chunk atual
            current_chunk_content.append(paragraph)
            current_chunk_tokens += paragraph_tokens

    # Adiciona o último chunk se houver conteúdo restante
    if current_chunk_content:
        doc_id = str(uuid.uuid4())
        metadata_copy = initial_metadata.copy()
        metadata_copy.update({"chunk_index": chunk_index, "document_id": doc_id})
        chunks_data.append({"content": "\n\n".join(current_chunk_content), "metadata": metadata_copy})

    logger.info(f"Dividido conteúdo de {initial_metadata.get('source_name', 'Desconhecido')} em {len(chunks_data)} chunks.")
    return chunks_data

@default_retry
def _insert_initial_chunks_supabase(supabase_cli: Client, batch: List[Dict[str, Any]], source_name: str) -> bool:
    """Insere um lote inicial de chunks na tabela 'documents' do Supabase."""
    if not supabase_cli or not batch:
        logger.warning(f"Supabase client não disponível ou lote vazio para {source_name}, pulando inserção.")
        return False

    records_to_insert = []
    for chunk_data in batch:
        # Garantir que metadata existe e é um dicionário
        metadata = chunk_data.get('metadata', {})
        if not isinstance(metadata, dict):
            logger.warning(f"Metadados inválidos encontrados para chunk em {source_name}, pulando chunk: {chunk_data}")
            continue

        records_to_insert.append({
            'document_id': metadata.get('document_id'), # Já gerado em split_content_into_chunks
            'content': chunk_data.get('content'),
            'metadata': metadata, # Inclui source_name, gdrive_id, chunk_index, etc.
            'annotation_status': 'pending', # Status inicial
            'indexing_status': 'pending'    # Status inicial
        })

    if not records_to_insert:
         logger.warning(f"Nenhum registro válido para inserir para {source_name} após validação.")
         return False

    try:
        logger.info(f"Inserindo {len(records_to_insert)} chunks iniciais no Supabase para {source_name}...")
        response: PostgrestAPIResponse = supabase_cli.table('documents').insert(records_to_insert).execute()

        # Verificar se houve erro na resposta da API (mesmo com status 2xx)
        # A biblioteca supabase-py pode retornar sucesso mesmo que alguns registros falhem
        # por constraints, etc. Uma verificação mais robusta seria ideal aqui, mas
        # por ora confiamos na ausência de exceção e no status code geral.
        # A resposta `response.data` contém os dados inseridos se a operação foi bem-sucedida.
        if hasattr(response, 'data') and response.data:
             logger.info(f"Sucesso: {len(response.data)} chunks inseridos para {source_name}.")
             return True
        else:
             # Tentar logar um erro mais específico se disponível
             error_details = getattr(response, 'error', None) or getattr(response, 'message', 'Detalhes indisponíveis')
             logger.error(f"Falha ao inserir chunks para {source_name}. Resposta sem dados ou indicando erro: {error_details}")
             return False

    except PostgrestAPIError as api_error:
         logger.error(f"Erro da API Postgrest ao inserir chunks para {source_name}: {api_error}", exc_info=True)
         # Tentar logar detalhes da resposta se disponíveis
         try:
             logger.error(f"Detalhes do erro Postgrest: Code={api_error.code}, Details={api_error.details}, Hint={api_error.hint}, Message={api_error.message}")
         except Exception:
             pass
         return False
    except Exception as e:
        logger.error(f"Erro inesperado ao inserir chunks no Supabase para {source_name}: {e}", exc_info=True)
        return False

# --- FIM: Código copiado/adaptado ---

def authenticate_gdrive():
    """Autentica na API do Google Drive usando credenciais de Service Account.

    Prioriza o uso do conteúdo JSON da variável de ambiente
    `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64`. Como fallback, tenta usar
    o caminho do arquivo especificado em `GOOGLE_SERVICE_ACCOUNT_JSON`.

    Returns:
        googleapiclient.discovery.Resource: Um objeto de serviço autorizado da API
                                            do Google Drive (v3).

    Raises:
        ValueError: Se nenhuma credencial válida for encontrada.
        googleapiclient.errors.HttpError: Se ocorrer um erro durante a autenticação.
        Exception: Para outros erros inesperados.
    """
    creds = None
    # --- INÍCIO DA MODIFICAÇÃO ---
    creds_content_b64 = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64')
    
    if creds_content_b64:
        try:
            logger.info("Tentando autenticar usando GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64...")
            creds_content_json = base64.b64decode(creds_content_b64).decode('utf-8')
            credentials_dict = json.loads(creds_content_json)
            creds = service_account.Credentials.from_service_account_info(info=credentials_dict, scopes=SCOPES)
            logger.info("Autenticação via conteúdo JSON (Base64) bem-sucedida.")
        except Exception as e:
            logger.warning(f"Falha ao usar GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64: {e}. Tentando fallback via caminho...")
            creds = None # Garante que tentaremos o fallback

    # Fallback: Tentar usar o caminho do arquivo se o conteúdo falhar ou não existir
    if not creds:
        creds_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
        if creds_path and os.path.exists(creds_path):
            try:
                logger.info(f"Tentando autenticar usando GOOGLE_SERVICE_ACCOUNT_JSON (caminho: {creds_path})...")
                creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
                logger.info("Autenticação via caminho do arquivo JSON bem-sucedida.")
            except Exception as e:
                logger.error(f"Falha ao autenticar usando o caminho {creds_path}: {e}")
                creds = None # Marca como falha
        else:
            logger.warning("Variável GOOGLE_SERVICE_ACCOUNT_JSON (caminho) não definida ou inválida.")
    
    # Se nenhuma credencial foi carregada com sucesso
    if not creds:
        error_msg = "Falha ao autenticar com Google Drive. Nenhuma credencial válida encontrada (via conteúdo Base64 ou caminho)."
        logger.error(error_msg)
        raise ValueError(error_msg)
    # --- FIM DA MODIFICAÇÃO ---
        
    try:
        # Construir o serviço usando as credenciais obtidas (creds)
        service = build('drive', 'v3', credentials=creds)
        logger.info("Serviço Google Drive API construído com sucesso.")
        return service
    except HttpError as auth_error:
        logger.error(f"Erro HTTP durante a construção do serviço Google Drive: {auth_error}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado durante a construção do serviço Google Drive: {e}", exc_info=True)
        raise

def export_and_download_gdoc(service, file_id, export_mime_type):
    """Exporta um Google Doc para um MIME type específico e baixa o conteúdo.

    Usado principalmente para converter Google Docs nativos para um formato
    processável (como DOCX) antes da extração de texto.

    Args:
        service: Objeto de serviço autorizado da API do Google Drive.
        file_id (str): O ID do Google Doc a ser exportado.
        export_mime_type (str): O MIME type para o qual exportar
                                (ex: `application/vnd.openxmlformats-officedocument.wordprocessingml.document`).

    Returns:
        Optional[bytes]: O conteúdo binário do arquivo exportado, ou None se ocorrer um erro.
    """
    logger.debug(f"  -> Exportando Google Doc (ID: {file_id}) para {export_mime_type}...")
    try:
        request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        logger.debug(f"  -> Exportação concluída para {file_id}.")
        return fh.getvalue()
    except HttpError as error:
        logger.error(f"   -> Erro HTTP {error.resp.status} durante a exportação do Google Doc {file_id}: {error}")
        try:
            error_content = error.content.decode('utf-8')
            logger.error(f"   -> Conteúdo da resposta de erro (export): {error_content}")
        except Exception:
            pass
        return None
    except Exception as e:
        logger.error(f"   -> Erro inesperado durante a exportação do Google Doc {file_id}: {e}", exc_info=True)
        return None

def extract_text_from_file(mime_type: str, file_content_bytes: bytes, file_name: str) -> Optional[str]:
    """Extrai texto do conteúdo binário de um arquivo baixado.

    Utiliza decodificação direta para `text/plain` (com fallback para latin-1)
    e `Docling DocumentConverter` para DOCX e PDF.
    Normaliza o texto extraído.

    Args:
        mime_type (str): O MIME type do arquivo original.
        file_content_bytes (bytes): O conteúdo binário do arquivo.
        file_name (str): O nome original do arquivo (usado para logging).

    Returns:
        Optional[str]: O texto extraído e normalizado, ou None se a extração falhar
                       ou o tipo de arquivo não for suportado.
    """
    logger.debug(f"   -> Iniciando extração de texto para {file_name} ({mime_type})")
    text_content = None
    if not file_content_bytes:
        logger.warning(f"   -> Conteúdo binário vazio recebido para {file_name}")
        return None

    try:
        # Caso 1: Tratar texto plano diretamente
        if mime_type == 'text/plain':
            try:
                text_content = file_content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                 logger.warning(f"    -> Erro ao decodificar TXT {file_name} como UTF-8, tentando latin-1.")
                 try:
                     text_content = file_content_bytes.decode('latin-1')
                 except Exception as decode_err:
                     logger.error(f"    -> Falha ao decodificar TXT {file_name} com latin-1: {decode_err}")
                     text_content = None # Marcar como falha
            except Exception as txt_err:
                logger.error(f"    -> Erro ao processar TXT {file_name}: {txt_err}")
                text_content = None # Marcar como falha

        # Caso 2: Usar Docling DocumentConverter para outros tipos suportados (PDF, DOCX exportado)
        elif docling_converter and (mime_type == GDRIVE_EXPORT_MIME or mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' or mime_type == 'application/pdf'):
            temp_file_path = None # Inicializar para garantir que existe no bloco finally
            try:
                # Salvar bytes em arquivo temporário
                with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{os.path.basename(file_name)}") as temp_input_file:
                    temp_input_file.write(file_content_bytes)
                    temp_file_path = temp_input_file.name

                logger.debug(f"    -> Usando Docling DocumentConverter para {temp_file_path} (origin: {file_name})")
                # Passar o caminho do arquivo temporário para o conversor
                conversion_result = docling_converter.convert(temp_file_path)
                # Extrair o texto do resultado
                text_content = conversion_result.document.export_to_markdown() # Ajustar se necessário
                logger.debug(f"    -> Docling extraiu texto de {file_name}.")
            except Exception as docling_err:
                 logger.error(f"    -> Erro do Docling DocumentConverter ao processar {file_name}: {docling_err}", exc_info=True)
                 text_content = None
            finally:
                 # Remover o arquivo temporário
                 if temp_file_path and os.path.exists(temp_file_path):
                     try:
                         os.remove(temp_file_path)
                     except Exception as rem_err:
                          logger.warning(f"     -> Falha ao remover arquivo temporário {temp_file_path}: {rem_err}")

        elif not docling_converter and mime_type != 'text/plain':
             logger.error(f"   -> Docling DocumentConverter não inicializado. Não é possível processar {file_name} ({mime_type}).")
             text_content = None

        else:
            logger.warning(f"   -> Tipo MIME não suportado ou Docling não aplicável: {mime_type} para {file_name}")
            text_content = None

        # Normalização e limpeza final do texto extraído
        if text_content:
            text_content = unicodedata.normalize('NFKC', text_content).strip()
            if text_content: # Verificar se não ficou vazio após strip
                 logger.debug(f"   -> Extração/Decodificação de texto bem-sucedida para {file_name}.")
                 return text_content
            else:
                 logger.warning(f"   -> Texto extraído/decodificado resultou em vazio para {file_name}.")
                 return None
        else:
            logger.warning(f"   -> Falha na extração/decodificação de texto para {file_name}.")
            return None

    except Exception as e:
        logger.error(f"   -> Erro inesperado durante a extração de texto para {file_name} ({mime_type}). Arquivo pode estar corrompido ou formato não suportado corretamente. Erro: {e}", exc_info=True)
        return None

def download_file(service, file_id):
    """Baixa o conteúdo binário de um arquivo regular (não Google Doc) do Drive.

    Args:
        service: Objeto de serviço autorizado da API do Google Drive.
        file_id (str): O ID do arquivo a ser baixado.

    Returns:
        Optional[bytes]: O conteúdo binário do arquivo, ou None se ocorrer um erro.
    """
    logger.debug(f"  -> Baixando arquivo (ID: {file_id})...")
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        logger.debug(f"  -> Download concluído para {file_id}.")
        return fh.getvalue()
    except HttpError as error:
        logger.error(f"  -> Erro HTTP {error.resp.status} durante o download do arquivo {file_id}: {error}")
        try:
            error_content = error.content.decode('utf-8')
            logger.error(f"   -> Conteúdo da resposta de erro (download): {error_content}")
        except Exception:
            pass
        return None
    except Exception as e:
        logger.error(f"  -> Erro inesperado durante o download do arquivo {file_id}: {e}", exc_info=True)
        return None

# ADICIONAL: Função para identificar arquivos de imagem suportados
def is_supported_image(file: Dict[str, Any]) -> bool:
    mime_type = file.get('mimeType', '').lower()
    name = file.get('name', '').lower()
    # Identificar imagens por mimeType
    if mime_type in ['image/jpeg', 'image/png', 'image/heic']:
        return True
    # Permitir arquivos com extensão de imagem se não forem vídeos
    if name.endswith(('.jpeg', '.jpg', '.png', '.heic')) and not mime_type.startswith('video'):
        return True
    return False

def ingest_gdrive_folder(
    service,
    folder_name: str,
    folder_id: str,
    dry_run: bool = False,
    access_level: Optional[str] = None,
    current_path: str = ""
) -> bool:
    """
    Ingere recursivamente arquivos e pastas de uma pasta do Google Drive.

    Modificado para processamento incremental e sem temp_dir_path explícito.
    """
    # results = [] # Removido
    overall_success = True # Flag para rastrear sucesso

    # Construir caminho para logging
    folder_path_log = os.path.join(current_path, folder_name)

    logger.info(f"\nIniciando ingestão da pasta: {folder_path_log} (ID: {folder_id}, Access: {access_level})")

    page_token = None
    all_files_in_folder = []

    # 1. Paginação: Coletar todos os itens da pasta primeiro
    while True:
        try:
            response = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, size, parents, capabilities, webViewLink)',
                pageToken=page_token
            ).execute()

            files_in_current_page = response.get('files', [])
            logger.info(f"  Encontrados {len(files_in_current_page)} itens nesta página para {folder_path_log}...")
            all_files_in_folder.extend(files_in_current_page)

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                logger.debug(f"  Fim da paginação para {folder_path_log}. Total de itens: {len(all_files_in_folder)}")
                break # Sai do loop de paginação
        except HttpError as error:
            logger.error(f"Erro HTTP ao listar arquivos na pasta {folder_id} ({folder_path_log}): {error}")
            return False # Falha crítica na listagem
        except Exception as e:
            logger.error(f"Erro inesperado ao listar arquivos na pasta {folder_id} ({folder_path_log}): {e}", exc_info=True)
            return False # Falha crítica na listagem

    # 2. Iteração e Processamento Incremental
    for item in all_files_in_folder:
        file_id = item.get('id')
        file_name = item.get('name', 'NomeDesconhecido')
        mime_type = item.get('mimeType')
        capabilities = item.get('capabilities', {})
        can_download = capabilities.get('canDownload', False)
        item_path_log = os.path.join(folder_path_log, file_name) # Para logs

        # --- Lógica de Verificação de Arquivo Processado --- START ---
        if supabase_client:
            try:
                # Adicionar retry aqui também
                @default_retry
                def check_processed(cli, f_id):
                    return cli.table('processed_files')\
                              .select('file_id', count='exact')\
                              .eq('file_id', f_id)\
                              .execute()

                supabase_response = check_processed(supabase_client, file_id)

                if supabase_response.count > 0:
                    logger.info(f"  -> Arquivo '{item_path_log}' (ID: {file_id}) já existe em 'processed_files'. Pulando.")
                    continue # Pula para o próximo item
                else:
                    logger.debug(f"  [Check Supabase] Arquivo '{item_path_log}' (ID: {file_id}) não encontrado em 'processed_files'. Prosseguindo.")
            except PostgrestAPIError as api_error:
                # Logar erro mas continuar, assumindo que o arquivo não foi processado
                logger.error(f"  [Check Supabase] Erro API ao verificar {item_path_log} (ID: {file_id}): {api_error.message}. Assumindo não processado.", exc_info=True)
            except Exception as check_err:
                # Logar erro mas continuar
                 logger.error(f"  [Check Supabase] Erro inesperado ao verificar {item_path_log} (ID: {file_id}): {check_err}. Assumindo não processado.", exc_info=True)
        else:
             logger.debug("  Supabase client não disponível, pulando verificação de arquivos processados.")
        # --- Lógica de Verificação de Arquivo Processado --- END ---

        # --- Lógica de Filtragem --- START ---
        file_ext = os.path.splitext(file_name)[1].lower()
        is_hidden = file_name.startswith('.')
        normalized_file_name = file_name.lower()

        if normalized_file_name in IGNORED_FILENAMES or \
           file_ext in IGNORED_EXTENSIONS or \
           is_hidden:
            reason = (f"(Nome: {normalized_file_name in IGNORED_FILENAMES}, "
                      f"Ext: {file_ext in IGNORED_EXTENSIONS}, "
                      f"Oculto: {is_hidden})")
            logger.info(f"  -> Ignorando arquivo irrelevante/config: {item_path_log} {reason}")
            continue # Pula para o próximo item
        # --- Lógica de Filtragem --- END ---

        # Identificar tipo e processar
        item_type = None
        text_content = None
        file_data_for_chunking = None # Dados a serem usados para chunk/save

        if mime_type == 'application/vnd.google-apps.folder':
            # --- Processar Subpasta (Recursão) ---
            logger.info(f"  Identificada SUBPASTA: {file_name}. Iniciando ingestão recursiva...")
            if not dry_run:
                # Passar o caminho atual para a chamada recursiva
                subfolder_success = ingest_gdrive_folder(service, file_name, file_id, dry_run, access_level, folder_path_log)
                if not subfolder_success:
                    overall_success = False # Propagar falha de subpasta
            continue

        elif mime_type in DOCUMENT_MIME_TYPES:
            # --- Processar Documento ---
            logger.info(f"  Identificado DOCUMENTO: {item_path_log} (ID: {file_id}, Tipo: {mime_type})")
            if not can_download and mime_type != 'application/vnd.google-apps.document':
                 logger.warning(f"   -> Sem permissão para baixar o documento {item_path_log}. Pulando.")
                 continue

            if dry_run: continue # Pular download/processamento em dry-run

            file_content_bytes = None
            try:
                if mime_type == 'application/vnd.google-apps.document':
                    file_content_bytes = export_and_download_gdoc(service, file_id, GDRIVE_EXPORT_MIME)
                    # Usar DOCX como mime_type para extração, já que foi exportado
                    extraction_mime_type = GDRIVE_EXPORT_MIME
                else:
                    file_content_bytes = download_file(service, file_id)
                    extraction_mime_type = mime_type

                if file_content_bytes:
                    text_content = extract_text_from_file(extraction_mime_type, file_content_bytes, file_name)
                    if text_content:
                        logger.info(f"   -> Texto extraído com sucesso de {item_path_log}.")
                        file_data_for_chunking = { # Preparar dados para chunk/save
                            "content": text_content,
                            "metadata": create_metadata(item)
                        }
                    else:
                        logger.warning(f"   -> Falha ao extrair texto de {item_path_log}.")
                        continue # Pular para o próximo item se não extrair texto
                else:
                    logger.warning(f"   -> Falha ao baixar/exportar {item_path_log}.")
                    continue # Pular para o próximo item se falhar download
            except Exception as doc_proc_err:
                 logger.error(f"   -> Erro inesperado ao processar documento {item_path_log}: {doc_proc_err}", exc_info=True)
                 overall_success = False # Marcar falha, mas não pular para próximo item (pode ser intermitente)

        elif mime_type in VIDEO_MIME_TYPES:
            # --- Processar Vídeo ---
            file_size_mb = int(item.get('size', 0)) / (1024 * 1024)
            logger.info(f"  Identificado VÍDEO: {item_path_log} (ID: {file_id}, Tipo: {mime_type}, Tamanho: {file_size_mb:.2f} MB)")
            if not can_download:
                 logger.warning(f"   -> Sem permissão para baixar o vídeo {item_path_log}. Pulando.")
                 continue

            if dry_run: continue # Pular download/processamento em dry-run

            downloaded_video_path = None
            try:
                # --- Download do vídeo ---
                logger.debug(f"   -> Baixando arquivo de vídeo (ID: {file_id})...")
                file_content_bytes = download_file(service, file_id)
                if file_content_bytes:
                    # Salvar vídeo temporariamente (agora usa tempfile.NamedTemporaryFile sem dir explícito)
                    temp_video_suffix = os.path.splitext(file_name)[1] or '.mp4'
                    with tempfile.NamedTemporaryFile(delete=False, suffix=temp_video_suffix) as temp_video_file:
                         temp_video_file.write(file_content_bytes)
                         downloaded_video_path = temp_video_file.name
                    logger.info(f"   -> Vídeo {file_name} baixado para {downloaded_video_path}")

                    # --- Transcrição ---
                    logger.info(f"   -> Iniciando transcrição para {downloaded_video_path}...")
                    from ingestion.video_transcription import process_video
                    transcription_result = process_video(downloaded_video_path)

                    if transcription_result and transcription_result.get("transcription"):
                        logger.info(f"   -> Transcrição concluída para {file_name}.")
                        file_data_for_chunking = {
                            "content": transcription_result.get("transcription"),
                            "metadata": create_metadata(item)
                        }
                    else:
                         logger.warning(f"   -> Falha ou transcrição vazia para {file_name}.")
                         overall_success = False # Marcar falha
                else:
                    logger.warning(f"   -> Falha ao baixar vídeo {item_path_log}.")
                    overall_success = False # Marcar falha

            except Exception as video_proc_err:
                 logger.error(f"   -> Erro inesperado ao processar vídeo {item_path_log}: {video_proc_err}", exc_info=True)
                 overall_success = False # Marcar falha
            finally:
                 # Limpar vídeo baixado (inalterado)
                 if downloaded_video_path and os.path.exists(downloaded_video_path):
                      try:
                          os.remove(downloaded_video_path)
                          logger.debug(f"   -> Arquivo de vídeo temporário removido: {downloaded_video_path}")
                      except OSError as e:
                          logger.error(f"   -> Erro ao remover arquivo de vídeo temporário {downloaded_video_path}: {e}")

        elif is_supported_image(item):
            # --- Processar Imagem (se habilitado futuramente) ---
            logger.info(f"  Identificada IMAGEM SUPORTADA: {item_path_log} (Pulando por enquanto)")
            # file_data = process_image(service, item, temp_dir_path) # Chamar process_image
            # if file_data:
            #     file_data_for_chunking = file_data # Preparar para chunk/save se OCR retornar texto
            continue # Pular processamento de imagem por agora
        else:
            logger.info(f"  -> Ignorando tipo de arquivo não suportado/config: {item_path_log} (Tipo: {mime_type})")
            continue # Pula para o próximo item

        # --- Processamento Incremental: Chunk e Save ---
        if file_data_for_chunking and supabase_client:
            content_to_chunk = file_data_for_chunking.get("content")
            metadata_for_chunks = file_data_for_chunking.get("metadata", {})
            source_name_log = metadata_for_chunks.get("source_name", file_id) # Usar nome ou ID para log

            if content_to_chunk:
                logger.info(f"  Iniciando chunking para {source_name_log}...")
                chunks = split_content_into_chunks(content_to_chunk, metadata_for_chunks)

                if chunks:
                    logger.info(f"  Iniciando salvamento de {len(chunks)} chunks para {source_name_log} no Supabase...")
                    save_success = _insert_initial_chunks_supabase(supabase_client, chunks, source_name_log)

                    if save_success:
                         logger.info(f"  Chunks para {source_name_log} salvos com sucesso. Marcando como processado.")
                         # --- Marcar como Processado ---
                         try:
                             # Adicionar retry
                             @default_retry
                             def mark_processed(cli, f_id):
                                 return cli.table('processed_files').insert({"file_id": f_id}).execute()

                             mark_response = mark_processed(supabase_client, file_id)
                             # Verificar resposta - pode variar, mas ausência de erro é bom sinal
                             if hasattr(mark_response, 'error') and mark_response.error:
                                logger.error(f"  -> Falha ao marcar {source_name_log} como processado (Erro API Supabase): {mark_response.error}")
                                overall_success = False # Marcar falhou
                             else:
                                logger.info(f"  -> Arquivo {source_name_log} (ID: {file_id}) marcado como processado.")

                         except Exception as mark_err:
                             logger.error(f"  -> Erro inesperado ao marcar {source_name_log} como processado: {mark_err}", exc_info=True)
                             overall_success = False # Marcar falhou
                    else:
                         logger.error(f"  Falha ao salvar chunks para {source_name_log}. Arquivo NÃO será marcado como processado.")
                         overall_success = False # Salvar chunks falhou
                else:
                    logger.warning(f"  Nenhum chunk gerado para {source_name_log} (conteúdo pode ser vazio ou erro no chunking).")
                    # Considerar se isso deve marcar como falha ou apenas pular
                    # overall_success = False
            else:
                logger.warning(f"  Conteúdo vazio encontrado para {source_name_log} antes do chunking.")
                # Considerar se isso deve marcar como falha
                # overall_success = False
        elif not supabase_client:
            logger.warning("  Supabase client não configurado. Pulando salvamento de chunks e marcação de processado.")
            overall_success = False # Considerar falha se Supabase é essencial
        # --- Fim Processamento Incremental ---

    logger.info(f"Ingestão da pasta {folder_path_log} concluída.")
    return overall_success

# Adicionar função auxiliar _mark_file_processed_supabase (copiada/adaptada) se não existir
# @default_retry
# def _mark_file_processed_supabase(supabase_cli: Client, file_id: str, source_name: str):
#    # ... (lógica de inserção em processed_files) ...
#    # Esta lógica foi integrada diretamente acima para clareza

def create_metadata(item: Dict[str, Any]) -> Dict[str, Any]:
    """Cria um dicionário de metadados padronizado a partir do item do GDrive."""
    return {
        "source_name": item.get("name"),
        "gdrive_id": item.get("id"),
        "mime_type": item.get("mimeType"),
        "gdrive_parent_id": item.get("parents", [None])[0], # Pega o primeiro pai
        "created_time": item.get("createdTime"),
        "modified_time": item.get("modifiedTime"),
        "size_bytes": item.get("size"),
        "gdrive_webview_link": item.get("webViewLink"), # Adicionado link web
        "origin": "gdrive" # Indica a origem
        # Adicionar outros metadados relevantes aqui
    }


# ... (função ingest_all_gdrive_content e main existentes, podem precisar de ajustes menores
#      para não esperar mais a lista de resultados de ingest_gdrive_folder) ...

def ingest_all_gdrive_content(dry_run=False):
    """
    Função principal para ingerir conteúdo de todas as pastas raiz configuradas.
    """
    # DEBUG: Logar o dicionário os.environ completo antes de acessar a variável
    try:
        env_vars_json = json.dumps(dict(os.environ), indent=2, sort_keys=True)
        logger.debug(f"[DEBUG env vars] Conteúdo de os.environ antes de get('GDRIVE_ROOT_FOLDER_IDS'):\n{env_vars_json}")
    except Exception as e:
        logger.error(f"[DEBUG env vars] Erro ao tentar serializar os.environ: {e}")

    root_folder_ids_str = os.environ.get('GDRIVE_ROOT_FOLDER_IDS')
    if not root_folder_ids_str:
        logger.critical("Variável de ambiente GDRIVE_ROOT_FOLDER_IDS não definida.")
        return None # Retorna None se não estiver definida

    root_folder_ids = [folder_id.strip() for folder_id in root_folder_ids_str.split(',') if folder_id.strip()]
    logger.info(f"Pastas raiz a serem processadas: {root_folder_ids}")

    all_ingested_items = []
    temp_dir_path = None # Inicializar como None fora do loop

    # <<< INICIALIZAR O SERVICE AQUI >>>
    service = authenticate_gdrive()
    if not service:
        logger.critical("Falha ao autenticar na API do Google Drive. Abortando ingestão.")
        return None # Retornar None se a autenticação falhar

    for folder_id in root_folder_ids:
        try:
            # Obter nome da pasta raiz para logs
            folder_metadata = service.files().get(fileId=folder_id, fields='id, name, capabilities').execute()
            folder_name = folder_metadata.get('name', folder_id)
            logger.info(f"Iniciando processamento da pasta raiz: '{folder_name}' (ID: {folder_id})")

            # Iniciar ingestão recursiva (passando o service)
            # A função ingest_gdrive_folder agora gerencia seu próprio diretório temp
            ingest_successful = ingest_gdrive_folder(
                service=service, # Passar o service autenticado
                folder_name=folder_name,
                folder_id=folder_id,
                dry_run=dry_run,
                access_level=None # Determinar acesso se necessário
            )
            # A função ingest_gdrive_folder agora retorna bool
            if not ingest_successful:
                 logger.warning(f"Houve falhas ao processar/salvar itens na pasta '{folder_name}' (ID: {folder_id}). Verifique logs anteriores.")
            # A lógica de agregação foi movida para dentro de ingest_gdrive_folder
            # if ingested_for_folder:
            #     all_ingested_items.extend(ingested_for_folder)

        except HttpError as error:
            logger.error(f"Erro HTTP ao processar pasta raiz {folder_id}: {error}", exc_info=True)
        except Exception as e:
            logger.error(f"Erro inesperado ao processar pasta raiz {folder_id}: {e}", exc_info=True)
        # finally: # <-- Bloco finally removido daqui, limpeza é feita dentro de ingest_gdrive_folder
        #     if temp_dir_path and os.path.exists(temp_dir_path):
        #         try:
        #             shutil.rmtree(temp_dir_path)
        #             logger.info(f"Diretório temporário {temp_dir_path} removido.")
        #         except Exception as e_clean:
        #             logger.error(f"Erro ao remover diretório temporário {temp_dir_path}: {e_clean}", exc_info=True)
        #     temp_dir_path = None # Resetar para a próxima pasta

    # Retornar None por enquanto, pois run_pipeline não espera mais os dados diretamente
    # A lógica de processamento pega os dados do Supabase agora.
    # No futuro, pode retornar um status ou resumo.
    logger.info("Processamento de todas as pastas raiz concluído.")
    return None # Mudar se necessário


def main():
    parser = argparse.ArgumentParser(description="Ingestão de conteúdo do Google Drive.")
    parser.add_argument("--dry-run", action="store_true", help="Executa o script em modo dry-run, sem baixar arquivos ou interagir com APIs externas além da listagem inicial.")
    args = parser.parse_args()

    logger.info("Iniciando script de ingestão gdrive_ingest.py...")
    if args.dry_run:
        logger.info("*** EXECUTANDO EM MODO DRY-RUN ***")

    ingest_all_gdrive_content(dry_run=args.dry_run)

    logger.info("Script gdrive_ingest.py finalizado.")

if __name__ == "__main__":
    main()
