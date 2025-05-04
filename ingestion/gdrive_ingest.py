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
import logging # Adicionar import logging
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from docling.document_converter import DocumentConverter
import tempfile
from typing import Any, List, Dict, Optional # Adicionado Optional
from ingestion.image_processor import ImageProcessor  # Import para processamento de imagens
from supabase import create_client, Client, PostgrestAPIResponse
from postgrest.exceptions import APIError

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

def authenticate_gdrive():
    """Autentica na API do Google Drive usando credenciais de Service Account.

    Lê o caminho para o arquivo JSON de credenciais da variável de ambiente
    `GOOGLE_SERVICE_ACCOUNT_JSON`.

    Returns:
        googleapiclient.discovery.Resource: Um objeto de serviço autorizado da API
                                            do Google Drive (v3).

    Raises:
        ValueError: Se a variável de ambiente `GOOGLE_SERVICE_ACCOUNT_JSON` não
                    estiver definida ou o arquivo não for encontrado.
        googleapiclient.errors.HttpError: Se ocorrer um erro durante a autenticação.
        Exception: Para outros erros inesperados.
    """
    creds_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_path or not os.path.exists(creds_path):
        logger.error("Caminho para GOOGLE_SERVICE_ACCOUNT_JSON não definido ou inválido.")
        raise ValueError("Caminho para GOOGLE_SERVICE_ACCOUNT_JSON não definido ou inválido.")

    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    try:
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=scopes)
        service = build('drive', 'v3', credentials=creds)
        logger.info("Autenticação com Google Drive bem-sucedida.")
        return service
    except HttpError as auth_error:
        logger.error(f"Erro HTTP durante autenticação com Google Drive: {auth_error}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado durante autenticação com Google Drive: {e}", exc_info=True)
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
    temp_dir_path: str,
    dry_run: bool = False,
    access_level: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Ingere arquivos de uma pasta específica do Google Drive e suas subpastas.

    Lista arquivos na pasta (com paginação), identifica tipos suportados
    (documentos, vídeos) e subpastas. Para documentos, baixa/exporta e extrai texto.
    Para vídeos, baixa o arquivo para um diretório temporário.
    Para subpastas, chama a si mesma recursivamente.

    Args:
        service: Objeto de serviço autorizado da API do Google Drive.
        folder_name (str): Nome lógico da pasta (para metadados).
        folder_id (str): O ID da pasta no Google Drive.
        temp_dir_path (str): Caminho para o diretório onde os vídeos baixados
                             serão salvos temporariamente.
        dry_run (bool): Se True, apenas lista os arquivos/pastas que seriam processados
                        sem baixar ou extrair conteúdo. Defaults to False.
        access_level (str): O nível de acesso ('internal' ou 'student') herdado
                            da pasta pai ou determinado inicialmente.

    Returns:
        List[Dict[str, Any]]: Lista de dicionários representando os itens ingeridos.
    """
    results = [] # Inicialização correta
    logger.info(f"\nIniciando ingestão da pasta: {folder_name} (ID: {folder_id}, Access: {access_level})")

    page_token = None
    processed_in_folder = 0 # Contador para arquivos processados *nesta* chamada

    try:
        while True: # Loop de paginação
            try:
                # Listar todos os itens na pasta (filtragem de tipos realizada posteriormente)
                response = service.files().list(
                    q=f"'{folder_id}' in parents and trashed=false",
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, size)',
                    pageToken=page_token,
                    pageSize=100
                ).execute()
            except HttpError as list_error:
                logger.error(f"Erro HTTP ao listar arquivos na página (token: {page_token}) da pasta {folder_name}: {list_error}")
                break 
            except Exception as list_exc:
                logger.error(f"Erro inesperado ao listar arquivos na página (token: {page_token}) da pasta {folder_name}: {list_exc}", exc_info=True)
                break 

            files = response.get('files', [])
            if not files and not page_token:
                 logger.info(f"  Nenhum item encontrado na pasta {folder_name}.")
                 break 

            logger.info(f"  Encontrados {len(files)} itens nesta página para {folder_name}...")

            for file in files:
                file_id = file.get('id')
                file_name = file.get('name')
                mime_type = file.get('mimeType')
                file_size = file.get('size')

                file_data = None

                # --- Lógica de Filtragem --- BEGIN ---
                file_ext = os.path.splitext(file_name)[1].lower()
                is_hidden = file_name.startswith('.')
                # Normalizar nome para comparação (caso de nomes com/sem case variado)
                normalized_file_name = file_name.lower()

                if normalized_file_name in IGNORED_FILENAMES or \
                   file_ext in IGNORED_EXTENSIONS or \
                   is_hidden:
                    # Corrigir indentação e garantir que não há espaços após \
                    reason = (f"(Nome: {normalized_file_name in IGNORED_FILENAMES}, "
                              f"Ext: {file_ext in IGNORED_EXTENSIONS}, "
                              f"Oculto: {is_hidden})")
                    logger.info(f"  -> Ignorando arquivo irrelevante/config: {file_name} {reason}")
                    continue # Pula para o próximo item no loop
                # --- Lógica de Filtragem --- END ---

                # ===== Verificar se já foi processado (ANTES de qualquer download/processamento) =====
                if file_id and supabase_client and mime_type != 'application/vnd.google-apps.folder': # Não verificar pastas
                    try:
                        # Usar file_id que é o gdrive_id
                        logger.debug(f"  [Check Supabase] Verificando se file_id '{file_id}' existe em 'processed_files'...")
                        # Renomear variável para evitar conflito
                        supabase_response = supabase_client.table('processed_files')\
                                                .select('file_id', count='exact')\
                                                .eq('file_id', file_id)\
                                                .execute()

                        # Verificar o atributo count da nova variável
                        if supabase_response.count > 0:
                            logger.info(f"  [Check Supabase] Arquivo '{file_name}' (ID: {file_id}) já existe em 'processed_files'. Pulando.")
                            continue # Pula para o próximo arquivo neste loop
                        else:
                            logger.debug(f"  [Check Supabase] Arquivo '{file_name}' (ID: {file_id}) não encontrado em 'processed_files'. Prosseguindo.")

                    except APIError as api_err:
                        logger.error(f"  [Check Supabase] Erro API ao verificar {file_name} (ID: {file_id}): {api_err}")
                        # Decisão: Pular este arquivo se a verificação falhar para evitar reprocessamento acidental?
                        # Por segurança, vamos pular se não pudermos confirmar que *não* foi processado.
                        logger.warning(f"  [Check Supabase] Pulando arquivo {file_name} devido a erro na verificação.")
                        continue
                    except Exception as e:
                        logger.error(f"  [Check Supabase] Erro inesperado ao verificar {file_name} (ID: {file_id}): {e}", exc_info=True)
                        # Pular também em caso de erro inesperado na verificação
                        logger.warning(f"  [Check Supabase] Pulando arquivo {file_name} devido a erro inesperado na verificação.")
                        continue
                elif not file_id:
                    logger.warning(f"  Arquivo '{file_name}' sem ID encontrado na pasta {folder_name}. Pulando.")
                    continue
                elif not supabase_client:
                    # Apenas logar uma vez se o cliente não estiver disponível?
                    # Já logado na inicialização, talvez não precise aqui.
                    pass # Continuar sem verificação se o cliente não foi inicializado

                # ===== Fim da Verificação =====

                # ===== MODIFICAÇÃO AQUI: Tratar Pastas Recursivamente =====
                if mime_type == 'application/vnd.google-apps.folder':
                    logger.info(f"  Identificada SUBPASTA: {file_name} (ID: {file_id}). Iniciando ingestão recursiva...")
                    subfolder_data = ingest_gdrive_folder(
                        service,
                        f"{folder_name}/{file_name}",  # Constrói nome hierárquico
                        file_id,
                        temp_dir_path,
                        dry_run=dry_run,
                        access_level=access_level
                    )
                    results.extend(subfolder_data) # Adiciona resultados da subpasta
                    continue # Pula para o próximo item na pasta atual
                # ===== MODIFICAÇÃO: Processar IMAGENS =====
                elif is_supported_image(file):
                    logger.info(f"  Identificada IMAGEM: {file_name} (ID: {file_id}, Tipo: {mime_type})")
                    if dry_run:
                        file_data = {
                            "type": "image",
                            "id": file_id,
                            "name": file_name,
                            "content": None,
                            "embeddings": None,
                            "metadata": {
                                "gdrive_id": file_id,
                                "gdrive_name": file_name,
                                "gdrive_mime": mime_type,
                                "gdrive_folder_name": folder_name,
                                "gdrive_folder_id": folder_id,
                                "access_level": access_level
                            }
                        }
                        logger.info(f"    -> DRY RUN: Imagem {file_name} seria processada.")
                    else:
                        image_bytes = download_file(service, file_id)
                        if image_bytes:
                            text = image_processor.extract_text(image_bytes)
                            metadata = image_processor.extract_metadata(image_bytes)
                            embeddings = image_processor.generate_embeddings(image_bytes)
                            file_data = {
                                "type": "image",
                                "id": file_id,
                                "name": file_name,
                                "content": text,
                                "embeddings": embeddings,
                                "metadata": {
                                    "gdrive_id": file_id,
                                    "gdrive_name": file_name,
                                    "gdrive_mime": mime_type,
                                    "gdrive_folder_name": folder_name,
                                    "gdrive_folder_id": folder_id,
                                    "access_level": access_level,
                                    **metadata
                                }
                            }
                            logger.info(f"    -> Imagem {file_name} processada: texto extraído={bool(text)}, embeddings obtidos={len(embeddings) if embeddings else 0}")
                        else:
                            logger.warning(f"    -> Falha ao baixar a imagem: {file_name} (ID: {file_id})")
                # ============================================================

                # 1. Processar VÍDEOS
                elif mime_type in VIDEO_MIME_TYPES:
                    logger.info(f"  Identificado VÍDEO: {file_name} (ID: {file_id}, Tipo: {mime_type}, Tamanho: {file_size})")
                    if dry_run:
                        # ... (lógica dry_run existente) ...
                        file_data = {"type": "video", "id": file_id, "name": file_name, "path": "dry_run_video", "metadata": {"access_level": access_level}}
                        logger.info(f"    -> DRY RUN: Video {file_name} seria baixado.")
                    else:
                        video_bytes = download_file(service, file_id)
                        if video_bytes:
                            safe_filename = "".join(c for c in file_name if c.isalnum() or c in (' ', '.', '_')).rstrip()
                            video_path = os.path.join(temp_dir_path, safe_filename)
                            try:
                                with open(video_path, 'wb') as f:
                                    f.write(video_bytes)
                                file_data = {
                                    "type": "video",
                                    "id": file_id,
                                    "name": file_name,
                                    "path": video_path,
                                    "metadata": {
                                        "gdrive_id": file_id,
                                        "gdrive_name": file_name,
                                        "gdrive_mime": mime_type,
                                        "gdrive_folder_name": folder_name, # Manter nome da pasta pai imediata?
                                        "gdrive_folder_id": folder_id,
                                        "access_level": access_level
                                    }
                                }
                                logger.info(f"    -> Vídeo {file_name} baixado para {video_path}")
                            except IOError as e:
                                logger.error(f"    -> Erro ao salvar vídeo {file_name} em {video_path}: {e}")
                        else:
                            logger.warning(f"    -> Falha ao baixar o vídeo: {file_name} (ID: {file_id})")

                # 2. Processar DOCUMENTOS
                elif mime_type in SUPPORTED_MIME_TYPES:
                    logger.info(f"  Identificado DOCUMENTO: {file_name} (ID: {file_id}, Tipo: {mime_type})")
                    if dry_run:
                        # ... (lógica dry_run existente) ...
                        file_data = {"type": "document", "id": file_id, "name": file_name, "content": "dry_run_content", "metadata": {"access_level": access_level}}
                        logger.info(f"    -> DRY RUN: Documento {file_name} seria baixado/exportado e processado.")
                    else:
                        file_content_bytes = None
                        effective_mime_type = mime_type
                        if mime_type == 'application/vnd.google-apps.document':
                            file_content_bytes = export_and_download_gdoc(service, file_id, GDRIVE_EXPORT_MIME)
                            effective_mime_type = GDRIVE_EXPORT_MIME
                        elif mime_type in DOCUMENT_MIME_TYPES:
                            file_content_bytes = download_file(service, file_id)
                        # else: Bloco removido pois já tratado por SUPPORTED_MIME_TYPES

                        if file_content_bytes:
                            text_content = extract_text_from_file(effective_mime_type, file_content_bytes, file_name)
                            if text_content:
                                file_data = {
                                    "type": "document",
                                    "id": file_id,
                                    "name": file_name,
                                    "content": text_content,
                                    "metadata": {
                                        "gdrive_id": file_id,
                                        "gdrive_name": file_name,
                                        "gdrive_mime": mime_type,
                                        "gdrive_folder_name": folder_name,
                                        "gdrive_folder_id": folder_id,
                                        "access_level": access_level
                                    }
                                }
                                logger.info(f"    -> Texto extraído com sucesso de {file_name}.")
                            else:
                                logger.warning(f"    -> Falha ao extrair texto de {file_name} após download/export.")
                        else:
                             logger.warning(f"    -> Falha ao obter conteúdo binário para {file_name} (ID: {file_id}).")

                else:
                    logger.warning(f"  -> Item ignorado (tipo MIME não suportado ou não é pasta): {file_name} ({mime_type})")

                # Se o arquivo foi processado (vídeo ou doc), adiciona aos dados
                if file_data:
                    results.append(file_data) # <<< CORRIGIR: usar 'results' em vez de 'ingested_data'
                    processed_in_folder += 1

            page_token = response.get('nextPageToken', None)
            logger.debug(f"Processamento da página concluído para pasta {folder_name}. Próximo pageToken: {page_token}")
            if page_token is None:
                logger.info(f"  Fim da listagem para a pasta {folder_name}.")
                break

    except Exception as e:
        logger.error(f"Erro inesperado durante a ingestão da pasta {folder_name}: {e}", exc_info=True)

    logger.info(f"Ingestão da pasta {folder_name} e subpastas concluída. Total de itens processados nesta chamada recursiva: {processed_in_folder}")
    return results # <<< CORRIGIR: retornar 'results' em vez de 'ingested_data'

def ingest_all_gdrive_content(dry_run=False):
    """Ingere conteúdo de todas as pastas configuradas no Google Drive.

    Coordena a autenticação, determina as pastas a processar com base nas
    variáveis de ambiente (`GDRIVE_ROOT_FOLDER_ID`, `GDRIVE_MARKETING_FOLDER_ID`),
    cria um diretório temporário para vídeos e chama `ingest_gdrive_folder`
    para cada pasta configurada.

    Args:
        dry_run (bool): Se True, repassa o modo dry_run para `ingest_gdrive_folder`.
                        Defaults to False.

    Returns:
        Tuple[List[Dict[str, Any]], Optional[str]]:
            - Uma lista contendo todos os dados ingeridos de todas as pastas.
            - O caminho para o diretório temporário criado para vídeos (ou None se nenhum
              foi criado ou se ocorreu erro).
    """
    service = authenticate_gdrive()
    all_ingested_data = []

    drive_folder_ids_to_process = {}
    root_folder_id = os.getenv("GDRIVE_ROOT_FOLDER_ID")
    marketing_folder_id = os.getenv("GDRIVE_MARKETING_FOLDER_ID", "18DqNZ7dyJfrkiCI4gF8TumjOyND6iI6M")

    if root_folder_id:
        drive_folder_ids_to_process['arquivos_pdc'] = root_folder_id
    else:
        logger.warning("Variável de ambiente GDRIVE_ROOT_FOLDER_ID não definida. Pasta 'arquivos_pdc' será ignorada.")

    if marketing_folder_id:
        drive_folder_ids_to_process['marketing_digital'] = marketing_folder_id
    else:
        logger.warning("Variável de ambiente GDRIVE_MARKETING_FOLDER_ID não definida e fallback não disponível. Pasta 'marketing_digital' será ignorada.")

    if not drive_folder_ids_to_process:
        logger.error("Nenhum ID de pasta do Google Drive válido encontrado (via variáveis de ambiente). Abortando ingestão.")
        return [], None

    temp_video_dir = tempfile.mkdtemp(prefix="gdrive_videos_")
    logger.info(f"Diretório temporário para vídeos criado em: {temp_video_dir}")
    logger.info(f"Pastas que serão processadas: {json.dumps(drive_folder_ids_to_process, indent=2)}")

    try:
        for folder_name, folder_id in drive_folder_ids_to_process.items():
            if not folder_id or '_PLACEHOLDER' in folder_id:
                 logger.warning(f"ID da pasta '{folder_name}' parece ser um placeholder ('{folder_id}'). Pulando esta pasta.")
                 continue

            # ===== MODIFICAÇÃO AQUI: Determinar e passar access_level inicial =====
            # Determinar o nível de acesso inicial para as pastas raiz
            initial_access_level = "internal" # Default
            if folder_id == root_folder_id:
                initial_access_level = "student"
            elif folder_id == marketing_folder_id:
                initial_access_level = "internal"
            else: # Caso algum outro ID seja configurado diretamente
                logger.warning(f"ID da pasta raiz {folder_id} não reconhecido como ROOT ou MARKETING. Usando 'internal' como default.")
                initial_access_level = "internal"

            logger.info(f"Iniciando processo para pasta raiz: {folder_name}")
            folder_data = ingest_gdrive_folder(
                service,
                folder_name,
                folder_id,
                temp_video_dir,
                dry_run
            )
            # ===================================================================

            all_ingested_data.extend(folder_data)
            # if dry_run and folder_data: # Dry run agora percorre tudo
            #      logger.info("Dry run: Parando após processar a primeira pasta com sucesso.")
            #      break

    except Exception as e:
        logger.error(f"Erro fatal durante a ingestão de todas as pastas: {e}", exc_info=True)

    logger.info(f"Ingestão de todas as pastas GDrive concluída. Total de itens ingeridos: {len(all_ingested_data)}.")
    return all_ingested_data, temp_video_dir

def main():
    """Função principal para executar a ingestão do Google Drive via linha de comando.

    Processa argumentos `--dry-run` e `--output-json`, chama
    `ingest_all_gdrive_content` e salva um resumo dos itens ingeridos
    em um arquivo JSON.
    """
    parser = argparse.ArgumentParser(description='Ingere arquivos do Google Drive e extrai conteúdo.')
    parser.add_argument('--dry-run', action='store_true', help='Apenas lista os arquivos que seriam processados.')
    parser.add_argument('--output-json', type=str, default='/tmp/gdrive_ingest_summary.json', help='Caminho para salvar o resumo JSON dos arquivos ingeridos.')
    args = parser.parse_args()

    logger.info("Iniciando script de ingestão do Google Drive...")
    # logger.info(f"Pastas configuradas: {json.dumps(DRIVE_FOLDER_IDS, indent=2)}") # Removido daqui

    ingested_items, temp_dir = ingest_all_gdrive_content(dry_run=args.dry_run)

    # Preparar resumo para salvar
    summary = []
    for item in ingested_items:
        summary_item = {
            "id": item.get("id"),
            "name": item.get("name"),
            "type": item.get("type"),
            "metadata": item.get("metadata", {})
        }
        if item.get("type") == "document":
             summary_item["content_length"] = len(item.get("content", ""))
        elif item.get("type") == "video":
             summary_item["temp_path"] = item.get("path")
        elif item.get("type") == "image":
             summary_item["content_length"] = len(item.get("content", "")) if item.get("content") else 0
             summary_item["embeddings_length"] = len(item.get("embeddings", [])) if item.get("embeddings") else 0
        summary.append(summary_item)

    # Salvar o resumo em JSON
    try:
        output_path = args.output_json
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        logger.info(f"Resumo da ingestão salvo em: {output_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar o resumo JSON: {e}")

    # Log final sobre o diretório temporário
    logger.info(f"Ingestão concluída. Vídeos (se houver) estão em {temp_dir}. A limpeza deste diretório deve ser feita pelo ETL principal.")
    # NÃO remover o temp_dir aqui, pois o ETL precisa dele.

    logger.info("Script de ingestão do Google Drive finalizado.")

if __name__ == "__main__":
    main()
