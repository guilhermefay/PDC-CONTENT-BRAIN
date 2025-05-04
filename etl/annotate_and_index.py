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
print("--- DEBUG: annotate_and_index.py STARTING ---", file=sys.stderr)
try:
    # Adiciona o diretório raiz do projeto ao PYTHONPATH
    # Isso garante que módulos como 'agents', 'infra', etc., sejam encontrados
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    print(f"--- DEBUG: Project root added to sys.path: {project_root} ---", file=sys.stderr)
except Exception as e_path:
    print(f"--- DEBUG: ERROR setting sys.path: {e_path} ---", file=sys.stderr)
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
from agents.annotator_agent import AnnotatorAgent
from supabase import create_client, Client, PostgrestAPIResponse
from postgrest.exceptions import APIError
from concurrent.futures import ThreadPoolExecutor, as_completed
from infra.r2r_client import R2RClientWrapper
from ingestion.gdrive_ingest import ingest_all_gdrive_content
from ingestion.local_ingest import ingest_local_directory
from ingestion.video_transcription import process_all_videos_in_directory

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
    3. (Opcional) Envia os chunks para o `AnnotatorAgent`.
    4. Armazena todos os chunks (com resultados da anotação) no Supabase.
    5. (Opcional) Envia os chunks marcados como `keep=True` para o R2R para indexação.
    6. Se o armazenamento/indexação for bem-sucedido, marca o `file_id` como processado no Supabase.

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
        except APIError as e:
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
        return None

    logging.info(f"[Chunking] Iniciando para {source_name}...")
    start_chunking = time.time()
    all_chunks = split_content_into_chunks(source_content, initial_metadata)
    chunking_time = time.time() - start_chunking
    logging.info(f"[Chunking] Concluído para {source_name} em {chunking_time:.2f} segundos. {len(all_chunks)} chunks criados.")

    if not all_chunks:
        logging.warning(f"Nenhum chunk criado para {source_name}. Pulando etapas subsequentes.")
        return 0

    # --- Anotação --- 
    processed_chunks = [] # Lista completa de chunks com dados de anotação
    if skip_annotation:
        logging.info(f"[Anotação] Pulando para {source_name} conforme solicitado.")
        processed_chunks = [{**chunk, 'keep': True, 'tags': ['interno', 'tecnico'], 'reason': 'Anotação pulada via flag'} for chunk in all_chunks]
    else:
        logging.info(f"[Anotação] Iniciando para {len(all_chunks)} chunks de {source_name}...")
        start_annotation = time.time()
        try:
            processed_chunks = annotator.run(all_chunks)
            annotation_time = time.time() - start_annotation
            logging.info(f"[Anotação] Concluída para {source_name} em {annotation_time:.2f} segundos. {len(processed_chunks)} chunks processados pela anotação.")
        except Exception as e:
            logging.error(f"Erro durante a execução do AnnotatorAgent para {source_name}: {e}", exc_info=True)
            processed_chunks = [{**chunk, 'keep': False, 'tags': [], 'reason': f'Falha na anotação: {e}'} for chunk in all_chunks]

    # --- Armazenamento/Indexação --- 
    chunks_sent_to_r2r_count = 0
    storage_successful = False # Flag para saber se armazenamento/indexação foi bem sucedido
    if skip_indexing:
        logging.info(f"[Armazenamento/Indexação] Pulando para {source_name} conforme solicitado.")
        return 0
    elif not processed_chunks:
         logging.info(f"[Armazenamento/Indexação] Nenhum chunk processado pela anotação para {source_name}. Pulando.")
         return 0
    else:
        logging.info(f"[Armazenamento/Indexação] Iniciando para {len(processed_chunks)} chunks de {source_name}...")
        start_storage = time.time()

        supabase_batch_data = []
        r2r_upload_tasks = [] 
        for chunk in processed_chunks:
            doc_id = chunk.get("document_id", str(uuid.uuid4()))
            chunk["document_id"] = doc_id 
            if supabase_client:
                supabase_data = {
                    "document_id": doc_id,
                    "content": chunk.get("content"),
                    "metadata": chunk.get("metadata"),
                    "annotation_tags": chunk.get("tags"),
                    "annotation_keep": chunk.get("keep", False),
                    "annotation_reason": chunk.get("reason", ""),
                    "token_count": count_tokens(chunk.get("content", ""))
                }
                supabase_batch_data.append(supabase_data)
            if r2r_client_instance and chunk.get("keep"):
                r2r_upload_tasks.append((chunk, doc_id))

        supabase_insert_ok = False
        if supabase_client and supabase_batch_data:
            logging.info(f"[Supabase] Tentando inserir {len(supabase_batch_data)} chunks em lote para {source_name}...")
            try:
                response: PostgrestAPIResponse = supabase_client.table('documents').insert(supabase_batch_data).execute()
                if response.data or (hasattr(response, 'status_code') and 200 <= response.status_code < 300):
                    logging.info(f"[Supabase] Inserção em lote bem-sucedida para {source_name}.")
                    supabase_insert_ok = True
                else:
                     logger.error(f"[Supabase] Inserção em lote pode ter falhado para {source_name}. Resposta: {getattr(response, 'data', 'N/A')}, Status: {getattr(response, 'status_code', 'N/A')}")
            except APIError as e:
                logger.error(f"[Supabase] Erro API na inserção em lote para {source_name}: {e}", exc_info=True)
                if hasattr(e, 'json') and callable(e.json):
                    try: logger.error(f"  - JSON do Erro: {e.json()}")
                    except Exception: pass
            except Exception as general_exception:
                logger.error(f"[Supabase] Erro inesperado na inserção em lote para {source_name}", exc_info=True)

        r2r_upload_ok = False
        if r2r_client_instance and r2r_upload_tasks:
            logging.info(f"[R2R] Iniciando upload paralelo de {len(r2r_upload_tasks)} chunks aprovados para {source_name}...")
            
            def upload_single_chunk_to_r2r(chunk_data, doc_id):
                """Função auxiliar para rodar em threads."""
                chunk_content = chunk_data.get("content")
                chunk_metadata = chunk_data.get("metadata", {})
                annotation_tags = chunk_data.get("tags", [])
                annotation_reason = chunk_data.get("reason", "")
                chunk_index = chunk_metadata.get("chunk_index", -1)
                temp_file_path = None
                try:
                    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix=".txt", encoding='utf-8') as temp_file:
                        temp_file.write(chunk_content)
                        temp_file_path = temp_file.name

                    # *** REVERTIDO: Preparar metadados COMPLETOS para R2R ***
                    r2r_metadata = chunk_metadata.copy()
                    # Garantir que 'tags' seja uma lista de strings
                    r2r_metadata['tags'] = [str(tag) for tag in annotation_tags if isinstance(tag, str)]
                    r2r_metadata['reason'] = annotation_reason
                    # Adicionar outros metadados úteis se necessário
                    r2r_metadata['token_count'] = count_tokens(chunk_content)
                    # Remover marcador de teste, se existir
                    r2r_metadata.pop('test_marker', None)
                    logger.debug(f"[R2R Upload Thread] Usando metadados completos: {r2r_metadata}")

                    upload_result = r2r_client_instance.upload_file(
                        file_path=temp_file_path,
                        document_id=doc_id,
                        metadata=r2r_metadata
                    )
                    return upload_result # Retorna o dicionário de resultado
                except Exception as e:
                    logger.error(f"[R2R Upload Thread] Erro para chunk {chunk_index} de {source_name}: {e}", exc_info=False) # Log simplificado em thread
                    return {"success": False, "error": str(e)}
                finally:
                    # Garantir limpeza do arquivo temporário
                    if temp_file_path and os.path.exists(temp_file_path):
                        try: os.remove(temp_file_path)
                        except Exception: pass

            # Usar ThreadPoolExecutor para paralelizar uploads R2R
            successful_r2r_uploads = 0
            with ThreadPoolExecutor(max_workers=max_workers_r2r_upload) as executor:
                future_to_chunk = {executor.submit(upload_single_chunk_to_r2r, chunk, doc_id): chunk for chunk, doc_id in r2r_upload_tasks}
                
                for future in as_completed(future_to_chunk):
                    chunk_info = future_to_chunk[future]
                    chunk_idx_log = chunk_info.get("metadata", {}).get("chunk_index", "?")
                    try:
                        result_dict = future.result()
                        if result_dict and result_dict.get("success"):
                            successful_r2r_uploads += 1
                            logger.debug(f"[R2R Upload Thread] Sucesso para chunk {chunk_idx_log} de {source_name}.")
                        else:
                            logger.warning(f"[R2R Upload Thread] Falha para chunk {chunk_idx_log} de {source_name}. Erro: {result_dict.get('error')}")
                    except Exception as exc:
                        logger.error(f"[R2R Upload Thread] Exceção ao obter resultado para chunk {chunk_idx_log} de {source_name}: {exc}")
            
            chunks_sent_to_r2r_count = successful_r2r_uploads
            logging.info(f"[R2R] Upload paralelo concluído para {source_name}. {chunks_sent_to_r2r_count}/{len(r2r_upload_tasks)} chunks enviados com sucesso.")
            # Considerar sucesso se pelo menos alguns chunks foram enviados?
            # Por enquanto, sucesso se não houver tarefas ou se alguma foi bem-sucedida.
            if not r2r_upload_tasks or chunks_sent_to_r2r_count > 0:
                 r2r_upload_ok = True
            # Se houveram tarefas mas NENHUMA foi bem-sucedida, considerar falha.
            elif r2r_upload_tasks and chunks_sent_to_r2r_count == 0:
                 r2r_upload_ok = False 
        elif not r2r_upload_tasks: # Se não havia chunks para enviar ao R2R
             r2r_upload_ok = True # Considerar sucesso para o passo R2R

        storage_time = time.time() - start_storage
        logging.info(f"[Armazenamento/Indexação] Concluído para {source_name} em {storage_time:.2f} segundos. {len(supabase_batch_data)} chunks armazenados no Supabase. {chunks_sent_to_r2r_count} chunks enviados para R2R.")
        
        # Condição de sucesso geral do armazenamento/indexação
        # Sucesso se a inserção no Supabase ocorreu E o upload R2R (se aplicável) ocorreu.
        if supabase_insert_ok and r2r_upload_ok:
             storage_successful = True

        # --- Adicionado: Marcar como processado APÓS sucesso --- 
        if storage_successful and supabase_client and source_id:
            logging.debug(f"[Mark Processed] Marcando {source_name} (ID: {source_id}) como processado...")
            try:
                # Usar upsert para criar ou atualizar o registro
                response = supabase_client.table('processed_files').upsert({
                    'file_id': source_id,
                    'status': 'processed',
                    'source': initial_metadata.get('origin', 'unknown'),
                    'last_processed_at': datetime.now(timezone.utc).isoformat()
                }).execute()
                # Verificar resposta do upsert
                if hasattr(response, 'data') and response.data:
                     logging.debug(f"[Mark Processed] Marcação (upsert) bem-sucedida para {source_id}.")
                elif hasattr(response, 'status_code') and 200 <= response.status_code < 300:
                     # Status code OK também indica sucesso, mesmo sem 'data' explícito no retorno upsert simples
                     logging.debug(f"[Mark Processed] Marcação (upsert) retornou status OK para {source_id}.")
                else:
                     logger.warning(f"[Mark Processed] Resposta inesperada do upsert para {source_id}: Status {getattr(response, 'status_code', 'N/A')}, Data: {getattr(response, 'data', 'N/A')}")

            except APIError as e:
                logging.error(f"[Mark Processed] Erro API ao marcar {source_id} como processado: {e}")
                if hasattr(e, 'json') and callable(e.json):
                    try: logger.error(f"  - JSON do Erro: {e.json()}")
                    except Exception: pass
            except Exception as e:
                logging.error(f"[Mark Processed] Erro inesperado ao marcar {source_id} como processado: {e}", exc_info=True)
        elif not storage_successful:
             logging.warning(f"[Mark Processed] Pulando marcação para {source_name} (ID: {source_id}) devido a falha no armazenamento/indexação.")
        # --- Fim Adicionado --- 

    # Retorna o número de chunks enviados para R2R se o armazenamento/indexação foi bem sucedido
    return chunks_sent_to_r2r_count if storage_successful else None

def run_pipeline(
    source: str,
    local_dir: str,
    dry_run: bool,
    dry_run_limit: Optional[int],
    skip_annotation: bool,
    skip_indexing: bool,
    max_workers_r2r_upload: int = 5
):
    print("--- DEBUG: Entering run_pipeline function --- ", file=sys.stderr) # DEBUG LOG
    """
    Executa o pipeline ETL completo, focado na ingestão do Google Drive.

    Coordena a ingestão via `ingest_all_gdrive_content`, processamento paralelo
    de documentos/vídeos (chunking, anotação, armazenamento, indexação) e loga
    um resumo final.

    Args:
        source (str): Tipo da fonte de dados (gdrive, video, local).
        local_dir (str): Diretório para fontes 'video' ou 'local'.
        dry_run (bool): Se True, executa a ingestão mas pula anotação e indexação.
        dry_run_limit (Optional[int]): Limita o número de arquivos processados pela
                                        ingestão do Google Drive em modo dry_run.
        skip_annotation (bool): Se True, pula a etapa de anotação via CrewAI.
        skip_indexing (bool): Se True, pula o armazenamento no Supabase e indexação no R2R.
        max_workers_r2r_upload (int): Número máximo de threads para uploads R2R.

    Raises:
        ValueError: Se variáveis de ambiente essenciais não estiverem configuradas.
        Exception: Se ocorrer erro na inicialização dos clientes (Supabase, R2R, Annotator).
    """
    # Tentar carregar explicitamente o .env da raiz do projeto
    # Esta parte pode permanecer aqui ou ser movida para fora se a configuração for global
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    loaded = load_dotenv(dotenv_path=dotenv_path, override=True, verbose=True)
    if not loaded:
        logger.warning(f"Arquivo .env não encontrado ou não carregado em: {dotenv_path}. Tentando carregar do diretório atual ou ambiente.")
        load_dotenv(override=True, verbose=True)
    else:
        logger.info(f"Arquivo .env carregado de: {dotenv_path}")

    supabase_url = os.getenv("SUPABASE_URL")
    # Usar SUPABASE_SERVICE_KEY consistentemente
    supabase_service_key = os.getenv("SUPABASE_SERVICE_KEY") 
    r2r_base_url = os.getenv("R2R_BASE_URL")

    # Validação movida para o início da função
    if not all([supabase_url, supabase_service_key, r2r_base_url]):
        logging.error(f"Variáveis de ambiente obrigatórias ausentes: SUPABASE_URL={'OK' if supabase_url else 'FALTA'}, SUPABASE_SERVICE_KEY={'OK' if supabase_service_key else 'FALTA'}, R2R_BASE_URL={'OK' if r2r_base_url else 'FALTA'}")
        # Considerar levantar uma exceção em vez de return para testes
        raise ValueError("Missing required environment variables for pipeline execution.")
        # return

    # Inicialização dos clientes e agentes dentro da função para escopo de execução
    # Isso permite mockar as instâncias durante os testes
    try:
        supabase: Client = create_client(supabase_url, supabase_service_key) # Usar a variável correta
        logger.info("Cliente Supabase inicializado para pipeline ETL.")
    except Exception as e:
        logger.error(f"Erro ao inicializar cliente Supabase: {e}", exc_info=True)
        raise # Re-lança a exceção para falhar o pipeline
        
    try:
        r2r_client = R2RClientWrapper()
        logger.info("R2R Client Wrapper inicializado para pipeline ETL.")
    except Exception as e:
        logger.error(f"Erro ao inicializar R2R Client Wrapper: {e}", exc_info=True)
        raise # Re-lança a exceção
        
    try:
        annotator = AnnotatorAgent()
        # Verificar se annotator.run existe, mesmo que não seja explicitamente chamado aqui
        # A verificação original está mantida para segurança, mas pode ser redundante
        if not hasattr(annotator, 'run'):
            logger.error("ERRO CRÍTICO: Instância de AnnotatorAgent NÃO possui o método 'run' após a criação.")
            raise TypeError("AnnotatorAgent instance is missing the 'run' method.")
        logger.info("AnnotatorAgent inicializado para pipeline ETL.")
    except Exception as e:
        logger.error(f"Erro ao inicializar AnnotatorAgent: {e}", exc_info=True)
        raise # Re-lança a exceção

    # Step 1: Ingestão de Dados por fonte
    logging.info(f"Starting ETL pipeline for source: {source}")
    all_files_data = []
    temp_video_dir = None
    temp_dirs_to_clean = []
    try:
        if source == 'gdrive':
            logging.info("--- Etapa 1: Ingestão de Dados (GDrive) ---")
            ingested_data, temp_video_dir = ingest_all_gdrive_content(dry_run=dry_run)
            all_files_data = ingested_data
            if temp_video_dir:
                temp_dirs_to_clean.append(temp_video_dir)
        elif source == 'local':
            logging.info(f"Ingestão de arquivos locais de: {local_dir}")
            all_files_data = ingest_local_directory(local_dir, dry_run=dry_run, dry_run_limit=dry_run_limit)
        elif source == 'video':
            logging.info(f"Ingestão de vídeos de: {local_dir}")
            video_results = process_all_videos_in_directory(local_dir)
            all_files_data = [
                {'content': data.get('text'), 'metadata': data.get('metadata', {})}
                for data in video_results.values()
            ]
        else:
            logging.error(f"Source '{source}' não suportado para pipeline ETL.")
            raise ValueError(f"Source '{source}' not supported for ETL pipeline.")

        if not all_files_data:
            logging.warning("Nenhum dado ingerido. Saindo do pipeline.")
            return
        if dry_run:
            logging.info("Dry run: Pulando etapas de anotação e indexação.")
            return

        # --- Step 2 & 3: Chunking, Annotating and Indexing Data --- 
        logging.info("--- Step 2 & 3: Chunking, Annotating and Indexing Data ---")
        total_processed_chunks = 0
        processed_sources = set()
        failed_sources = set()
        max_workers_processing = 5 # Usar um valor razoável para processamento paralelo

        # *** RESTAURADO: Usar ThreadPoolExecutor para processar documentos em paralelo ***
        with ThreadPoolExecutor(max_workers=max_workers_processing) as executor:
            future_to_source = {
                executor.submit(
                    process_single_source_document,
                    source_doc,
                    annotator,
                    r2r_client,
                    supabase,
                    skip_annotation,
                    skip_indexing,
                    # Passar max_workers para upload R2R (pode ser diferente)
                    # max_workers_r2r_upload=5 
                ): source_doc.get('metadata', {}).get('source_name', f'unknown_source_{i}')
                for i, source_doc in enumerate(all_files_data)
            }

            for future in as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    num_chunks = future.result() # Obter o resultado da thread
                    if num_chunks is not None:
                        total_processed_chunks += num_chunks
                        processed_sources.add(source_name)
                        logging.info(f"Successfully processed {source_name} ({num_chunks} chunks sent to R2R).")
                    else:
                        logging.warning(f"Processing skipped entirely for: {source_name}")
                        failed_sources.add(source_name)
                except Exception as exc:
                     logging.error(f'{source_name} generated an exception during processing: {exc}', exc_info=True)
                     failed_sources.add(source_name)
        # *** FIM RESTAURADO ***

        # --- Final Summary --- 
        logging.info("\n--- ETL Pipeline Finished ---")
        logging.info(f"Total source documents/videos processed attempt: {len(all_files_data)}")
        logging.info(f"Successfully processed sources: {len(processed_sources)}")
        logging.info(f"Failed/Skipped sources: {len(failed_sources)}")
        logging.info(f"Total chunks processed and potentially indexed: {total_processed_chunks}")
        if failed_sources:
            logging.warning(f"Failed sources list: {failed_sources}")

    except Exception as e:
        logging.error(f"Erro fatal no pipeline ETL: {e}", exc_info=True)
        raise
    finally:
        # Limpeza de diretórios temporários
        for td in temp_dirs_to_clean:
                try:
                    shutil.rmtree(td)
                    logging.info(f"Diretório temporário removido: {td}")
                except Exception as cleanup_err:
                    logging.error(f"Erro ao limpar diretório temporário {td}: {cleanup_err}")

def main():
    print("--- DEBUG: Entering main function --- ", file=sys.stderr) # DEBUG LOG
    """Função principal para executar o pipeline via linha de comando."""
    parser = argparse.ArgumentParser(description="Pipeline ETL para processar e indexar conteúdo PDC do Google Drive.")
    parser.add_argument("--source", type=str, required=True, choices=['gdrive', 'video', 'local'], help="Tipo da fonte de dados (gdrive, video, local)")
    parser.add_argument("--local-dir", type=str, default="data/raw", help="Diretório para fontes 'video' ou 'local'. Padrão: data/raw")
    parser.add_argument("--skip-annotation", action="store_true", help="Pula a etapa de anotação via CrewAI.")
    parser.add_argument("--skip-indexing", action="store_true", help="Pula o armazenamento no Supabase e indexação no R2R.")
    parser.add_argument("--dry-run", action="store_true", help="Executa apenas a ingestão do GDrive e mostra o que seria processado.")
    parser.add_argument("--dry-run-limit", type=int, default=None, help="Limita o número de arquivos na ingestão do GDrive em modo dry-run.")
    parser.add_argument("--max-r2r-workers", type=int, default=5, help="Número máximo de threads para upload paralelo ao R2R.")

    args = parser.parse_args()

    # Configurar logging para arquivo e console
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "etl.log")

    # Remover handlers antigos para evitar duplicação em execuções múltiplas no mesmo processo (raro)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configurar de novo
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - [%(name)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'), # Append mode
            logging.StreamHandler() # Console
        ]
    )

    logger.info(f"\n===== INICIANDO PIPELINE ETL em {datetime.now()} =====")
    logger.info(f"Argumentos: {args}")

    try:
        # Chamar run_pipeline sem os argumentos removidos
        run_pipeline(
            source=args.source,
            local_dir=args.local_dir,
            dry_run=args.dry_run,
            dry_run_limit=args.dry_run_limit,
            skip_annotation=args.skip_annotation,
            skip_indexing=args.skip_indexing,
            max_workers_r2r_upload=args.max_r2r_workers
        )
    except ValueError as ve:
         logger.error(f"Erro de configuração impediu a execução do pipeline: {ve}")
    except Exception as e:
         logger.error(f"Erro inesperado durante a execução do pipeline: {e}", exc_info=True)
    finally:
        logger.info(f"===== PIPELINE ETL FINALIZADO em {datetime.now()} =====\n")


if __name__ == "__main__":
    print("--- DEBUG: Running under __main__ block --- ", file=sys.stderr) # DEBUG LOG
    main() 