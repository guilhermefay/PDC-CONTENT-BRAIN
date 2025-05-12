#!/usr/bin/env python3
import os
import json
import argparse
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any

from supabase import create_client, Client
from dotenv import load_dotenv
from postgrest.exceptions import APIError

# Importar a função de pós-processamento real
from scripts.utils.processing_logic import _post_process_sections

# Tentativa de importar StructureAnalyzerAgent e IdentifiedSection
# Se scripts/restructure_chunks.py estiver no mesmo nível ou no PYTHONPATH
try:
    from agents.structure_analyzer_agent import StructureAnalyzerAgent, IdentifiedSection
    # Supondo que _post_process_sections e outras constantes/funções úteis de restructure_chunks.py
    # possam ser refatoradas em um módulo compartilhado ou importadas diretamente se necessário.
    # Por enquanto, vamos focar na estrutura principal deste novo script.
    # from .restructure_chunks import MIN_SECTION_LENGTH, TARGET_CHUNK_SIZE, ESSENTIAL_SHORT_TYPES # Exemplo
except ImportError:
    logging.error("Falha ao importar StructureAnalyzerAgent ou outros componentes. Verifique o PYTHONPATH e a estrutura do projeto.")
    # Definir classes dummy para o script não quebrar completamente se a importação falhar durante o desenvolvimento inicial
    class IdentifiedSection:
        def __init__(self, section_type: str, content: str, original_content: Optional[str] = None, metadata: Optional[Dict] = None):
            self.section_type = section_type
            self.content = content
            self.original_content = original_content if original_content is not None else content
            self.metadata = metadata if metadata is not None else {}
        def __repr__(self):
            return f"IdentifiedSection(type='{self.section_type}', len='{len(self.content)}')"

    class StructureAnalyzerAgent:
        def __init__(self, *args, **kwargs): # Modificado para aceitar quaisquer argumentos
            logger.warning("Usando StructureAnalyzerAgent DUMMY devido a falha na importação.")
        def analyze_document_structure(self, text: str, source_type: str, context: Optional[str] = None) -> List[IdentifiedSection]:
            logger.warning("StructureAnalyzerAgent DUMMY: analyze_document_structure chamado, retornando seção de fallback.")
            return [IdentifiedSection(section_type=f"{source_type}_corpo_geral_dummy", content=text)]

# Configuração básica de logging
log_format = '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente do .env
load_dotenv()

# Configuração do cliente Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Variáveis de ambiente SUPABASE_URL ou SUPABASE_SERVICE_KEY não definidas.")
    # exit(1) # Comentado para permitir execução mockada sem DB para testes iniciais de lógica
    supabase_client: Optional[Client] = None
else:
    try:
        supabase_client: Optional[Client] = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Cliente Supabase inicializado com sucesso.")
    except Exception as e:
        logger.error(f"Falha ao inicializar cliente Supabase: {e}")
        supabase_client = None

# --- Constantes e Configurações Específicas para Transcrições ---
DEFAULT_INPUT_DIR = "data/transcriptions_youtube/metadata" # Diretório onde os {VIDEO_ID}.json estão
# Definir constantes de nome de diretório que estavam faltando
TRANSCRIPTIONS_DIR_NAME = "transcriptions"
METADATA_DIR_NAME = "metadata"

DB_INSERT_BATCH_SIZE = 50
NEW_SOURCE_TYPE = "youtube_video_transcription"

# Constantes locais MIN_SECTION_LENGTH e TARGET_CHUNK_SIZE removidas pois não são usadas
# ou são sobrepostas pela lógica em scripts.utils.processing_logic
ESSENTIAL_SHORT_TYPES_TRANSCRIPTS = {"video_title_card", "video_intro_music", "video_outro_music"} # Exemplo

VOCABULARIES_TRANSCRIPTS = { # Exemplo de vocabulário para transcrições
    NEW_SOURCE_TYPE: [
        "introducao_apresentador",
        "fala_principal",
        "segmento_perguntas_respostas",
        "demonstracao_tela",
        "conclusao_resumo",
        "chamada_para_acao",
        "vinheta_encerramento"
    ]
}

def find_transcription_metadata_files(input_dir: str, limit: Optional[int] = None) -> List[str]:
    """Encontra arquivos .json de metadados de transcrição no diretório de entrada."""
    json_files = []
    if not os.path.isdir(input_dir):
        logger.error(f"Diretório de entrada não encontrado: {input_dir}")
        return json_files

    video_ids_processed = 0
    for root, _, files in os.walk(input_dir):
        if limit and video_ids_processed >= limit:
            break
        for file in files:
            if file.endswith(".json"):
                # Assumindo que o nome do arquivo é {VIDEO_ID}.json
                # e está em uma estrutura como .../metadata/{VIDEO_ID}/{VIDEO_ID}.json
                # No nosso caso, é .../metadata/{VIDEO_ID}.json (ajustar se a estrutura for diferente)
                # Com a estrutura do transcribe_youtube_videos.py, o file é {VIDEO_ID}.json
                # e está em .../metadata/{VIDEO_ID}/{VIDEO_ID}.json
                # Se o transcribe_youtube_videos salva em .../metadata/{VIDEO_ID}.json, isso está ok.
                # O script anterior salva em metadata_path/{VIDEO_ID}/{VIDEO_ID}.json
                # Portanto, o 'root' aqui será o /metadata/{VIDEO_ID}/
                
                # Correção baseada na estrutura de `transcribe_youtube_videos.py`:
                # `metadata_path = os.path.join(output_dir, METADATA_DIR_NAME, video_id)`
                # `json_filepath = os.path.join(metadata_path, f"{video_id}.json")`
                # Isso significa que cada JSON está em sua própria subpasta video_id.
                
                # Verificando se o diretório pai é nomeado com o video_id e o arquivo também
                dir_name = os.path.basename(root)
                file_basename_no_ext = os.path.splitext(file)[0]
                if dir_name == file_basename_no_ext:
                    json_files.append(os.path.join(root, file))
                    video_ids_processed +=1
                    if limit and video_ids_processed >= limit:
                        break
        if limit and video_ids_processed >= limit: # Verifica novamente para sair do os.walk
            break
            
    logger.info(f"Encontrados {len(json_files)} arquivos de metadados de transcrição em {input_dir}" + (f" (limitado a {limit})" if limit else ""))
    return json_files

def check_if_already_processed(db_client: Optional[Client], video_id: str) -> bool:
    """Verifica no Supabase se chunks para este video_id já existem."""
    if not db_client:
        logger.warning("Cliente Supabase não disponível, não é possível verificar processamento anterior. Assumindo não processado.")
        return False
    try:
        response = db_client.table("documents").select("document_id").eq("metadata->>video_id", video_id).eq("metadata->>source_type", NEW_SOURCE_TYPE).limit(1).execute()
        if response.data:
            logger.info(f"Chunks para video_id {video_id} (source_type: {NEW_SOURCE_TYPE}) já existem no banco de dados.")
            return True
        return False
    except Exception as e:
        logger.error(f"Erro ao verificar processamento anterior para video_id {video_id}: {e}")
        return False # Erra por segurança, permitindo o processamento

def process_single_transcription(
    metadata_filepath: str,
    db_client: Optional[Client],
    structure_analyzer: StructureAnalyzerAgent,
    force_reprocess: bool,
    dry_run: bool
):
    """Processa um único arquivo JSON de metadados de transcrição."""
    logger.info(f"Processando arquivo de metadados: {metadata_filepath}")
    try:
        with open(metadata_filepath, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    except Exception as e:
        logger.error(f"Falha ao ler ou parsear JSON de {metadata_filepath}: {e}")
        return False

    video_id = metadata.get("video_id")
    video_title = metadata.get("video_title", "Título Desconhecido")
    transcription_details = metadata.get("transcription_details")

    if not video_id or not transcription_details:
        logger.error(f"Metadados incompletos em {metadata_filepath} (video_id ou transcription_details ausente). Pulando.")
        return False

    logger.info(f"Iniciando reestruturação para video_id: {video_id} - Título: {video_title}")

    if not force_reprocess and check_if_already_processed(db_client, video_id):
        logger.info(f"Video_id {video_id} já processado e --force-reprocess não definido. Pulando.")
        return True # Considerado sucesso, pois já foi feito.

    if dry_run and not force_reprocess:
        # Em dry-run, se não for forçado, e já existe, também pulamos para simular comportamento real.
        # A função check_if_already_processed já loga, então não precisa logar de novo aqui.
        pass


    # --- 1. Formar Seções Iniciais a partir de `utterances` ou `words` ---
    initial_sections: List[IdentifiedSection] = []
    utterances = transcription_details.get("utterances", [])
    full_text_from_transcript = transcription_details.get("text", "")

    if utterances:
        logger.info(f"Usando {len(utterances)} utterances para formar seções iniciais.")
        for i, utt in enumerate(utterances):
            speaker = utt.get("speaker", "Desconhecido")
            text = utt.get("text", "").strip()
            if text:
                # Usar um section_type mais descritivo se possível, ex: `fala_speaker_{speaker}`
                # Ou deixar para o LLM classificar melhor.
                initial_sections.append(IdentifiedSection(section_type=f"fala_speaker_{speaker if speaker else 'X'}", content=text, metadata={"original_utterance_index": i}))
    elif transcription_details.get("words"):
        # Fallback: se não há utterances, tentar agrupar palavras (lógica simplificada)
        # Uma lógica mais sofisticada agruparia por pausas maiores.
        logger.warning(f"Não foram encontradas 'utterances'. Usando texto completo como uma única seção inicial. Aprimorar esta lógica se necessário.")
        if full_text_from_transcript:
             initial_sections.append(IdentifiedSection(section_type="corpo_completo_video", content=full_text_from_transcript))
    else:
        logger.error(f"Nenhum conteúdo (utterances ou words) encontrado em transcription_details para {video_id}. Pulando.")
        return False
    
    if not initial_sections:
        logger.error(f"Nenhuma seção inicial pôde ser formada para {video_id}. Pulando.")
        return False

    logger.debug(f"Seções iniciais formadas ({len(initial_sections)}): {initial_sections[:3]}...")

    # --- 2. (Opcional/Placeholder) Refinar com StructureAnalyzerAgent ---
    # O texto a ser analisado pelo LLM pode ser o full_text_from_transcript ou uma concatenação das initial_sections
    # Por enquanto, vamos assumir que o LLM analisaria o texto completo e tentaria aplicar o vocabulário.
    analyzed_sections: List[IdentifiedSection] = []
    if VOCABULARIES_TRANSCRIPTS.get(NEW_SOURCE_TYPE) and full_text_from_transcript:
        logger.info(f"Chamando StructureAnalyzerAgent para refinar estrutura para {video_id}...")
        try:
            # Passar um contexto relevante, se houver. Ex: título do vídeo.
            # context_for_llm = f"Título do Vídeo: {video_title}" # Removido pois não é usado
            analyzed_sections = structure_analyzer.analyze_structure(
                document_content=full_text_from_transcript, # Nome do parâmetro esperado pela classe real
                source_type=NEW_SOURCE_TYPE
                # O parâmetro 'context' não existe no método 'analyze_structure' da classe real.
                # A classe real já constrói seu próprio contexto/prompt.
            )
            logger.info(f"StructureAnalyzerAgent retornou {len(analyzed_sections)} seções analisadas.")
        except Exception as e:
            logger.error(f"Erro ao chamar StructureAnalyzerAgent para {video_id}: {e}. Usando seções iniciais.")
            analyzed_sections = initial_sections # Fallback para seções iniciais
    else:
        logger.info(f"Pulando StructureAnalyzerAgent para {video_id} (sem vocabulário ou texto). Usando seções iniciais.")
        analyzed_sections = initial_sections

    if not analyzed_sections: # Segurança extra
        logger.error(f"Nenhuma seção (inicial ou analisada) disponível após etapa LLM para {video_id}. Pulando.")
        return False
        
    logger.debug(f"Seções após análise LLM ({len(analyzed_sections)}): {analyzed_sections[:3]}...")

    # --- 3. Pós-processamento (fusão, divisão) ---
    logger.info(f"Aplicando pós-processamento em {len(analyzed_sections)} seções...")
    final_sections = _post_process_sections(
        sections=analyzed_sections, # Adicionando 'sections=' para clareza e consistência
        essential_short_types=ESSENTIAL_SHORT_TYPES_TRANSCRIPTS
        # min_length e target_size não são mais passadas diretamente aqui
    )
    logger.info(f"Pós-processamento resultou em {len(final_sections)} seções finais.")
    logger.debug(f"Seções finais ({len(final_sections)}): {final_sections[:3]}...")
    
    if not final_sections:
        logger.error(f"Nenhuma seção final após pós-processamento para {video_id}. Pulando.")
        return False

    # --- 4. Preparar e Inserir Chunks no Supabase ---
    new_chunks_for_db = []
    current_time_utc = datetime.now(timezone.utc)

    for idx, section in enumerate(final_sections):
        new_doc_id = str(uuid.uuid4())
        # Usar acesso por chave de dicionário
        section_content = section.get('content') # Usar .get para segurança se a chave puder faltar
        section_type = section.get('section_type')

        chunk_metadata = {
            "source_type": NEW_SOURCE_TYPE,
            "video_id": video_id, # ID original do YouTube
            "video_title": video_title,
            "original_document_id": video_id, # Usar video_id como o ID do "documento pai"
            "section_type": section_type, # Usar a variável obtida com .get()
            "section_index": idx, # Índice da seção dentro deste vídeo
            "created_at": current_time_utc.isoformat(),
            "processing_script_version": "restructure_video_transcripts_v0.1" # Exemplo de versionamento
        }
        # Adicionar quaisquer metadados específicos da seção, se houver
        # Acessar metadata da seção também com .get()
        section_meta = section.get('metadata') 
        if section_meta and isinstance(section_meta, dict):
            chunk_metadata.update(section_meta)

        new_chunk_payload = {
            "document_id": new_doc_id,
            "content": section_content, # Usar a variável obtida com .get()
            "embedding": None, # Embedding será gerado depois
            "metadata": chunk_metadata,
            "keep": True,
            "indexing_status": "pending"
        }
        new_chunks_for_db.append(new_chunk_payload)

    logger.info(f"Preparados {len(new_chunks_for_db)} novos chunks para video_id: {video_id}.")

    if dry_run:
        logger.info(f"[DRY-RUN] {len(new_chunks_for_db)} chunks seriam preparados para {video_id}.")
        for i, chunk in enumerate(new_chunks_for_db[:3]): # Logar os 3 primeiros como exemplo
            logger.info(f"  [DRY-RUN] Chunk {i+1}: id_proposto={chunk['document_id']}, type={chunk['metadata']['section_type']}, len={len(chunk['content'])}")
        return True

    if not db_client:
        logger.error("Cliente Supabase não disponível. Não é possível inserir chunks no banco de dados.")
        return False

    # Lógica de Deleção (se reprocessando) e Inserção
    if force_reprocess: # Implica que se chegou aqui, é para deletar os antigos e inserir novos
        logger.info(f"Modo --force-reprocess: Deletando chunks existentes para video_id {video_id} com source_type {NEW_SOURCE_TYPE}...")
        try:
            delete_response = db_client.table("documents").delete().eq("metadata->>video_id", video_id).eq("metadata->>source_type", NEW_SOURCE_TYPE).execute()
            logger.info(f"Deleção de chunks antigos para {video_id} concluída. Resposta: {delete_response.data if delete_response else 'N/A'}")
        except Exception as e:
            logger.error(f"Erro ao deletar chunks antigos para {video_id}: {e}. Prosseguindo com a inserção de qualquer maneira (pode causar duplicatas).")

    if new_chunks_for_db:
        try:
            # Inserir em lotes
            for i in range(0, len(new_chunks_for_db), DB_INSERT_BATCH_SIZE):
                batch = new_chunks_for_db[i:i + DB_INSERT_BATCH_SIZE]
                logger.info(f"Inserindo lote de {len(batch)} chunks no Supabase para video_id {video_id}...")
                try:
                    # Corrigir: returning="minimal" é um parâmetro de insert(), não um método option()
                    insert_response = db_client.table("documents").insert(batch, returning="minimal").execute()
                    # No modo minimal, insert_response.data provavelmente será vazio ou terá um formato diferente.
                    # Não vamos mais logar insert_response.data diretamente para evitar confusão.
                    logger.info(f"Inserção do lote para {video_id} no Supabase (aparentemente) bem-sucedida.")
                except APIError as e:
                    logger.error(f"Erro API ao inserir chunks no Supabase para video_id {video_id}: {e.message}")
                    # Logar mais detalhes do erro se disponíveis
                    if hasattr(e, 'json') and callable(e.json):
                        logger.error(f"Detalhes do erro JSON: {e.json()}")
                    return False # Falha na inserção do lote
                except Exception as e:
                    logger.error(f"Erro ao inserir chunks no Supabase para video_id {video_id}: {e}")
                    return False
            
            logger.info(f"Todos os {len(new_chunks_for_db)} chunks para {video_id} inseridos no Supabase.")
            return True
        except Exception as e: # Este except é para o bloco `if new_chunks_for_db` - parece um pouco deslocado.
            # Deveria ser `except APIError as e:` ou `except Exception as e:` no nível da tentativa de inserção.
            # O erro original que ocorria aqui era APIError.
            # Esta linha `except Exception as e:` provavelmente não será atingida como está, pois os erros de API são capturados acima.
            # Vou remover este `exc_info=True` redundante e ajustar o log se o erro persistir de forma diferente.
            logger.error(f"Erro ao inserir chunks no Supabase para video_id {video_id}: {e}")
            return False
    else:
        logger.info(f"Nenhum novo chunk para inserir para video_id: {video_id}.")
        return True # Considerado sucesso se não havia nada a fazer.


def main():
    parser = argparse.ArgumentParser(description="Reestrutura transcrições de vídeo do YouTube em chunks semânticos.")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR,
                        help=f"Diretório contendo os subdiretórios {METADATA_DIR_NAME} com os arquivos JSON das transcrições (padrão: {DEFAULT_INPUT_DIR}).")
    parser.add_argument("--log-level", default="INFO", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Nível de logging (padrão: INFO).")
    parser.add_argument("--force-reprocess", action="store_true",
                        help="Forçar o reprocessamento de transcrições que já foram ingeridas.")
    parser.add_argument("--limit", type=int, help="Limitar o número de transcrições a processar (para teste).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Executar o script em modo de simulação, sem gravar no banco de dados.")

    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level.upper())
    logger.info(f"Nível de log configurado para: {args.log_level.upper()}")
    if args.dry_run:
        logger.info("Executando em modo DRY-RUN. Nenhuma alteração será feita no banco de dados.")
    if args.force_reprocess:
        logger.info("Modo FORCE-REPROCESS ativo.")
    if args.limit:
        logger.info(f"Processando no máximo {args.limit} transcrições.")


    # Inicializar o StructureAnalyzerAgent
    structure_analyzer = StructureAnalyzerAgent()

    # Encontrar arquivos de metadados
    # A lógica para actual_metadata_dir foi simplificada, assumindo que args.input_dir é o diretório dos metadados.
    actual_metadata_dir = args.input_dir
    if not os.path.isdir(actual_metadata_dir):
        logger.error(f"O diretório de metadados especificado ({actual_metadata_dir}) não existe ou não é um diretório. Encerrando.")
        return

    metadata_files = find_transcription_metadata_files(actual_metadata_dir, args.limit)

    if not metadata_files:
        logger.info(f"Nenhum arquivo de metadados encontrado em {actual_metadata_dir}. Encerrando.")
        return

    total_files = len(metadata_files)
    success_count = 0
    fail_count = 0

    for i, filepath in enumerate(metadata_files):
        logger.info(f"--- Processando arquivo {i+1}/{total_files}: {filepath} ---")
        try:
            if process_single_transcription(filepath, supabase_client, structure_analyzer, args.force_reprocess, args.dry_run):
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            logger.error(f"Erro fatal não tratado ao processar {filepath}: {e}", exc_info=True)
            fail_count +=1
        logger.info(f"--- Concluído arquivo {i+1}/{total_files} ---")


    logger.info("=================================================================")
    logger.info(f"Processamento de reestruturação de transcrições concluído.")
    logger.info(f"Total de arquivos de metadados encontrados: {total_files}")
    logger.info(f"Processados com sucesso: {success_count}")
    logger.info(f"Falhas no processamento: {fail_count}")
    logger.info("=================================================================")


if __name__ == "__main__":
    main()