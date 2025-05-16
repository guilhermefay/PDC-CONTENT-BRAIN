import os
import logging
import argparse
import uuid
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
import re

from supabase import create_client, Client
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import litellm

# Importar a função de pós-processamento do novo local
from scripts.utils.processing_logic import _post_process_sections

# Tentar importar o agente
try:
    from agents.structure_analyzer_agent import StructureAnalyzerAgent, IdentifiedSection, DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX
except ImportError as e:
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.error(f"Falha ao importar StructureAnalyzerAgent: {e}. Certifique-se de que 'agents' está no PYTHONPATH.")
    # Define uma classe dummy para permitir que o script carregue, mas falhe na execução.
    class IdentifiedSection(dict): pass
    class StructureAnalyzerAgent:
        def __init__(self, *args, **kwargs):
            raise ImportError("StructureAnalyzerAgent não pôde ser importado corretamente.")
        def analyze_structure(self, *args, **kwargs) -> List[IdentifiedSection]:
            raise NotImplementedError("StructureAnalyzerAgent não está disponível.")
    DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX = "_corpo_geral"
    # sys.exit(1) # Considerar sair se o agente for crucial

# --- Configuração de Logging ---
LOG_FILE_PATH = 'scripts/restructure_chunks.log'
# Remover handlers existentes para evitar duplicação
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Configuração básica inicial, será ajustada pelo argumento --log-level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuração Supabase ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.error("Erro: SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem ser definidos no arquivo .env")
    exit(1)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Cliente Supabase inicializado com sucesso.")
except Exception as e:
    logger.error(f"Erro ao inicializar cliente Supabase: {e}")
    exit(1)

# --- Constantes ---
DEFAULT_SOURCE_TYPE = 'email'
CHUNK_FETCH_BATCH_SIZE = 500 # Para buscar chunks de um doc original
DB_INSERT_BATCH_SIZE = 100 # Para inserir novos chunks (ajustar conforme limites da API)
DB_UPDATE_BATCH_SIZE = 100 # Para marcar chunks antigos (ajustar conforme limites da API)
MIN_SECTION_LENGTH = 600 # Limite mínimo de caracteres para não fundir (ajustado para chunks maiores)
ESSENTIAL_SHORT_TYPES = { # Tipos que podem ser curtos e não devem ser fundidos automaticamente
    'email_assunto', 'email_ps', 'email_saudacao', 'email_assinatura'
}
TARGET_CHUNK_SIZE = 1800 # Alvo de caracteres para chunks do corpo (AUMENTADO)
OVERSIZE_MULTIPLIER = 1.5 # Multiplicador para considerar um parágrafo/linha muito grande

# Vocabulário de Tipos de Seção por Source Type
# Adicionar/Ajustar conforme novos tipos de documentos são processados
VOCABULARIES = {
    'email': [
        "assunto", "saudacao", "gancho_problema", "historia_conexao",
        "apresentacao_oferta", "detalhamento_oferta", "beneficios",
        "prova_social", "garantia", "cta_principal", "cta_secundario",
        "bonus_urgencia", "fechamento_reforco", "assinatura", "ps"
    ],
     'roteiro_video_youtube': [ # NOVO VOCABULÁRIO
        "titulo", "introducao", "gancho_visual_ou_verbal", "apresentacao_tema",
        "segmento_principal", "ponto_chave", "exemplo_demonstracao", "transicao",
        "recapitulacao_resumo", "conclusao", "cta", "informacao_adicional",
        "vinheta_encerramento"
    ]
    # Adicionar outros source_types e seus vocabulários aqui
}

# --- Regex Patterns (Iniciais - Podem precisar de ajuste) ---
# Assunto: Linha começando com "Assunto:" (case-insensitive)
SUBJECT_PATTERN = re.compile(r"^\s*Assunto:(.*)$", re.IGNORECASE | re.MULTILINE)
# Saudação: Linhas iniciais que parecem uma saudação (Olá, Oi, Prezado, etc.)
GREETING_PATTERN = re.compile(r"^((Oi|Olá|Ola|Prezad[oa]s?)[^\n]*\n)+?", re.IGNORECASE | re.MULTILINE)
# Assinatura: Linhas finais ANTES de um possível PS, começando com palavras de despedida comuns.
# Deve ser conservador para não pegar frases de fechamento genéricas.
SIGNATURE_PATTERN = re.compile(
    r"^\s*("
    r"Atenciosamente|Cordialmente|Respeitosamente|Abra[çc]os|Abraço|"
    r"Beijo[s]?|Grato|Grata|Obrigado|Obrigada|Com[ ]carinho|"
    r"Valeu|Falou|Att\\.?|Sds\\.?"
    r")"
    r"(?:[.,\s]|\s*\n(?!(?:P\\.?S\\.?[:>]|\s*\S)).*)*$", # Permite vírgula, espaços, ou novas linhas (com conteúdo) que não sejam um PS
    re.IGNORECASE | re.MULTILINE
)
# Padrão de teste simplificado - Removido
# SIGNATURE_PATTERN_TEST = re.compile(r"^\s*Beijos,?\s*$", re.IGNORECASE | re.MULTILINE)
# logger.warning("USANDO SIGNATURE_PATTERN DE TESTE SIMPLIFICADO!")

# PS: Linhas começando com P.S. ou PS:
PS_PATTERN = re.compile(r"^(P\\.?S\\.?[:>])(.*)", re.IGNORECASE | re.MULTILINE | re.DOTALL)

# --- Funções Auxiliares Regex ---
def extract_regex_section(pattern: re.Pattern, text: str) -> Tuple[Optional[str], str]:
    """Tenta extrair a primeira ocorrência de um padrão e retorna a seção e o texto restante."""
    match = pattern.search(text)
    if match:
        section_content = match.group(0).strip()
        remaining_text = text[:match.start()] + text[match.end():] # Remove a seção do texto
        return section_content, remaining_text.strip()
    return None, text

def extract_regex_section_end(pattern: re.Pattern, text: str) -> Tuple[Optional[str], str]:
    """Tenta extrair a última ocorrência de um padrão (útil para assinatura/ps) e retorna a seção e o texto restante."""
    last_match = None
    for match in pattern.finditer(text):
        last_match = match
    
    if last_match:
        # --- LÓGICA CORRIGIDA --- 
        match_start = last_match.start()
        match_end = last_match.end()
        # logger.debug(f"extract_regex_section_end: Pattern {pattern.pattern} found match from index {match_start} to {match_end}.")

        # Extrai a seção correspondente
        section_content = text[match_start:match_end].strip() 
        # logger.debug(f"extract_regex_section_end: Pattern {pattern.pattern} section_content after strip ({len(section_content)} chars): '{section_content[:200]}...'")

        # Constrói o texto restante concatenando antes e depois
        text_before = text[:match_start]
        text_after = text[match_end:]
        remaining_text = (text_before + text_after).strip() 
        # logger.debug(f"extract_regex_section_end: Pattern {pattern.pattern} remaining_text after extraction ({len(remaining_text)} chars):\n'{remaining_text[:500]}...'")
        # --- FIM DA LÓGICA CORRIGIDA ---
        
        # Se for PS, pode pegar só o conteúdo após 'PS:' (mantém lógica original aqui)
        if pattern == PS_PATTERN and len(last_match.groups()) > 1:
             section_content = last_match.group(2).strip() # Pega o conteúdo após P.S.
             # logger.debug(f"extract_regex_section_end: PS_PATTERN adjusted section_content: '{section_content[:200]}...'")
        return section_content, remaining_text
    return None, text

def get_distinct_gdrive_ids(supabase_client: Client, source_type: str, limit: Optional[int] = None) -> List[str]:
    """
    Busca gdrive_ids distintos da tabela 'documents' para um determinado source_type,
    considerando apenas chunks ativos e não obsoletos.
    Aplica um limite se fornecido AO RESULTADO FINAL de IDs distintos.
    A query inicial ao Supabase busca um lote fixo para ter mais chance de encontrar IDs distintos.
    """
    logger.info(f"Buscando gdrive_ids distintos para source_type='{source_type}'...")

    try:
        # A query busca um lote de CHUNK_FETCH_BATCH_SIZE para ter uma boa amostra
        # O argumento 'limit' da função será aplicado depois, à lista de IDs distintos.
        response = supabase_client.table('documents') \
                         .select('metadata->>gdrive_id') \
                         .eq('metadata->>source_type', source_type) \
                         .eq('keep', True) \
                         .neq('metadata->>gdrive_id', 'null') \
                         .range(0, CHUNK_FETCH_BATCH_SIZE - 1) \
                         .execute()
        
        if hasattr(response, 'data') and response.data:
            # Coleta todos os gdrive_ids distintos do lote buscado
            distinct_ids_from_batch = sorted(list({item['gdrive_id'] for item in response.data if item.get('gdrive_id')}))
            logger.debug(f"Busca gdrive_id: {len(response.data)} chunks no lote, {len(distinct_ids_from_batch)} IDs distintos encontrados no lote.")
            
            if limit and limit > 0: # Aplicar o limite se for especificado e válido
                logger.info(f"Aplicando limite de {limit} aos {len(distinct_ids_from_batch)} gdrive_ids distintos encontrados.")
                return distinct_ids_from_batch[:limit]
            else: # Se não houver limite, ou limite for 0 ou None, retorna todos os distintos encontrados no lote
                return distinct_ids_from_batch
        else:
            if hasattr(response, 'error') and response.error:
               logger.error(f"Erro Supabase ao buscar gdrive_ids: {response.error}")
            return []
    except Exception as e:
        logger.error(f"Exceção ao buscar gdrive_ids: {e}", exc_info=True)
        return []

def reconstruct_document_content(supabase_client: Client, gdrive_id: str) -> Tuple[Optional[str], List[str], Optional[Dict]]:
    """
    Busca todos os chunks de um gdrive_id e tenta reconstruir o conteúdo original.
    Retorna o conteúdo concatenado, a lista dos IDs dos chunks antigos e os metadados do primeiro chunk.
    """
    logger.debug(f"Reconstruindo conteúdo para gdrive_id: {gdrive_id} buscando apenas chunks ATIVOS")
    all_chunks = []
    offset = 0
    while True:
        try:
            query = supabase_client.table('documents') \
                             .select('document_id, content, metadata') \
                             .eq('metadata->>gdrive_id', gdrive_id) \
                             .eq('keep', True)
            
            # Adicionando condições para excluir status obsoletos
            query = query.neq('indexing_status', 'obsolete_restructured')
            query = query.neq('indexing_status', 'obsolete_manual_revert')
            
            response = query.range(offset, offset + CHUNK_FETCH_BATCH_SIZE - 1).execute()
            
            if not hasattr(response, 'data') or not response.data:
                if hasattr(response, 'error') and response.error:
                    logger.error(f"Erro ao buscar chunks para gdrive_id {gdrive_id} (offset {offset}): {response.error}")
                break # Sai se não houver dados ou erro

            all_chunks.extend(response.data)
            if len(response.data) < CHUNK_FETCH_BATCH_SIZE:
                break # Último lote
            offset += CHUNK_FETCH_BATCH_SIZE
        except Exception as e:
            logger.error(f"Exceção ao buscar chunks para gdrive_id {gdrive_id} (offset {offset}): {e}")
            return None, [], None

    if not all_chunks:
        logger.warning(f"Nenhum chunk encontrado para gdrive_id: {gdrive_id}")
        return None, [], None

    # --- Lógica de Ordenação CRÍTICA ---
    try:
        def get_chunk_index(chunk):
            idx = chunk.get('metadata', {}).get('chunk_index')
            # Tenta converter para int, mesmo que seja string ou float
            try: return int(idx) if idx is not None else float('inf')
            except (ValueError, TypeError): return float('inf')

        all_chunks.sort(key=get_chunk_index)
        if get_chunk_index(all_chunks[-1]) == float('inf') and len(all_chunks) > 1:
            logger.warning(f"Ordenação incerta para gdrive_id {gdrive_id}: nem todos os chunks tinham chunk_index numérico válido.")
        logger.debug(f"Chunks ordenados por metadata.chunk_index para gdrive_id: {gdrive_id}")
    except Exception as e_sort:
        logger.warning(f"Erro ao tentar ordenar chunks para gdrive_id {gdrive_id}: {e_sort}. A ordem pode estar incorreta.")

    full_content = "\n\n".join([chunk['content'] for chunk in all_chunks if chunk.get('content')])
    old_chunk_ids = [chunk['document_id'] for chunk in all_chunks if chunk.get('document_id')]
    base_metadata = all_chunks[0].get('metadata', {}) if all_chunks else {}

    logger.debug(f"Conteúdo reconstruído para {gdrive_id} (tamanho: {len(full_content)}). IDs antigos: {len(old_chunk_ids)}")
    return full_content, old_chunk_ids, base_metadata

def _chunk_body_text(text: str, target_size: int) -> List[Dict]:
    """Divide o texto do corpo em chunks de tamanho alvo, lidando com parágrafos longos."""
    initial_paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
    if not initial_paragraphs:
        return []

    processed_segments = [] # Lista para armazenar linhas/segmentos menores após processar parágrafos longos
    for paragraph in initial_paragraphs:
        # Se o parágrafo já é razoavelmente dimensionado, adiciona-o diretamente
        if len(paragraph) <= target_size * OVERSIZE_MULTIPLIER:
            processed_segments.append(paragraph)
        else:
            # Tenta dividir o parágrafo longo por quebras de linha simples
            sub_lines = [line.strip() for line in paragraph.split('\n') if line.strip()]
            if not sub_lines:
                # Se não houver sublinhas (caso estranho), ainda adiciona o parágrafo original
                processed_segments.append(paragraph) 
                continue
            
            for line in sub_lines:
                # Se a sub-linha ainda for muito longa, quebra por caractere
                if len(line) > target_size * OVERSIZE_MULTIPLIER:
                    # Quebra forçada - pode quebrar no meio de palavras/frases
                    start = 0
                    while start < len(line):
                        end = min(start + target_size, len(line))
                        processed_segments.append(line[start:end])
                        start = end
                else:
                    # Adiciona a sub-linha como está
                    processed_segments.append(line)

    # Agora, agrupa os processed_segments em chunks
    chunks = []
    current_chunk_content = ""
    current_chunk_len = 0

    for i, segment in enumerate(processed_segments):
        segment_len = len(segment)
        
        # Se o chunk atual está vazio, adiciona o primeiro segmento
        if not current_chunk_content:
            current_chunk_content = segment
            current_chunk_len = segment_len
            continue

        # Calcula o tamanho potencial se adicionarmos o próximo segmento
        # Usa '\n\n' como separador preferencial se possível, senão '\n'
        separator = "\n\n"
        potential_next_len = current_chunk_len + len(separator) + segment_len

        # Se adicionar o próximo segmento não estourar muito o limite, adiciona
        # Ou se é o último segmento (adiciona ao chunk atual de qualquer forma)
        if potential_next_len <= target_size * OVERSIZE_MULTIPLIER or i == len(processed_segments) - 1:
            current_chunk_content += separator + segment
            current_chunk_len = len(current_chunk_content) # Recalcula o tamanho exato
        else:
            # Finaliza o chunk atual e começa um novo com o segmento atual
            chunks.append({"content": current_chunk_content, "section_type": "email_corpo_principal"})
            current_chunk_content = segment
            current_chunk_len = segment_len

    # Adiciona o último chunk restante
    if current_chunk_content:
        chunks.append({"content": current_chunk_content, "section_type": "email_corpo_principal"})

    # Log para diagnóstico
    logger.debug(f"_chunk_body_text: Texto original ({len(text)} chars) dividido em {len(chunks)} chunks.")
    
    return chunks

def mark_old_chunks_as_obsolete(
    supabase_client: Client, 
    new_status: str,
    gdrive_id: Optional[str] = None, 
    source_type_to_match: Optional[str] = None,
    specific_chunk_ids: Optional[List[str]] = None
) -> bool:
    """
    Marca chunks como obsoletos.
    - Se specific_chunk_ids for fornecido, opera APENAS nesses IDs.
    - Senão, opera em todos os chunks para o gdrive_id e source_type fornecidos (se ambos estiverem presentes).
    Define 'keep' como False e 'indexing_status' com o valor de new_status.
    Retorna True se bem-sucedido ou se nenhum chunk precisar ser atualizado, False se ocorrer um erro.
    """
    if not specific_chunk_ids and not (gdrive_id and source_type_to_match):
        logger.error("Deve ser fornecido specific_chunk_ids OU (gdrive_id e source_type_to_match) para mark_old_chunks_as_obsolete.")
        return False

    log_message_identifier = ""
    if specific_chunk_ids:
        log_message_identifier = f"IDs específicos (total: {len(specific_chunk_ids)})"
        # Para não logar todos os IDs se a lista for muito grande
        if len(specific_chunk_ids) > 3:
            log_message_identifier += f": [{', '.join(specific_chunk_ids[:3])}, ...]"
        else:
            log_message_identifier += f": {specific_chunk_ids}"
    elif gdrive_id and source_type_to_match: # gdrive_id e source_type_to_match devem estar presentes
        log_message_identifier = f"gdrive_id: {gdrive_id}, source_type: {source_type_to_match}"
    else: # Caso de fallback, embora a checagem inicial deva pegar isso
        logger.error("Argumentos insuficientes para identificar chunks em mark_old_chunks_as_obsolete.")
        return False

    logger.info(f"Marcando chunks para {log_message_identifier} com status: {new_status}")

    try:
        update_query = supabase_client.table("documents")\
            .update({"keep": False, "indexing_status": new_status})

        if specific_chunk_ids:
            # Se estamos atualizando IDs específicos, não precisamos da lógica de .neq("indexing_status", "obsolete_restructured")
            # pois esta chamada é para definir explicitamente o status desses IDs (geralmente para 'obsolete_restructured')
            update_query = update_query.in_("document_id", specific_chunk_ids)
        elif gdrive_id and source_type_to_match: 
            update_query = update_query.match({"metadata->>gdrive_id": gdrive_id, "metadata->>source_type": source_type_to_match})
            # A lógica de não sobrescrever 'obsolete_restructured' SÓ se aplica se estamos no modo gdrive_id/source_type
            # E o novo status é 'obsolete_forced_reprocess'.
            if new_status == "obsolete_forced_reprocess":
                update_query = update_query.neq("indexing_status", "obsolete_restructured")
        else:
            # Esta condição não deveria ser alcançada devido à verificação no início da função.
            logger.error("Condição de match não atendida em mark_old_chunks_as_obsolete.")
            return False
            
        update_result = update_query.execute()

        if hasattr(update_result, 'error') and update_result.error:
            logger.error(f"Erro ao marcar chunks como obsoletos ({new_status}) para {log_message_identifier}: {update_result.error}")
            return False
        
        logger.info(f"Chunks para {log_message_identifier} (se aplicável) foram marcados com status '{new_status}'.")
        return True
    except Exception as e:
        logger.error(f"Exceção ao marcar chunks como obsoletos ({new_status}) para {log_message_identifier}: {e}", exc_info=True)
        return False

def process_documents(supabase_client: Client, agent: StructureAnalyzerAgent, source_type: str, limit: Optional[int] = None, dry_run: bool = True, force_reprocess: bool = False):
    logger.info(f"Iniciando processamento para source_type: '{source_type}', Dry run: {dry_run}, Force reprocess: {force_reprocess}")
    gdrive_ids = get_distinct_gdrive_ids(supabase_client, source_type, limit)

    if not gdrive_ids:
        logger.info(f"Nenhum gdrive_id encontrado para processar para source_type '{source_type}'.")
        return

    logger.info(f"Encontrados {len(gdrive_ids)} gdrive_ids para processar: {gdrive_ids}")
    processed_count = 0
    successful_count = 0

    for gdrive_id in gdrive_ids:
        processed_count += 1
        logger.info(f"Processando gdrive_id {processed_count}/{len(gdrive_ids)}: {gdrive_id}")

        # Lógica para buscar e reconstruir conteúdo original
        original_content, old_chunk_ids, base_metadata = reconstruct_document_content(supabase_client, gdrive_id)

        if original_content is None:
            logger.warning(f"Conteúdo não pôde ser reconstruído para gdrive_id: {gdrive_id}. Pulando.")
            continue
        
        if not base_metadata: # Garante que temos metadados base
            logger.warning(f"Metadados base não encontrados para gdrive_id: {gdrive_id} após reconstrução. Pulando.")
            continue

        # Garantir que estamos usando o source_type correto dos metadados do documento original,
        # em vez do parâmetro source_type da função, para o caso de haver alguma inconsistência.
        # No entanto, a busca inicial de gdrive_ids já filtra por source_type, então deve ser consistente.
        actual_source_type = base_metadata.get('source_type', source_type)
        if actual_source_type != source_type:
            logger.warning(f"Inconsistência de source_type para gdrive_id {gdrive_id}: esperado '{source_type}', encontrado nos metadados '{actual_source_type}'. Usando '{actual_source_type}'.")
        
        # Lógica de reprocessamento forçado
        if force_reprocess and not dry_run:
            if old_chunk_ids: # Apenas tentar marcar se existirem chunks antigos identificados
                logger.info(f"Force reprocess habilitado. Tentando marcar chunks antigos como obsoletos para gdrive_id: {gdrive_id}")
                # Chamada para marcar os chunks do gdrive_id/source_type como 'obsolete_forced_reprocess'
                if not mark_old_chunks_as_obsolete(
                    supabase_client, 
                    new_status="obsolete_forced_reprocess",
                    gdrive_id=gdrive_id, 
                    source_type_to_match=actual_source_type
                ):
                    logger.error(f"Falha ao marcar chunks antigos (force_reprocess) para {gdrive_id}. O reprocessamento pode resultar em duplicatas. Continuando com cautela.")
            else:
                logger.info(f"Force reprocess habilitado, mas nenhum chunk antigo identificado por reconstruct_document_content para {gdrive_id}. Nada a marcar inicialmente.")

        # Análise de Estrutura com LLM (agente)
        logger.info(f"Enviando conteúdo (aprox {len(original_content)} chars) para StructureAnalyzerAgent para gdrive_id: {gdrive_id}")
        try:
            analyzed_sections = agent.analyze_structure(original_content, actual_source_type)
        except Exception as e_agent:
            logger.error(f"Erro ao analisar estrutura com o agente para gdrive_id {gdrive_id}: {e_agent}", exc_info=True)
            continue # Pula para o próximo gdrive_id

        if not analyzed_sections:
            logger.warning(f"Nenhuma seção analisada retornada pelo agente para gdrive_id: {gdrive_id}. Pulando.")
            continue
        
        logger.info(f"Agente retornou {len(analyzed_sections)} seções analisadas para gdrive_id: {gdrive_id}")

        # Pós-processamento das seções (fusão, etc.)
        # Passar o vocabulário específico para ESSENTIAL_SHORT_TYPES
        current_essential_short_types = ESSENTIAL_SHORT_TYPES # Usar o global por enquanto, pode ser refinado por source_type
        
        # Corrigir a chamada para corresponder à assinatura da função importada:
        # _post_process_sections(sections, min_length, essential_short_types, default_fallback_suffix)
        # O default_fallback_suffix é pego da constante no módulo utils, não precisa ser passado aqui.
        # actual_source_type não é um parâmetro de _post_process_sections.
        final_sections = _post_process_sections(
            analyzed_sections,
            MIN_SECTION_LENGTH, 
            current_essential_short_types
            # DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX não é mais passado aqui, pois é default na função
        )
        logger.info(f"{len(final_sections)} seções finais após pós-processamento para gdrive_id: {gdrive_id}")

        # Preparar novos chunks para o banco de dados
        new_chunks_for_db = []
        current_time_utc = datetime.now(timezone.utc)

        for idx, section_data in enumerate(final_sections):
            new_doc_id = str(uuid.uuid4())
            section_content = section_data.get('content')
            section_type_from_agent = section_data.get('section_type')

            # Garantir que section_type_from_agent nunca seja None ou vazio
            if not section_type_from_agent:
                logger.warning(f"section_type_from_agent era '{section_type_from_agent}' para gdrive_id {gdrive_id}, seção {idx}. Usando fallback.")
                section_type_from_agent = f"{actual_source_type}{DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX}"

            if not section_content: # Apenas checar o conteúdo aqui, pois o tipo já foi tratado
                logger.warning(f"Seção {idx} para gdrive_id {gdrive_id} está sem conteúdo após pós-processamento. Pulando esta seção.")
                continue

            # Usar metadados base do documento original e adicionar/sobrescrever específicos do chunk
            chunk_metadata = base_metadata.copy() # Começa com uma cópia dos metadados do doc original
            chunk_metadata.update({
                'chunk_index': idx,
                'total_chunks_in_doc': len(final_sections),
                'section_type': section_type_from_agent, # Agora garantido que não é None/vazio
                'original_document_id': gdrive_id, # ID original do gdrive/documento pai
                'restructure_approach': 'hybrid_strong',
                # 'gdrive_id' já está em base_metadata,
                # 'source_type' já está em base_metadata,
                # outros metadados originais são mantidos...
            })
            # Remover chaves que não queremos duplicar ou que não fazem sentido no nível do chunk se vieram do doc original
            chunk_metadata.pop('chunk_index_original', None) # Exemplo de limpeza, ajustar conforme necessário

            new_chunk = {
                'document_id': new_doc_id,
                'content': section_content,
                'metadata': chunk_metadata,
                'embedding': None, # Embedding será gerado por outro processo
                'keep': True,
                'indexing_status': 'structured_new',
                'created_at': current_time_utc.isoformat(),
                'updated_at': current_time_utc.isoformat()
            }
            new_chunks_for_db.append(new_chunk)

        if not new_chunks_for_db:
            logger.warning(f"Nenhum novo chunk foi preparado para inserção para gdrive_id: {gdrive_id} após processamento completo. Pulando inserção.")
            continue

        # Inserir novos chunks no banco de dados
        if not dry_run:
            logger.info(f"Inserindo {len(new_chunks_for_db)} novos chunks para gdrive_id: {gdrive_id}")
            all_inserted_successfully = True
            for i in range(0, len(new_chunks_for_db), DB_INSERT_BATCH_SIZE):
                batch = new_chunks_for_db[i:i + DB_INSERT_BATCH_SIZE]
                try:
                    response = supabase_client.table('documents').insert(batch, returning="minimal").execute()
                    if hasattr(response, 'error') and response.error:
                        logger.error(f"Erro Supabase ao inserir lote de chunks para gdrive_id {gdrive_id}: {response.error}")
                        all_inserted_successfully = False
                        break # Interrompe a inserção para este gdrive_id se um lote falhar
                except Exception as e:
                    logger.error(f"Exceção ao inserir lote de chunks para gdrive_id {gdrive_id}: {e}", exc_info=True)
                    all_inserted_successfully = False
                    break
            
            if all_inserted_successfully:
                logger.info(f"Todos os {len(new_chunks_for_db)} chunks para gdrive_id {gdrive_id} inseridos com sucesso.")
                successful_count += 1
                
                if old_chunk_ids: 
                    logger.info(f"Tentando marcar os {len(old_chunk_ids)} chunks antigos específicos como 'obsolete_restructured' para gdrive_id: {gdrive_id} após inserção bem-sucedida dos novos.")
                    if not mark_old_chunks_as_obsolete(
                        supabase_client, 
                        new_status="obsolete_restructured",
                        specific_chunk_ids=old_chunk_ids # Passa a lista de IDs dos chunks originais
                    ):
                        logger.warning(f"Falha ao marcar os chunks antigos específicos ({len(old_chunk_ids)} IDs) como 'obsolete_restructured' para {gdrive_id} após inserção bem-sucedida. Dados podem estar inconsistentes.")
                else:
                    logger.info(f"Nenhum chunk antigo (old_chunk_ids) foi identificado por reconstruct_document_content para {gdrive_id}. Nada a marcar como 'obsolete_restructured'.")
            else:
                logger.error(f"Falha ao inserir um ou mais lotes de chunks para gdrive_id {gdrive_id}. Verifique os logs. Alguns chunks podem ter sido inseridos.")

        else: # dry_run
            logger.info(f"[DRY RUN] {len(new_chunks_for_db)} chunks seriam preparados para gdrive_id: {gdrive_id}")
            for chunk_to_insert in new_chunks_for_db:
                logger.debug(f"[DRY RUN] Chunk a ser inserido: ID={chunk_to_insert['document_id']}, Tipo={chunk_to_insert['metadata'].get('section_type')}, Tamanho={len(chunk_to_insert['content'])}")
            successful_count +=1 # Em dry_run, contamos como sucesso para fins de relatório de processamento.

    logger.info(f"Processamento concluído para source_type '{source_type}'. Total de gdrive_ids: {len(gdrive_ids)}, Processados: {processed_count}, Bem-sucedidos (ou seria em dry-run): {successful_count}.")

def main():
    parser = argparse.ArgumentParser(description="Reestrutura documentos do Supabase em chunks semânticos usando LLM.")
    parser.add_argument("--source-type", default=DEFAULT_SOURCE_TYPE, help=f"O tipo de source dos documentos a processar (padrão: {DEFAULT_SOURCE_TYPE}).")
    parser.add_argument("--limit", type=int, help="Limitar o número de gdrive_ids (documentos pai) a processar.")
    parser.add_argument("--dry-run", action="store_true", help="Executar o script em modo de simulação, sem fazer alterações no banco de dados.")
    # Adicionar --no-dry-run para explicitamente desativar dry_run se --dry-run não for o padrão.
    # No entanto, argparse lida bem com action='store_true' (padrão False).
    # Se quiséssemos que dry_run fosse True por padrão, poderíamos usar action='store_false' para --no-dry-run.
    # parser.add_argument("--no-dry-run", action="store_false", dest="dry_run", help="Desativa o modo dry-run (faz alterações reais).")
    
    # Novo argumento: --force-reprocess
    parser.add_argument("--force-reprocess", action="store_true", help="Força o reprocessamento de documentos mesmo que já tenham sido processados, marcando chunks antigos como obsoletos.")

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help="Nível de logging (padrão: INFO)."
    )
    args = parser.parse_args()

    # Ajustar nível de logging com base no argumento
    numeric_log_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_log_level, int):
        raise ValueError(f'Nível de log inválido: {args.log_level}')
    
    # Reconfigurar handlers para o novo nível
    # Limpar handlers existentes antes de adicionar novos para evitar duplicação se main() for chamada múltiplas vezes
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        level=numeric_log_level,
        format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE_PATH, mode='w'), # Sempre recria o arquivo de log
            logging.StreamHandler()
        ]
    )
    logger.info(f"Nível de logging configurado para: {args.log_level}")


    # Verificar se o agente pode ser inicializado (a importação pode ter falhado antes)
    try:
        agent = StructureAnalyzerAgent() # Usa o LLM padrão configurado no agente
    except ImportError: # Se a classe dummy foi usada devido à falha na importação inicial
        logger.critical("StructureAnalyzerAgent não pôde ser inicializado devido a falha na importação de dependências (CrewAI/Langchain). O script não pode continuar.")
        return # Ou sys.exit(1)

    process_documents(supabase, agent, args.source_type, args.limit, args.dry_run, args.force_reprocess)

if __name__ == "__main__":
    main() 