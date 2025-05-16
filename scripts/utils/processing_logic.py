import logging
from typing import List, Dict, Optional, Set

# Tentativa de importar IdentifiedSection do local esperado
# Se esta importação falhar nos scripts que usam este utilitário,
# eles precisarão garantir que o PYTHONPATH esteja configurado corretamente
# ou que a estrutura de pastas permita a importação.
try:
    from agents.structure_analyzer_agent import IdentifiedSection
except ImportError:
    # Fallback para uma definição dummy se a importação falhar
    # Isso permite que o módulo utilitário seja carregado, mas os scripts
    # que o utilizam precisarão da definição real de IdentifiedSection.
    logging.error("Falha ao importar IdentifiedSection de agents.structure_analyzer_agent no módulo utils. Usando dummy.")
    class IdentifiedSection(dict): # Ou uma dataclass simples
        def __init__(self, section_type: str, content: str, **kwargs):
            super().__init__(section_type=section_type, content=content, **kwargs)
            # Para compatibilidade com o acesso por atributo usado na função original
            self.section_type = section_type
            self.content = content
            self.metadata = kwargs.get('metadata', {})

        def __repr__(self):
            return f"DummyIdentifiedSection(type='{self.get("section_type")}', len='{len(self.get("content", ""))}')"


logger = logging.getLogger(__name__)

# Constantes movidas de restructure_chunks.py
MIN_SECTION_LENGTH = 600  # Limite mínimo de caracteres para não fundir
DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX = "_corpo_geral" # Sufixo para fallback

# NOTA: TARGET_CHUNK_SIZE e OVERSIZE_MULTIPLIER são mais específicos para a lógica 
# de _chunk_body_text, que não está sendo movida para cá neste momento.
# ESSENTIAL_SHORT_TYPES será passado como parâmetro.

def _post_process_sections(
    sections: List[IdentifiedSection],
    min_length: int = MIN_SECTION_LENGTH, # Usa o default do módulo, mas pode ser sobrescrito
    essential_short_types: Optional[Set[str]] = None,
    default_fallback_suffix: str = DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX
) -> List[IdentifiedSection]:
    """
    Aplica regras de pós-processamento para fundir seções granulares.
    1. Fundir seções curtas (exceto tipos essenciais) com a vizinha.
    2. Fundir seções consecutivas do mesmo tipo (exceto fallback).

    Args:
        sections: Lista de IdentifiedSection a processar.
        min_length: Comprimento mínimo para uma seção não ser considerada curta.
        essential_short_types: Set de section_types que podem ser curtos e não devem ser fundidos.
        default_fallback_suffix: Sufixo usado para identificar tipos de seção de fallback.
    """
    if not sections:
        return []

    if essential_short_types is None:
        essential_short_types = set()

    processed_sections = sections

    # --- PASSO 1: Fundir seções curtas --- 
    logger.debug(f"_post_process_sections: Iniciando Passo 1 de fusão (min_len={min_length}, essential_types={essential_short_types})")
    merged_pass1: List[IdentifiedSection] = []
    i = 0
    while i < len(processed_sections):
        # É importante trabalhar com cópias para não modificar a lista original inesperadamente
        # ou os objetos IdentifiedSection originais se eles forem instâncias de classes complexas.
        # Se IdentifiedSection for um dict simples como no fallback, .copy() é suficiente.
        # Se for uma classe, ela precisaria de um método copy() ou usar copy.deepcopy().
        # Assumindo que IdentifiedSection se comporta como um dicionário ou tem um método copy().

        current_item = processed_sections[i]
        
        # Assegurar que estamos trabalhando com um objeto que suporta .get e acesso por chave
        # Se IdentifiedSection for uma classe, o acesso pode ser direto (current_item.content)
        # Se for um dict, é current_item.get('content') ou current_item['content']
        # O código original usava .get, então vamos manter essa compatibilidade.
        
        # Para o IdentifiedSection (classe) importado de agents, o acesso é por atributo.
        # Para o IdentifiedSection (dict) do fallback, o acesso é por .get().
        # Precisamos de uma maneira de acessar que funcione para ambos, ou normalizar.
        # Por simplicidade no refactor, vamos assumir que o objeto `current_item`
        # é a classe `IdentifiedSection` real que permite acesso por atributo.
        # Se estivermos usando o dict dummy, precisaremos ajustar ou garantir que o dummy simule melhor.
        # A classe dummy foi ajustada para suportar atributos básicos.

        current_content = getattr(current_item, 'content', current_item.get('content', ''))
        current_content_len = len(current_content.strip())
        current_type = getattr(current_item, 'section_type', current_item.get('section_type', ''))

        is_short = current_content_len < min_length
        is_essential = current_type in essential_short_types

        if is_short and not is_essential:
            if i + 1 < len(processed_sections): # Tenta fundir com a PRÓXIMA
                next_s_item = processed_sections[i+1]
                next_s_content = getattr(next_s_item, 'content', next_s_item.get('content', ''))
                next_s_type = getattr(next_s_item, 'section_type', next_s_item.get('section_type', ''))
                
                logger.debug(f"_post_process_sections Pass1 (curta->próxima): Fundindo '{current_type}' [len={current_content_len}] em '{next_s_type}'")
                
                new_content = (current_content or '') + "\n\n" + (next_s_content or '')
                
                # Atualiza o conteúdo da próxima seção.
                # Se for uma classe, setattr(next_s_item, 'content', new_content)
                # Se for dict, next_s_item['content'] = new_content
                if hasattr(next_s_item, 'content'): # Assume classe
                    setattr(next_s_item, 'content', new_content)
                else: # Assume dict
                    next_s_item['content'] = new_content

                if hasattr(next_s_item, 'start_char_offset'): # Assume classe
                    setattr(next_s_item, 'start_char_offset', None)
                    setattr(next_s_item, 'end_char_offset', None)
                else: # Assume dict
                    next_s_item['start_char_offset'] = None
                    next_s_item['end_char_offset'] = None
                
                merged_pass1.append(next_s_item) 
                i += 2 
            elif merged_pass1: # É a ÚLTIMA seção e é curta, tenta fundir com a ANTERIOR na lista JÁ PROCESSADA
                # prev_s_item é o último item adicionado a merged_pass1
                prev_s_item = merged_pass1[-1]
                prev_s_content = getattr(prev_s_item, 'content', prev_s_item.get('content', ''))
                prev_s_type = getattr(prev_s_item, 'section_type', prev_s_item.get('section_type', ''))

                logger.debug(f"_post_process_sections Pass1 (última curta->anterior): Fundindo '{current_type}' [len={current_content_len}] em '{prev_s_type}'")
                
                new_content = (prev_s_content or '') + "\n\n" + (current_content or '')

                if hasattr(prev_s_item, 'content'):
                    setattr(prev_s_item, 'content', new_content)
                else:
                    prev_s_item['content'] = new_content
                
                if hasattr(prev_s_item, 'start_char_offset'):
                    setattr(prev_s_item, 'start_char_offset', None)
                    setattr(prev_s_item, 'end_char_offset', None)
                else:
                    prev_s_item['start_char_offset'] = None
                    prev_s_item['end_char_offset'] = None
                i += 1 
            else: # É a única seção e é curta, mantém
                 logger.debug(f"_post_process_sections: Mantendo seção única curta: '{current_type}' [len={current_content_len}]")
                 merged_pass1.append(current_item)
                 i += 1
        else: # Seção não é curta ou é tipo essencial, mantém
            merged_pass1.append(current_item)
            i += 1
    
    logger.debug(f"_post_process_sections: Fim do Passo 1 de fusão. Seções restantes: {len(merged_pass1)}")
    processed_sections = merged_pass1

    # --- PASSO 2: Fundir seções consecutivas do mesmo tipo --- 
    logger.debug(f"_post_process_sections: Iniciando Passo 2 de fusão (Tipos iguais consecutivos)")
    merged_pass2: List[IdentifiedSection] = []
    if not processed_sections:
        return []
    
    # current_processing_section deve ser uma cópia para evitar modificar o item em processed_sections
    # Se for classe, current_processing_section = copy.deepcopy(processed_sections[0]) ou método .copy()
    # Se for dict, current_processing_section = processed_sections[0].copy()
    first_item = processed_sections[0]
    if isinstance(first_item, dict):
        current_processing_section = first_item.copy()
    elif hasattr(first_item, 'copy') and callable(getattr(first_item, 'copy')) : # Se tiver um método copy
        current_processing_section = first_item.copy()
    else: # Fallback para deepcopy ou erro se não copiável. Por ora, assume que é um dict ou tem copy.
        import copy # Import tardio e condicional
        try:
            current_processing_section = copy.deepcopy(first_item)
            logger.warning("Using deepcopy for IdentifiedSection in _post_process_sections. Ensure it's efficient.")
        except Exception as e_copy:
            logger.error(f"Cannot copy IdentifiedSection object: {e_copy}. Using original (risk of modification).")
            current_processing_section = first_item


    for k in range(1, len(processed_sections)):
        next_item_original = processed_sections[k]
        
        current_proc_type = getattr(current_processing_section, 'section_type', current_processing_section.get('section_type', ''))
        next_item_type = getattr(next_item_original, 'section_type', next_item_original.get('section_type', ''))
        
        is_current_fallback = default_fallback_suffix in current_proc_type
        
        if current_proc_type == next_item_type and not is_current_fallback:
            logger.debug(f"_post_process_sections Pass2 (tipos iguais): Fundindo '{next_item_type}' em '{current_proc_type}'")
            
            current_proc_content = getattr(current_processing_section, 'content', current_processing_section.get('content', ''))
            next_item_content = getattr(next_item_original, 'content', next_item_original.get('content', ''))
            
            new_content = (current_proc_content or '') + "\n\n" + (next_item_content or '')

            if hasattr(current_processing_section, 'content'):
                setattr(current_processing_section, 'content', new_content)
                setattr(current_processing_section, 'start_char_offset', None)
                setattr(current_processing_section, 'end_char_offset', None)
            else:
                current_processing_section['content'] = new_content
                current_processing_section['start_char_offset'] = None
                current_processing_section['end_char_offset'] = None
        else:
            merged_pass2.append(current_processing_section)
            # current_processing_section = next_item_original.copy() # Cuidado com a cópia aqui também
            if isinstance(next_item_original, dict):
                current_processing_section = next_item_original.copy()
            elif hasattr(next_item_original, 'copy') and callable(getattr(next_item_original, 'copy')):
                current_processing_section = next_item_original.copy()
            else:
                import copy # Import tardio e condicional
                try:
                    current_processing_section = copy.deepcopy(next_item_original)
                except Exception as e_copy_loop: # pragma: no cover
                    logger.error(f"Cannot copy IdentifiedSection object in loop: {e_copy_loop}. Using original.")
                    current_processing_section = next_item_original


    merged_pass2.append(current_processing_section) # Adiciona a última
    logger.debug(f"_post_process_sections: Fim do Passo 2 de fusão. Seções restantes: {len(merged_pass2)}")

    # --- Re-indexar e Limpar --- 
    final_sections: List[IdentifiedSection] = []
    for idx, section_item in enumerate(merged_pass2):
        # Assumindo que section_item pode ser um dict ou uma classe
        if hasattr(section_item, 'section_index'):
            setattr(section_item, 'section_index', idx)
            if getattr(section_item, 'start_char_offset', None) is None or getattr(section_item, 'end_char_offset', None) is None:
                if hasattr(section_item, 'start_char_offset'): delattr(section_item, 'start_char_offset')
                if hasattr(section_item, 'end_char_offset'): delattr(section_item, 'end_char_offset')
        else: # Assume dict
            section_item['section_index'] = idx
            if section_item.get('start_char_offset') is None or section_item.get('end_char_offset') is None:
                section_item.pop('start_char_offset', None)
                section_item.pop('end_char_offset', None)
        final_sections.append(section_item)

    return final_sections 