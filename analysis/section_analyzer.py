import re
from typing import List, TypedDict, Optional

# 3. Estrutura de Dados de Retorno (Type Hinting)
class IdentifiedSection(TypedDict):
    section_type: str
    start_char_offset: int
    end_char_offset: int
    content: str
    confidence_heuristic: Optional[float]

def _analyze_email_sections(email_content: str) -> List[IdentifiedSection]:
    """
    Analisa o conteúdo de um email e tenta identificar seções estruturais básicas.
    Por enquanto, foca em 'assunto', 'saudacao', 'ps' e 'assinatura'.
    A lógica de sobreposição e preenchimento de lacunas é simplificada.
    """
    sections: List[IdentifiedSection] = []
    processed_spans = [] # Para marcar trechos já classificados e evitar sobreposição simples

    content_len = len(email_content)

    # 1. Tentar identificar Assunto (no início do texto)
    # Regex para "Assunto: ..." ou "Subject: ..."
    subject_match = re.search(r"^(Assunto|Subject)\s*:\s*(.+?)(?:\n\n|\n[\wÀ-ú]+:)", email_content, re.IGNORECASE | re.MULTILINE)
    if subject_match:
        start = subject_match.start()
        # O fim do assunto pode ser o início da próxima linha de metadados ou duas quebras de linha
        end_content = subject_match.start(2) + len(subject_match.group(2).strip())
        end_section = subject_match.end()
        
        sections.append({
            'section_type': 'assunto',
            'start_char_offset': start,
            'end_char_offset': end_content, # Fim do conteúdo do assunto
            'content': subject_match.group(2).strip(),
            'confidence_heuristic': 0.9
        })
        processed_spans.append((start, end_section)) # Marca o span completo do match

    # 2. Tentar identificar Saudação (geralmente após o assunto ou no início)
    # Procura no início do email ou após o último span processado
    search_start_saudacao = max(span[1] for span in processed_spans) if processed_spans else 0
    
    # Regex para saudações comuns
    # Considera variações como "Olá Fulano,", "Oi,", "Prezados,"
    saudacao_match = re.search(
        r"^(Olá|Oi|Prezado\(a\)|Caro\(a\)|E aí|Bom dia|Boa tarde|Boa noite|Querido\(a\))"
        r"([\s\wÀ-ú,\.]*?)(?:\n\n|\n[A-ZÀ-Ú0-9])", # Termina com duas quebras de linha ou linha começando com maiúscula/número (heurística)
        email_content[search_start_saudacao:], 
        re.IGNORECASE | re.MULTILINE
    )
    if saudacao_match:
        start = search_start_saudacao + saudacao_match.start()
        end = search_start_saudacao + saudacao_match.end()
        # Ajusta o fim para capturar apenas a saudação em si, não a quebra de linha dupla.
        content_end = search_start_saudacao + saudacao_match.end() - (2 if saudacao_match.group(0).endswith("\n\n") else (1 if saudacao_match.group(0).endswith("\n") else 0) )
        
        # Evitar sobreposição com assunto
        if not any(start < ps_end and end > ps_start for ps_start, ps_end in processed_spans):
            sections.append({
                'section_type': 'saudacao',
                'start_char_offset': start,
                'end_char_offset': content_end,
                'content': email_content[start:content_end].strip(),
                'confidence_heuristic': 0.8
            })
            processed_spans.append((start, end))


    # 3. Tentar identificar PS e PPS (procura do fim para o início do texto)
    # Regex para P.S., PS:, P.S.:, PPS, P. P. S. etc
    # Busca a última ocorrência
    last_ps_match = None
    for ps_pattern_text in [
        r"(P\.\s?S\.\s?[:\-]?\s*(.*))", 
        r"(PS\s?[:\-]?\s*(.*))",
        r"(P\.\s?P\.\s?S\.\s*[:\-]?\s*(.*))", # PPS
        r"(PPS\s*[:\-]?\s*(.*))"
    ]:
        iterator = re.finditer(ps_pattern_text, email_content, re.IGNORECASE | re.MULTILINE)
        for match in iterator:
            if not last_ps_match or match.start() > last_ps_match.start():
                 # Considera PS se estiver na segunda metade do email, para evitar falsos positivos
                if match.start() > content_len / 2:
                    last_ps_match = match
    
    if last_ps_match:
        start = last_ps_match.start()
        end = content_len # PS vai até o fim do email
        content_text = last_ps_match.group(1).strip() # Grupo 1 é o match completo (P.S. texto)
        
        if not any(start < ps_end and end > ps_start for ps_start, ps_end in processed_spans):
            sections.append({
                'section_type': 'ps',
                'start_char_offset': start,
                'end_char_offset': end,
                'content': content_text,
                'confidence_heuristic': 0.85
            })
            processed_spans.append((start, end))

    # 4. Tentar identificar Assinatura (Bloco antes do PS, ou no final)
    # Esta é uma heurística mais complexa. Começamos de forma simples.
    # Procura por despedidas comuns e algumas linhas seguintes.
    
    # Limite superior para busca da assinatura (início do PS, se houver, senão fim do email)
    assinatura_search_end = min(span[0] for span in processed_spans if span[0] > content_len / 2) if any(s[0] > content_len /2 for s in processed_spans) else content_len
    
    # Palavras-chave de despedida comuns
    despedidas_pattern_text = r"(Atenciosamente|Até breve|Abraços?|Abs|Grato\(a\)|Obrigado\(a\)|Com os melhores cumprimentos|Cordialmente|Respeitosamente|Saudações|Aguardo seu contato|Fico à disposição)[\s,]*\n"
    
    # Tenta encontrar a última despedida significativa
    last_assinatura_match = None
    for match_assinatura in re.finditer(despedidas_pattern_text, email_content[:assinatura_search_end], re.IGNORECASE | re.MULTILINE):
        # Considera apenas se estiver na segunda metade do email
        if match_assinatura.start() > content_len / 2:
            if not last_assinatura_match or match_assinatura.start() > last_assinatura_match.start():
                last_assinatura_match = match_assinatura

    if last_assinatura_match:
        start_assinatura = last_assinatura_match.start()
        # Tenta capturar algumas linhas após a despedida como parte da assinatura
        # (limite de X linhas ou até encontrar um PS ou o fim do bloco de busca)
        end_assinatura_block = assinatura_search_end
        
        # Simplificação: considera até o final do bloco de busca (antes do PS ou fim do email)
        assinatura_content_block = email_content[start_assinatura:end_assinatura_block].strip()
        
        # Verifica se o bloco de assinatura não é excessivamente longo (heurística)
        if len(assinatura_content_block.splitlines()) < 10 and len(assinatura_content_block) < 500: # Limites arbitrários
            if not any(start_assinatura < ps_end and end_assinatura_block > ps_start for ps_start, ps_end in processed_spans):
                sections.append({
                    'section_type': 'assinatura',
                    'start_char_offset': start_assinatura,
                    'end_char_offset': end_assinatura_block,
                    'content': assinatura_content_block,
                    'confidence_heuristic': 0.7
                })
                processed_spans.append((start_assinatura, end_assinatura_block))

    # 5. Classificar o restante como corpo_principal (muito simplificado por agora)
    # Esta lógica precisa ser muito mais robusta para preencher as lacunas
    # entre as seções identificadas e classificar corretamente.
    
    # Ordena os spans processados para facilitar a identificação de lacunas
    processed_spans.sort()
    
    current_pos = 0
    unclassified_spans = []

    for start_processed, end_processed in processed_spans:
        if start_processed > current_pos:
            unclassified_spans.append((current_pos, start_processed))
        current_pos = max(current_pos, end_processed)
    
    if current_pos < content_len:
        unclassified_spans.append((current_pos, content_len))

    for start_unclassified, end_unclassified in unclassified_spans:
        if end_unclassified > start_unclassified: # Se houver conteúdo
            unclassified_content = email_content[start_unclassified:end_unclassified].strip()
            if unclassified_content: # Se o conteúdo não for apenas espaços em branco
                sections.append({
                    'section_type': 'corpo_principal', # Placeholder
                    'start_char_offset': start_unclassified,
                    'end_char_offset': end_unclassified,
                    'content': unclassified_content,
                    'confidence_heuristic': 0.3 # Confiança baixa para placeholder
                })

    # Ordenar as seções encontradas pelo offset inicial
    sections.sort(key=lambda x: x['start_char_offset'])
    
    # Filtra seções vazias que podem ter sido criadas se o strip() resultou em nada
    sections = [s for s in sections if s['content']]

    return sections

# 4. Função Principal (Esqueleto)
def analyze_document_sections(document_content: str, source_type: str) -> List[IdentifiedSection]:
    """Analisa o conteúdo do documento e retorna uma lista de seções identificadas."""
    if source_type == "email":
        return _analyze_email_sections(document_content)
    # Adicionar elif para outros source_types no futuro (ex: roteiro_video_youtube, anuncio_copy)
    # elif source_type == "roteiro_video_youtube":
    #     return _analyze_youtube_script_sections(document_content)
    else:
        # Fallback: retorna o documento inteiro como uma única seção 'corpo_geral'
        # Isso garante que sempre haja alguma saída e que todo o conteúdo seja coberto.
        # É útil para source_types ainda não mapeados.
        return [{
            'section_type': 'corpo_geral',
            'start_char_offset': 0,
            'end_char_offset': len(document_content),
            'content': document_content,
            'confidence_heuristic': 0.1 # Baixa confiança para fallback genérico
        }]

if __name__ == '__main__':
    # Exemplos de teste rápido (seria melhor ter testes unitários dedicados)
    sample_email_1 = """Assunto: Reunião de Alinhamento Semanal

Olá Equipe,

Espero que esta mensagem encontre todos bem.

Gostaria de lembrá-los sobre nossa reunião de alinhamento amanhã às 10h.
Falaremos sobre os progressos da sprint e próximos passos.

Por favor, tragam suas atualizações.

Atenciosamente,
João Silva
Gerente de Projetos
(11) 99999-8888

P.S.: O café será por minha conta!
"""

    sample_email_2 = """Oi Maria, tudo bem?

Só para confirmar nosso almoço na sexta-feira. 
O lugar de sempre, às 12:30.

Me avisa se precisar remarcar.

Abraços,
Carlos

PS: Tenta não se atrasar desta vez! ;)
PPS: E traz o relatório que te pedi.
"""

    sample_email_3 = """Subject: Your Weekly Update

Hi team,

Quick reminder about the new deployment schedule.
More details on Confluence.

Thanks,
The Admin Team
Contact us at admin@example.com
"""
    
    sample_email_4 = """Prezada Ana,
Escrevo para formalizar o convite para o evento de lançamento.
Será no dia 10, às 19h.
Contamos com sua presença!
Cordialmente,
Equipe de Eventos

P.S. Haverá um sorteio especial para os participantes!
"""

    print("--- Email 1 ---")
    sections1 = analyze_document_sections(sample_email_1, "email")
    for section in sections1:
        print(f"- {section['section_type']} ({section['start_char_offset']}-{section['end_char_offset']}): '{section['content'][:50].replace('\n', ' ')}...' (Conf: {section.get('confidence_heuristic')})")

    print("\n--- Email 2 ---")
    sections2 = analyze_document_sections(sample_email_2, "email")
    for section in sections2:
        print(f"- {section['section_type']} ({section['start_char_offset']}-{section['end_char_offset']}): '{section['content'][:50].replace('\n', ' ')}...' (Conf: {section.get('confidence_heuristic')})")
        
    print("\n--- Email 3 (sem PS ou Assinatura clara) ---")
    sections3 = analyze_document_sections(sample_email_3, "email")
    for section in sections3:
        print(f"- {section['section_type']} ({section['start_char_offset']}-{section['end_char_offset']}): '{section['content'][:50].replace('\n', ' ')}...' (Conf: {section.get('confidence_heuristic')})")

    print("\n--- Email 4 (Cordialmente) ---")
    sections4 = analyze_document_sections(sample_email_4, "email")
    for section in sections4:
        print(f"- {section['section_type']} ({section['start_char_offset']}-{section['end_char_offset']}): '{section['content'][:50].replace('\n', ' ')}...' (Conf: {section.get('confidence_heuristic')})")
    
    print("\n--- Outro Tipo (Fallback) ---")
    sections_other = analyze_document_sections("Conteúdo de um roteiro de vídeo muito interessante.", "roteiro_video_youtube")
    for section in sections_other:
        print(f"- {section['section_type']} ({section['start_char_offset']}-{section['end_char_offset']}): '{section['content'][:50].replace('\n', ' ')}...' (Conf: {section.get('confidence_heuristic')})") 