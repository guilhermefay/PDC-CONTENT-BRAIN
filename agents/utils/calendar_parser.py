import re
import datetime
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

# Texto estático do calendário fornecido pelo usuário
# Nota: Tentando usar tabs literais (podem não ser visíveis) ou | como separador.
STATIC_CALENDAR_TEXT = """
CALENDÁRIO DE AÇÕES

Calendário para novos alunos:

Mês|Data|Início Captação|Estilo|Investimento|Temas
Janeiro|23/01 (5ª-feira)|06/01/25 (2ª-feira)|Live única lançamento|25-30K|Aniversário PDC (3 ANOS) - Protocolo Pediatra Referência
Fevereiro|Todo o mês|31/01/25 (2ª-feira)|Anúncios PPT||Bolsa residente
Março|Todo o mês|28/02/25 (6ª-feira)|Anúncios PPT||Mês da mulher
Abril|12/04/25 (sábado)|26/03/25 (4ª-feira)|Manhã de palestras (lançamento simpósio)||2º SIPCON
Maio|Todo o mês|30/04/25 (4ª-feira)|Anúncios PPT||Mês do trabalho e das mães
Junho|Todo o mês|30/05/25 (6ª-feira)|Anúncios PPT||Mês dos Namorados
Julho|24/07/25|03/07/25|Live única lançamento||Mês do Pediatra
Agosto|23/08/25|Início das vendas e divulgação 30/01/25|Pitch Presencial PDC Vitalício||INgresso
Setembro|Todo o mês|01/09/25 (2ª-feira)|Anúncios PPT||Mês da secretária
Outubro|Todo o mês|01/10/25 (4ª-feira)|Anúncios PPT||Mês do Médico
Novembro|08/11/25 com reabertura em 28/11/25|13/10/25 (2ª-feira)|Manhã de palestras (SIPCON?)||Black November
Dezembro|Todo o mês|01/12/25 (2ª-feira)|Anúncios PPT||Especial de Natal


Calendário de Ações para alunos PDC:

Ação|Data|Início Divulgação|Estilo|Temas
Janeiro - Início Vendas INgresso|30/01/25|06/01/25|Live de divulgação do INgresso|INgresso 2025 - 23/08/25
Fevereiro - Lançamento preparatório TEP|15/02/25|Dezembro 2024|Live Lançamento TEP|Preparatório TEP
Março - Vendas Notes|Todo o mês|-|Mensagens, e-mails, conteúdo|-
Abril - Vendas Especialidades|Todo o mês|-|Mensagens, e-mails, conteúdo|Reajuste do preço
Maio - ACP com valor de aluno|Todo o mês|-|Mensagens, e-mails, conteúdo|Mês do Trabalho e das mães
Julho - Mentoria PDC (de 18 por 15k)|Todo o mês|-|Mensagens, e-mails, conteúdo|Mês do Pediatra
Agosto - Vitalício|23/08/25|INgresso 2024|Evento presencial|INgresso 2025
Setembro - PSV com preço de aluno|Todo o mês|-|Mensagens, e-mails, conteúdo|Mês da Secretária
Outubro - ACP com valor de aluno|Todo o mês|-|Mensagens, e-mails, conteúdo|Mês do Médico
Novembro - Todos os produtos do PDC com 50% de desconto|Todo o mês|16/10/25|Live exclusiva para a comunidade|Black November
"""

def _parse_date(date_str: str, year: int = 2025) -> Optional[datetime.date]:
    """Helper para parsear datas nos formatos encontrados no texto."""
    if not date_str or date_str.strip() == '-' or date_str.lower() == 'todo o mês':
        return None
    
    # Remover dia da semana (ex: '(5ª-feira)')
    date_str = re.sub(r'\\s*\\(.*?\\)\\s*', '', date_str).strip()
    
    # Tentar formatos dd/mm/yy ou dd/mm
    match_yy = re.match(r'(\d{1,2})/(\d{1,2})/(\d{2})', date_str)
    match_no_yy = re.match(r'(\d{1,2})/(\d{1,2})', date_str)
    match_month_year = re.match(r'([A-Za-z]+)\\s+(\\d{4})', date_str, re.IGNORECASE) # Ex: Dezembro 2024

    try:
        if match_yy:
            day, month, yr = map(int, match_yy.groups())
            # Assume 20xx para anos de 2 dígitos
            return datetime.date(2000 + yr, month, day)
        elif match_no_yy:
            day, month = map(int, match_no_yy.groups())
            return datetime.date(year, month, day)
        elif match_month_year:
             # Para casos como "Dezembro 2024", podemos retornar o primeiro dia do mês?
             # Ou precisamos de um dia específico? Por ora, retorna None, precisa de clarificação.
             # TODO: Clarify how to handle "Month YYYY" format if needed.
             logger.warning(f"Formato de data 'Mês Ano' encontrado e ignorado por enquanto: {date_str}")
             return None
        else:
            logger.warning(f"Formato de data não reconhecido: {date_str}")
            return None
    except ValueError as e:
        logger.error(f"Erro ao parsear data '{date_str}': {e}")
        return None

def parse_static_calendar_text() -> List[Dict[str, Any]]:
    """
    Analisa o texto estático do calendário e retorna uma lista estruturada de eventos.
    """
    parsed_events = []
    # Usar splitlines() para melhor tratamento de quebras de linha
    lines = STATIC_CALENDAR_TEXT.strip().splitlines()
    
    current_calendar_type = None
    headers = []
    
    # Define os headers esperados para cada tipo (baseado na estrutura visual)
    headers_novos_alunos = ['Mês', 'Data', 'Início Captação', 'Estilo', 'Investimento', 'Temas']
    headers_alunos_pdc = ['Ação', 'Data', 'Início Divulgação', 'Estilo', 'Temas']

    current_month = None # Para manter o contexto do mês na primeira tabela

    for line in lines:
        line = line.strip()
        if not line or line == 'CALENDÁRIO DE AÇÕES':
            continue

        if line.startswith("Calendário para novos alunos:"):
            current_calendar_type = 'novos_alunos'
            headers = headers_novos_alunos
            logger.info("Iniciando parse do calendário 'novos_alunos'")
            continue
        elif line.startswith("Calendário de Ações para alunos PDC:"):
            current_calendar_type = 'alunos_pdc'
            headers = headers_alunos_pdc
            current_month = None # Resetar mês para a segunda tabela
            logger.info("Iniciando parse do calendário 'alunos_pdc'")
            continue

        # Ignorar linhas de header que estão no texto
        if line.startswith(tuple(headers)):
             continue

        if current_calendar_type:
            # Separar colunas por | (pipe)
            parts = line.split('|')

            # Verificar se o número de colunas bate com o esperado para o tipo de calendário
            if len(parts) != len(headers):
                 logger.warning(f"Número de colunas ({len(parts)}) não bate com headers ({len(headers)}) para linha: '{line}'. Pulando.")
                 continue

            data = {}
            data['calendar_type'] = current_calendar_type

            for i, header in enumerate(headers):
                value = parts[i].strip() if i < len(parts) else None
                data[header.lower().replace(' ', '_').replace('ç', 'c').replace('ã', 'a')] = value if value and value != '-' else None


            # Tratamento específico pós-parsing
            if current_calendar_type == 'novos_alunos':
                # Manter contexto do mês se a célula do mês estiver vazia
                if data.get('mes'):
                    current_month = data['mes']
                elif current_month:
                     data['mes'] = current_month
                
                # Parsear datas
                data['event_date_raw'] = data.get('data')
                data['event_date_parsed'] = _parse_date(data.get('data'))
                data['capture_start_raw'] = data.get('inicio_captacao')
                data['capture_start_parsed'] = _parse_date(data.get('inicio_captacao'))
                
            elif current_calendar_type == 'alunos_pdc':
                 # Parsear datas
                data['event_date_raw'] = data.get('data')
                data['event_date_parsed'] = _parse_date(data.get('data'))
                data['disclosure_start_raw'] = data.get('inicio_divulgacao')
                # Passa o ano explicitamente se for 2024
                year_override = 2024 if "2024" in (data.get('inicio_divulgacao') or "") else 2025
                data['disclosure_start_parsed'] = _parse_date(data.get('inicio_divulgacao'), year=year_override)

            parsed_events.append(data)

    logger.info(f"Parse concluído. Total de {len(parsed_events)} eventos extraídos.")
    return parsed_events

# Exemplo de uso (para teste rápido)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    eventos = parse_static_calendar_text()
    if eventos:
        import json
        print(json.dumps(eventos[:5], indent=2, default=str)) # Imprime os 5 primeiros formatados
        print(f"\\nTotal de eventos parseados: {len(eventos)}") 