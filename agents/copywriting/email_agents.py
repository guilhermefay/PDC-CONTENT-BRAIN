# agents/copywriting/email_agents.py
import logging
from crewai import Agent as CrewAgent
from typing import List
# Importar ferramentas necessárias (ex: R2RSearchTool)
from tools.search_tools import R2RSearchTool 

logger = logging.getLogger(__name__)

# Instanciar ferramentas (ou injetar depois)
r2r_search_tool = R2RSearchTool()
# web_search_tool = DuckDuckGoSearchRun() # Exemplo

def create_audience_analyst_agent():
    return CrewAgent(
        role='Analista de Audiência para Emails PDC',
        goal='Analisar o público-alvo especificado ({target_audience}) e fornecer insights sobre suas dores, desejos e linguagem para a escrita do email.',
        backstory=('Você entende profundamente a persona do pediatra de consultório (Natalia) e como segmentar a comunicação por email de forma eficaz.'),
        verbose=True,
        allow_delegation=False,
        tools=[r2r_search_tool] # Ferramenta para buscar dados da persona
        # llm=...
    )

def create_email_copywriter_agent():
    return CrewAgent(
        role='Copywriter Especialista em Email Marketing PDC',
        goal='Escrever um rascunho de email persuasivo e claro para o objetivo ({email_objective}), direcionado para {target_audience}, usando os insights da análise de audiência e o tom de voz do PDC.',
        backstory=('Você transforma objetivos de marketing em emails que conectam e convertem, mantendo a autenticidade e o profissionalismo do PDC.'),
        verbose=True,
        allow_delegation=False
        # tools=[r2r_search_tool] # Opcional: para buscar exemplos de email
        # llm=...
    )

def create_subject_line_optimizer_agent():
    return CrewAgent(
        role='Otimizador de Linhas de Assunto para Emails PDC',
        goal='Criar e otimizar múltiplas opções de linhas de assunto (subject lines) e preheaders para o rascunho de email fornecido, focando em maximizar a taxa de abertura para {target_audience}.',
        backstory=('Você é mestre em criar assuntos que geram curiosidade e urgência, testando diferentes ângulos e gatilhos mentais.'),
        verbose=True,
        allow_delegation=False
        # tools=[web_search_tool] # Opcional: para pesquisar boas práticas
        # llm=...
    ) 