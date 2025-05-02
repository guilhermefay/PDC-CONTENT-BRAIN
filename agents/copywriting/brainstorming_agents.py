# agents/copywriting/brainstorming_agents.py
import logging
from crewai import Agent as CrewAgent
from typing import List
# Importar ferramentas
from tools.search_tools import R2RSearchTool
from crewai_tools import SerperDevTool # Usar Serper para Web Search
import os

logger = logging.getLogger(__name__)

# Instanciar ferramentas (requer SERPER_API_KEY no .env)
# Verificar se a chave existe antes de instanciar
web_search_tool = None
if os.getenv("SERPER_API_KEY"):
    web_search_tool = SerperDevTool()
else:
    logger.warning("SERPER_API_KEY não encontrada no ambiente. WebSearchTool não estará disponível.")

r2r_search_tool = R2RSearchTool()

def create_trend_researcher_agent():
    tools_list = [web_search_tool] if web_search_tool else []
    return CrewAgent(
        role='Pesquisador de Tendências de Conteúdo (Pediatria/Marketing)',
        goal='Identificar 3-5 tendências recentes e tópicos quentes relevantes para {topic} e {target_audience} usando busca web.',
        backstory='Você está sempre atualizado sobre o que está em alta no mundo da pediatria, marketing digital e empreendedorismo médico.',
        verbose=True,
        allow_delegation=False,
        tools=tools_list
        # llm=...
    )

def create_audience_expert_agent():
    return CrewAgent(
        role='Especialista na Persona PDC (Natalia)',
        goal='Analisar a persona {target_audience} (foco em Natalia) usando R2R Search para extrair suas principais dores, desejos, perguntas frequentes e linguagem relacionada a {topic}.',
        backstory='Você conhece Natalia, a pediatra de consultório, melhor do que ninguém. Você sabe o que a motiva e quais são seus maiores desafios.',
        verbose=True,
        allow_delegation=False,
        tools=[r2r_search_tool]
        # llm=...
    )

def create_creative_ideator_agent():
    return CrewAgent(
        role='Gerador de Ideias Criativas para Conteúdo PDC',
        goal='Com base nas tendências e nos insights da audiência, gerar uma lista diversificada de 5-10 ideias de conteúdo (temas, ângulos), 5 hooks chamativos e 5 headlines para {topic} direcionado a {target_audience}.',
        backstory='Você é uma máquina de ideias, capaz de transformar dados brutos em conceitos de conteúdo criativos e engajadores para diferentes formatos.',
        verbose=True,
        allow_delegation=False
        # llm=...
    ) 