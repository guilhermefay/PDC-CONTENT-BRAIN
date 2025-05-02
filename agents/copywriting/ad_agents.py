# agents/copywriting/ad_agents.py
import logging
from crewai import Agent as CrewAgent
from typing import List
# Importar ferramentas necessárias (ex: R2RSearchTool, Web Search)
from tools.search_tools import R2RSearchTool
# from crewai_tools import SerperDevTool # Exemplo para busca web

logger = logging.getLogger(__name__)

# Instanciar ferramentas
r2r_search_tool = R2RSearchTool()
# web_search_tool = SerperDevTool() # Requer SERPER_API_KEY

def create_platform_specialist_agent():
    return CrewAgent(
        role='Especialista em Plataforma de Anúncios (Meta Ads)',
        goal='Analisar o objetivo do anúncio ({ad_objective}) e o público ({target_audience}), e fornecer diretrizes e restrições específicas da plataforma Meta Ads (limites de caracteres, políticas, melhores práticas).',
        backstory='Você conhece profundamente as políticas de anúncios do Meta, formatos e o que funciona melhor para engajar públicos como pediatras de consultório.',
        verbose=True,
        allow_delegation=False
        # tools=[web_search_tool] # Para buscar políticas atualizadas
        # llm=...
    )

def create_hook_writer_agent():
    return CrewAgent(
        role='Escritor de Ganchos (Hooks) para Anúncios PDC',
        goal='Criar 3-5 opções de headlines e primeiras linhas de texto extremamente chamativas para um anúncio com objetivo \'{ad_objective}\' para {target_audience}, seguindo as diretrizes da plataforma.',
        backstory='Sua especialidade é capturar a atenção nos primeiros segundos. Você cria ganchos que geram curiosidade e falam diretamente com a dor ou desejo do público.',
        verbose=True,
        allow_delegation=False
        # tools=[r2r_search_tool] # Para buscar exemplos de hooks anteriores
        # llm=...
    )

def create_benefit_writer_agent():
    return CrewAgent(
        role='Copywriter de Anúncios Focado em Benefícios PDC',
        goal='Desenvolver o corpo do anúncio e o Call to Action (CTA) para o objetivo \'{ad_objective}\', destacando os benefícios para {target_audience}, usando o melhor hook selecionado e respeitando as diretrizes da plataforma.',
        backstory='Você traduz features em benefícios claros e cria CTAs que levam à ação, sempre alinhado ao tom profissional e empático do PDC.',
        verbose=True,
        allow_delegation=False
        # tools=[r2r_search_tool] # Para buscar detalhes da oferta ou produto
        # llm=...
    ) 