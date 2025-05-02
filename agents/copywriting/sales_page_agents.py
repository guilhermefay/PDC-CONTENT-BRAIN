# agents/copywriting/sales_page_agents.py
import logging
from crewai import Agent as CrewAgent
from typing import List
# Importar ferramentas necessárias
from tools.search_tools import R2RSearchTool 
# from crewai_tools import SerperDevTool

logger = logging.getLogger(__name__)

# Instanciar ferramentas
r2r_search_tool = R2RSearchTool()
# web_search_tool = SerperDevTool()

def create_problem_solution_analyst_agent():
    return CrewAgent(
        role='Analista de Problema/Solução para Páginas de Vendas PDC',
        goal='Pesquisar e definir claramente o problema principal que {product_name} resolve para {target_audience}, e como a solução se posiciona de forma única.',
        backstory='Você é especialista em entender profundamente as dores do mercado e articular como uma solução específica as resolve melhor que as alternativas.',
        verbose=True,
        allow_delegation=False,
        tools=[r2r_search_tool] # , web_search_tool]
        # llm=...
    )

def create_storyteller_agent():
    return CrewAgent(
        role='Contador de Histórias para Páginas de Vendas PDC',
        goal='Criar uma narrativa envolvente para a página de vendas de {product_name}, conectando o problema do {target_audience} com a transformação oferecida pela solução, usando exemplos e storytelling.',
        backstory='Você sabe como tecer narrativas que geram conexão emocional e ilustram vividamente a jornada do cliente, desde a dor até o resultado desejado.',
        verbose=True,
        allow_delegation=False,
        tools=[r2r_search_tool] # Para buscar depoimentos/cases
        # llm=...
    )

def create_offer_crafter_agent():
    return CrewAgent(
        role='Criador de Oferta para Páginas de Vendas PDC',
        goal='Detalhar de forma clara e persuasiva a oferta completa de {product_name}, incluindo módulos/conteúdo, bônus, garantia, preço e formas de pagamento.',
        backstory='Você é mestre em apresentar ofertas de forma irresistível, destacando o valor e minimizando a percepção de risco para o comprador.',
        verbose=True,
        allow_delegation=False,
        tools=[r2r_search_tool] # Para buscar detalhes da oferta
        # llm=...
    )

def create_cta_writer_agent():
    return CrewAgent(
        role='Escritor de CTAs para Páginas de Vendas PDC',
        goal='Criar múltiplos Call to Actions (CTAs) claros, diretos e convincentes para serem usados em diferentes seções da página de vendas de {product_name}.',
        backstory='Você sabe exatamente que palavras usar para motivar o {target_audience} a tomar a próxima ação desejada (comprar, agendar, etc.).',
        verbose=True,
        allow_delegation=False
        # llm=...
    )

# Opcional: Agente Montador/Editor
def create_sales_page_assembler_agent():
     return CrewAgent(
        role='Montador/Editor Final de Páginas de Vendas PDC',
        goal='Montar todas as seções da página de vendas (problema, história, oferta, CTAs) em um fluxo lógico e coeso, garantindo consistência de tom, clareza e formatação adequada para web.',
        backstory='Você garante que a página de vendas final seja fácil de ler, visualmente agradável e otimizada para conversão, juntando o trabalho dos outros especialistas.',
        verbose=True,
        allow_delegation=False
        # llm=...
    ) 