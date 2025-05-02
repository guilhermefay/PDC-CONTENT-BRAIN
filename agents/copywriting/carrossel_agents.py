# agents/copywriting/carrossel_agents.py
import logging
from crewai import Agent as CrewAgent # Usar o Agent do CrewAI
# Remover BaseAgent, vamos usar CrewAgent diretamente ou um wrapper mínimo
# from agents.base import BaseAgent 
from typing import Dict, Any, List
# Importar a ferramenta de busca
from tools.search_tools import R2RSearchTool

logger = logging.getLogger(__name__)

# Instanciar ferramentas que podem ser compartilhadas
# (Melhor prática seria injetar via Crew, mas simplificando por agora)
r2r_search_tool = R2RSearchTool()

# Definir Agentes CrewAI diretamente

def create_researcher_agent():
    return CrewAgent(
        role='Pesquisador Estratégico de Conteúdo PDC',
        goal=('Para o tópico "{topic}", determinar o Nível de Consciência (Inconsciente, Problema, Solução, Produto, Muito Consciente) mais adequado. ' 
              'Depois, buscar na base R2R exemplos de carrosséis PDC e informações relevantes para esse tópico e nível de consciência.'),
        backstory=(
            'Você é um estrategista de conteúdo especialista na jornada do cliente PDC. Sua função é prover o contexto estratégico (nível de consciência) ' 
            'e os insumos criativos (exemplos, dados) para a equipe de copywriting criar carrosséis alinhados e eficazes.'
        ),
        verbose=True,
        allow_delegation=False,
        tools=[r2r_search_tool]
    )

def create_writer_agent():
    return CrewAgent(
        role='Copywriter de Carrosséis PDC (Estilo Gabi/Julie)',
        goal=('Escrever um carrossel de 7-10 slides sobre "{topic}" para o Nível de Consciência definido. ' 
              'Seguir a Receita do Carrossel PDC: 1-Hook forte; 2..N-1-Desenvolvimento (história/dor/solução); N-CTA alinhado ao nível. ' 
              'Usar a pesquisa e exemplos fornecidos. Manter tom informal, direto e empático.'),
        backstory=(
            'Você cria narrativas que prendem a atenção e guiam o leitor, no estilo autêntico do PDC. ' 
            'Você transforma informações em carrosséis que geram conexão e desejo.'
        ),
        verbose=True,
        allow_delegation=False
        # TODO: Adicionar StyleGuideTool?
    )

def create_editor_agent():
    return CrewAgent(
        role='Editor Final de Carrosséis (Guardião da Receita PDC)',
        goal=('Revisar o rascunho do carrossel sobre "{topic}". Validar 100% de aderência à Receita do Carrossel PDC: Nível de Consciência, Estrutura (7-10 slides, Hook->Corpo->CTA), ' 
              'Tipo de Hook apropriado, Tom de Voz (Gabi/Julie), CTA específico do nível. Corrigir tudo: gramática, clareza, fluxo.'),
        backstory='Você é o controle de qualidade final. Nenhum carrossel desalinhado da estratégia ou do tom PDC passa por você. Sua revisão garante impacto e consistência.',
        verbose=True,
        allow_delegation=False
        # TODO: Adicionar StyleGuideTool?
    )

# Manter as classes wrapper antigas comentadas ou remover se não forem mais úteis
# class ResearcherAgent(BaseAgent):
#     ...
# class WriterAgent(BaseAgent):
#     ...
# class EditorAgent(BaseAgent):
#     ... 