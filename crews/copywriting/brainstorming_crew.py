# crews/copywriting/brainstorming_crew.py
import logging
from crewai import Crew, Process, Task, Agent as CrewAgent
from crews.base import BaseCrew
from agents.copywriting.brainstorming_agents import (
    create_trend_researcher_agent,
    create_audience_expert_agent,
    create_creative_ideator_agent
)
from typing import List, Any, Dict, Optional

logger = logging.getLogger(__name__)

class BrainstormingCrew(BaseCrew):
    """
    Orquestra os agentes CrewAI para gerar ideias de conteúdo, hooks e headlines.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        logger.info("Inicializando BrainstormingCrew...")
        super().__init__(config)
        logger.info("BrainstormingCrew inicializada.")

    def _create_agents(self) -> List[CrewAgent]:
        logger.debug("Criando agentes para BrainstormingCrew...")
        researcher = create_trend_researcher_agent()
        expert = create_audience_expert_agent()
        ideator = create_creative_ideator_agent()
        return [researcher, expert, ideator]

    def _create_tasks(self) -> List[Task]:
        logger.debug("Criando tarefas para BrainstormingCrew...")
        # Placeholders {topic}, {target_audience} serão preenchidos via inputs

        task_research_trends = Task(
            description=(
                "Pesquisar tendências atuais (últimos 3 meses) relacionadas a '{topic}' que sejam relevantes para {target_audience}. "
                "Usar a ferramenta de busca web."
            ),
            agent=self.agents[0], # Trend Researcher
            expected_output="Uma lista de 3-5 tendências chave com uma breve explicação de sua relevância."
        )

        task_analyze_audience = Task(
            description=(
                "Analisar a persona '{target_audience}' usando R2R Search. "
                "Identificar suas 3 maiores dores, 3 maiores desejos e 3 perguntas frequentes relacionadas a '{topic}'."
            ),
            agent=self.agents[1], # Audience Expert
            # Contexto opcional: pode usar tendências para guiar a busca na persona?
            # context=[task_research_trends], 
            expected_output="Um resumo dos insights da audiência: dores, desejos e perguntas chave."
        )

        task_generate_ideas = Task(
            description=(
                "Com base nas tendências ({task_research_trends.output_key}) e insights da audiência ({task_analyze_audience.output_key}), "
                "gerar uma lista criativa para o tópico '{topic}' e público '{target_audience}'."
            ),
            agent=self.agents[2], # Creative Ideator
            context=[task_research_trends, task_analyze_audience],
            expected_output=(
                "Uma lista formatada contendo:\n"
                "- 5 a 10 Ideias de Conteúdo (temas/ângulos)\n"
                "- 5 Hooks/Ganchos chamativos\n"
                "- 5 Headlines/Títulos"
            )
        )
        
        # Fluxo sequencial: Pesquisa -> Audiência -> Ideias
        return [task_research_trends, task_analyze_audience, task_generate_ideas]

    def run(self, inputs: Optional[Dict[str, Any]] = None) -> Any:
        """
        Executa o processo completo da crew CrewAI para gerar ideias.

        Args:
            inputs (Optional[Dict[str, Any]]): Dicionário contendo {'topic': str, 'target_audience': str}.

        Returns:
            Any: O resultado final da execução da crew (lista de ideias).
        """
        if not inputs or 'topic' not in inputs or 'target_audience' not in inputs:
            err_msg = "Inputs 'topic' e 'target_audience' são necessários para executar a BrainstormingCrew."
            logger.error(err_msg)
            return f"Erro: {err_msg}"
        
        logger.info(f"Iniciando processo da BrainstormingCrew para o tópico: {inputs['topic']}, público: {inputs['target_audience']}")

        try:
            crew = Crew(
                agents=self.agents,
                tasks=self.tasks,
                process=Process.sequential,
                verbose=True
            )
            logger.info(f"Executando a crew CrewAI com inputs: {inputs}...")
            result = crew.kickoff(inputs=inputs)
            
            logger.info(f"Processo da BrainstormingCrew concluído.")
            logger.debug(f"Resultado Final (BrainstormingCrew): {result}")
            
            return result

        except Exception as e:
            logger.exception(f"Erro ao executar BrainstormingCrew: {e}")
            return f"Erro durante a execução da crew: {e}" 