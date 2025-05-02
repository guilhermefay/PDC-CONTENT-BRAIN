# crews/copywriting/ad_crew.py
import logging
from crewai import Crew, Process, Task, Agent as CrewAgent
from crews.base import BaseCrew
from agents.copywriting.ad_agents import (
    create_platform_specialist_agent,
    create_hook_writer_agent,
    create_benefit_writer_agent
)
from typing import List, Any, Dict, Optional

logger = logging.getLogger(__name__)

class AdCopyCrew(BaseCrew):
    """
    Orquestra os agentes CrewAI para criar cópias de anúncios (Meta Ads).
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        logger.info("Inicializando AdCopyCrew...")
        super().__init__(config)
        logger.info("AdCopyCrew inicializada.")

    def _create_agents(self) -> List[CrewAgent]:
        logger.debug("Criando agentes para AdCopyCrew...")
        specialist = create_platform_specialist_agent()
        hook_writer = create_hook_writer_agent()
        benefit_writer = create_benefit_writer_agent()
        return [specialist, hook_writer, benefit_writer]

    def _create_tasks(self) -> List[Task]:
        logger.debug("Criando tarefas para AdCopyCrew...")
        # Placeholders {ad_objective}, {target_audience} serão preenchidos via inputs
        task_analyze_platform = Task(
            description=(
                "Analisar o objetivo '{ad_objective}' e público '{target_audience}'. "
                "Fornecer diretrizes concisas para Meta Ads: limite de caracteres para headline/corpo, "
                "políticas chave a observar e 1-2 melhores práticas de formato/CTA para este público."
            ),
            agent=self.agents[0], # Specialist
            expected_output="Um resumo claro das diretrizes e restrições do Meta Ads para este anúncio específico."
        )

        task_write_hooks = Task(
            description=(
                "Criar 3 opções de Ganchos (Hook) para o anúncio: uma headline curta e a primeira frase do texto. "
                "Focar em {ad_objective} para {target_audience}, respeitando as diretrizes da plataforma." 
            ),
            agent=self.agents[1], # Hook Writer
            context=[task_analyze_platform],
            expected_output="Uma lista com 3 opções de [Headline, Primeira Frase]."
        )

        task_write_body_cta = Task(
            description=(
                "Selecionar o melhor gancho das opções fornecidas. "
                "Escrever o restante do corpo do anúncio (2-3 frases curtas) focado nos benefícios para {target_audience} e alinhado ao objetivo '{ad_objective}'. "
                "Criar um Call to Action (CTA) direto e claro. "
                "Combinar tudo (melhor hook + corpo + CTA) em 2-3 variações completas do anúncio, respeitando as diretrizes da plataforma."
            ),
            agent=self.agents[2], # Benefit Writer
            context=[task_analyze_platform, task_write_hooks],
            expected_output="2 a 3 variações completas do texto final do anúncio (Headline + Corpo + CTA)."
        )
        
        return [task_analyze_platform, task_write_hooks, task_write_body_cta]

    def run(self, inputs: Optional[Dict[str, Any]] = None) -> Any:
        """
        Executa o processo completo da crew CrewAI para gerar cópias de anúncio.

        Args:
            inputs (Optional[Dict[str, Any]]): Dicionário contendo {'target_audience': str, 'ad_objective': str}.

        Returns:
            Any: O resultado final da execução da crew (variações do anúncio).
        """
        if not inputs or 'target_audience' not in inputs or 'ad_objective' not in inputs:
            err_msg = "Inputs 'target_audience' e 'ad_objective' são necessários para executar a AdCopyCrew."
            logger.error(err_msg)
            return f"Erro: {err_msg}"
        
        logger.info(f"Iniciando processo da AdCopyCrew para o objetivo: {inputs['ad_objective']}, público: {inputs['target_audience']}")

        try:
            crew = Crew(
                agents=self.agents,
                tasks=self.tasks,
                process=Process.sequential,
                verbose=True
            )
            logger.info(f"Executando a crew CrewAI com inputs: {inputs}...")
            result = crew.kickoff(inputs=inputs)
            
            logger.info(f"Processo da AdCopyCrew concluído.")
            logger.debug(f"Resultado Final (AdCopyCrew): {result}")
            
            return result

        except Exception as e:
            logger.exception(f"Erro ao executar AdCopyCrew: {e}")
            return f"Erro durante a execução da crew: {e}" 