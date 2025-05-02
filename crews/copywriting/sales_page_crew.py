# crews/copywriting/sales_page_crew.py
import logging
from crewai import Crew, Process, Task, Agent as CrewAgent
from crews.base import BaseCrew
from agents.copywriting.sales_page_agents import (
    create_problem_solution_analyst_agent,
    create_storyteller_agent,
    create_offer_crafter_agent,
    create_cta_writer_agent,
    create_sales_page_assembler_agent # Importar o montador
)
from typing import List, Any, Dict, Optional

logger = logging.getLogger(__name__)

class SalesPageCopyCrew(BaseCrew):
    """
    Orquestra os agentes CrewAI para criar a cópia de uma página de vendas longa.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        logger.info("Inicializando SalesPageCopyCrew...")
        super().__init__(config)
        logger.info("SalesPageCopyCrew inicializada.")

    def _create_agents(self) -> List[CrewAgent]:
        logger.debug("Criando agentes para SalesPageCopyCrew...")
        analyst = create_problem_solution_analyst_agent()
        storyteller = create_storyteller_agent()
        offer_crafter = create_offer_crafter_agent()
        cta_writer = create_cta_writer_agent()
        assembler = create_sales_page_assembler_agent()
        return [analyst, storyteller, offer_crafter, cta_writer, assembler]

    def _create_tasks(self) -> List[Task]:
        logger.debug("Criando tarefas para SalesPageCopyCrew...")
        # Placeholders {product_name}, {target_audience} serão preenchidos via inputs

        task_analyze = Task(
            description=(
                "Analisar profundamente o problema que {product_name} resolve para {target_audience}. "
                "Identificar a dor principal, os desafios secundários e como a solução do PDC é única. "
                "Usar R2R para buscar dados da persona e do produto."
            ),
            agent=self.agents[0], # Analyst
            expected_output="Um documento claro definindo o problema central, as dores associadas e a proposta de valor única da solução."
        )

        task_narrative = Task(
            description=(
                "Desenvolver a narrativa principal da página de vendas para {product_name}, conectando o problema ({task_analyze.output_key}) à transformação. "
                "Incluir elementos de storytelling, como a jornada do cliente ou um case de sucesso (buscar exemplos no R2R)."
            ),
            agent=self.agents[1], # Storyteller
            context=[task_analyze],
            expected_output="Um esboço da narrativa da página de vendas, incluindo os principais pontos de história e transições."
        )

        task_offer = Task(
            description=(
                "Detalhar a oferta completa de {product_name}, incluindo módulos, bônus, garantia, preço e condições. "
                "Apresentar de forma clara e persuasiva, destacando o valor total. Usar R2R para detalhes precisos da oferta."
            ),
            agent=self.agents[2], # Offer Crafter
            context=[task_analyze], # Precisa saber o valor que a oferta entrega
            expected_output="Uma seção detalhada descrevendo todos os componentes da oferta, seus benefícios e o valor percebido."
        )

        task_cta = Task(
            description=(
                "Criar 3 variações de Call to Action (CTA) principal para {product_name}, e 2 CTAs secundários para diferentes pontos da página. "
                "Devem ser claros, diretos e criar um senso de urgência ou benefício imediato para {target_audience}."
            ),
            agent=self.agents[3], # CTA Writer
            context=[task_offer], # Precisa saber o que está sendo oferecido
            expected_output="Uma lista de 3 CTAs principais e 2 CTAs secundários, com sugestões de onde posicioná-los."
        )

        task_assemble = Task(
            description=(
                "Montar a estrutura final da página de vendas combinando a análise do problema ({task_analyze.output_key}), a narrativa ({task_narrative.output_key}), "
                "a oferta ({task_offer.output_key}) e os CTAs ({task_cta.output_key}). "
                "Garantir um fluxo lógico (ex: AIDA ou PAS), coesão, consistência de tom PDC e formatação básica para web (ex: usando markdown)."
            ),
            agent=self.agents[4], # Assembler
            context=[task_analyze, task_narrative, task_offer, task_cta],
            expected_output="O texto completo e montado da página de vendas, formatado em markdown, pronto para revisão final e design."
        )

        return [task_analyze, task_narrative, task_offer, task_cta, task_assemble]

    def run(self, inputs: Optional[Dict[str, Any]] = None) -> Any:
        """
        Executa o processo completo da crew CrewAI para gerar a cópia da página de vendas.

        Args:
            inputs (Optional[Dict[str, Any]]): Dicionário contendo {'product_name': str, 'target_audience': str}.

        Returns:
            Any: O resultado final da execução da crew (texto da página de vendas).
        """
        if not inputs or 'product_name' not in inputs or 'target_audience' not in inputs:
            err_msg = "Inputs 'product_name' e 'target_audience' são necessários para executar a SalesPageCopyCrew."
            logger.error(err_msg)
            return f"Erro: {err_msg}"
        
        logger.info(f"Iniciando processo da SalesPageCopyCrew para o produto: {inputs['product_name']}, público: {inputs['target_audience']}")

        try:
            crew = Crew(
                agents=self.agents,
                tasks=self.tasks,
                process=Process.sequential,
                verbose=True
            )
            logger.info(f"Executando a crew CrewAI com inputs: {inputs}...")
            result = crew.kickoff(inputs=inputs)
            
            logger.info(f"Processo da SalesPageCopyCrew concluído.")
            logger.debug(f"Resultado Final (SalesPageCopyCrew): {result}")
            
            return result

        except Exception as e:
            logger.exception(f"Erro ao executar SalesPageCopyCrew: {e}")
            return f"Erro durante a execução da crew: {e}" 