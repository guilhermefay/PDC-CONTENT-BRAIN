# crews/copywriting/email_crew.py
import logging
import os # Para configurar API Key do LLM
from crewai import Crew, Process, Task, Agent as CrewAgent
from crews.base import BaseCrew
from agents.copywriting.email_agents import (
    create_audience_analyst_agent,
    create_email_copywriter_agent,
    create_subject_line_optimizer_agent
)
from typing import List, Any, Dict, Optional

logger = logging.getLogger(__name__)

class EmailCopyCrew(BaseCrew):
    """
    Orquestra os agentes CrewAI para criar a cópia de um email marketing.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        logger.info("Inicializando EmailCopyCrew...")
        super().__init__(config)
        logger.info("EmailCopyCrew inicializada.")

    def _create_agents(self) -> List[CrewAgent]:
        logger.debug("Criando agentes para EmailCopyCrew...")
        analyst = create_audience_analyst_agent()
        copywriter = create_email_copywriter_agent()
        optimizer = create_subject_line_optimizer_agent()
        return [analyst, copywriter, optimizer]

    def _create_tasks(self) -> List[Task]:
        logger.debug("Criando tarefas para EmailCopyCrew...")
        # Placeholders {target_audience} e {email_objective} serão preenchidos via inputs
        task_analyze = Task(
            description=(
                "Analisar o público-alvo: '{target_audience}'. "
                "Usar a ferramenta de busca R2R para encontrar informações sobre suas dores, desejos, "
                "objeções comuns e linguagem preferida. Focar em insights para o objetivo: '{email_objective}'."
            ),
            agent=self.agents[0], # Analyst
            expected_output="Um resumo dos principais insights sobre a audiência ({target_audience}) relevantes para o objetivo '{email_objective}', incluindo pontos de dor e motivadores."
        )

        task_write_body = Task(
            description=(
                "Escrever o corpo completo de um email marketing para o objetivo: '{email_objective}'. "
                "Usar os insights sobre a audiência ({target_audience}) fornecidos pela análise. "
                "Seguir o tom de voz do PDC (profissional, empático, direto ao ponto). "
                "Incluir um CTA claro alinhado ao objetivo."
            ),
            agent=self.agents[1], # Copywriter
            context=[task_analyze],
            expected_output="O texto completo do corpo do email, pronto para revisão."
        )

        task_optimize_subject = Task(
            description=(
                "Criar 3-5 opções de linhas de assunto (subject lines) e preheaders para o email escrito (objetivo: '{email_objective}', público: '{target_audience}'). "
                "Focar em maximizar a taxa de abertura usando gatilhos como curiosidade, benefício ou urgência."
            ),
            agent=self.agents[2], # Optimizer
            context=[task_write_body], # Precisa do corpo para ter contexto
            expected_output="Uma lista de 3 a 5 pares de [Linha de Assunto, Preheader] otimizados."
        )
        
        # Adicionar tarefa final para consolidar?
        # task_consolidate = Task(...) agent=copywriter? para juntar tudo
        
        # Por enquanto, o resultado da última task (optimize_subject) será o output principal.
        # O corpo do email estará no output da task_write_body.
        return [task_analyze, task_write_body, task_optimize_subject]

    # O método run herdado de BaseCrew deve funcionar se _create_agents e _create_tasks estão implementados.
    # Apenas precisamos garantir que os inputs corretos (target_audience, email_objective) sejam passados.
    def run(self, inputs: Optional[Dict[str, Any]] = None) -> Any:
        """
        Executa o processo completo da crew CrewAI para gerar a cópia do email.

        Args:
            inputs (Optional[Dict[str, Any]]): Dicionário contendo {'target_audience': str, 'email_objective': str}.

        Returns:
            Any: O resultado final da execução da crew (provavelmente o output da última task).
        """
        if not inputs or 'target_audience' not in inputs or 'email_objective' not in inputs:
            err_msg = "Inputs 'target_audience' e 'email_objective' são necessários para executar a EmailCopyCrew."
            logger.error(err_msg)
            return f"Erro: {err_msg}"
        
        logger.info(f"Iniciando processo da EmailCopyCrew para o objetivo: {inputs['email_objective']}, público: {inputs['target_audience']}")

        try:
            crew = Crew(
                agents=self.agents,
                tasks=self.tasks,
                process=Process.sequential,
                verbose=True # Usar True em vez de 2
            )
            logger.info(f"Executando a crew CrewAI com inputs: {inputs}...")
            result = crew.kickoff(inputs=inputs)
            
            logger.info(f"Processo da EmailCopyCrew concluído.")
            logger.debug(f"Resultado Final (EmailCrewAI): {result}")
            
            # O resultado do kickoff é geralmente o output da última tarefa.
            # Pode ser necessário buscar o output de task_write_body explicitamente se quisermos o corpo e o assunto.
            # Ex: return {"subjects": result, "body": task_write_body.output} (mas task.output não é padrão)
            return result

        except Exception as e:
            logger.exception(f"Erro ao executar EmailCopyCrew: {e}")
            return f"Erro durante a execução da crew: {e}" 