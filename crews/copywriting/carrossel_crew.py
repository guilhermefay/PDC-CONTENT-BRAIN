# crews/copywriting/carrossel_crew.py
import logging
import os # Para configurar API Key do LLM
from crewai import Crew, Process, Task, Agent as CrewAgent
from crews.base import BaseCrew
# Importar as funções que criam os agentes CrewAI
from agents.copywriting.carrossel_agents import create_researcher_agent, create_writer_agent, create_editor_agent
from typing import List, Any, Dict, Optional # Importar tipos necessários

# Importar e configurar LLM (exemplo com OpenAI, requer pip install crewai[openai])
# from langchain_openai import ChatOpenAI

logger = logging.getLogger(__name__)

# TODO: Configurar LLM de forma mais robusta (ex: via config)
# os.environ["OPENAI_API_KEY"] = "SUA_CHAVE_AQUI"
# llm = ChatOpenAI(model="gpt-4o") # Exemplo

class CarrosselCopyCrew(BaseCrew):
    """
    Orquestra os agentes CrewAI para criar a cópia de um carrossel do Instagram.
    Implementa os métodos abstratos de BaseCrew.
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Inicializa a crew, chamando o __init__ da BaseCrew que
           por sua vez chama _create_agents e _create_tasks."""
        logger.info("Inicializando CarrosselCopyCrew...")
        # TODO: Passar config do LLM e ferramentas aqui se necessário
        super().__init__(config)
        logger.info("CarrosselCopyCrew inicializada.")

    # Implementação do método abstrato
    def _create_agents(self) -> List[CrewAgent]: # Retornar lista de CrewAgent
        """Cria as instâncias dos agentes CrewAI para esta crew."""
        logger.debug("Criando agentes para CarrosselCopyCrew...")
        researcher = create_researcher_agent()
        writer = create_writer_agent()
        editor = create_editor_agent()
        # TODO: Configurar LLM para os agentes aqui, se não for global
        # Ex: researcher.llm = self.config.get('llm')
        return [researcher, writer, editor]

    # Implementação do método abstrato
    def _create_tasks(self) -> List[Task]: # Retornar lista de Task
        """Define as tarefas CrewAI para esta crew.
        O tópico específico será injetado via inputs no kickoff.
        """
        logger.debug("Criando tarefas para CarrosselCopyCrew...")
        
        # <<< REFINADO v2: Tasks alinhadas com goals refinados e Receita >>>
        task_research = Task(
            description=(
                "Para o tópico '{topic}', determine o Nível de Consciência alvo (Inconsciente, Problema, Solução, Produto, Muito Consciente) "
                "e pesquise no R2R por exemplos e informações relevantes para esse tópico e nível."
            ),
            agent=self.agents[0], # Researcher
            expected_output=(
                "Um objeto JSON contendo: 'consciousness_level': [Nível Identificado], 'research_summary': [Resumo da Pesquisa e Exemplos]."
            )
            # Adicionar ID para referência explícita se necessário
            # id="research_task"
        )

        task_write = Task(
            description=(
                "Usando o output da tarefa anterior (nível de consciência e pesquisa), escreva um carrossel de 7-10 slides sobre '{topic}'. "
                "Siga a Receita PDC: Slide 1 com hook forte e adequado ao nível; Slides 2-Penúltimo com desenvolvimento (história/dor/solução); Slide Final com CTA alinhado ao nível. Use tom PDC."
            ),
            agent=self.agents[1], # Writer
            context=[task_research], 
            expected_output="O texto completo de cada slide do carrossel (1 a N), seguindo a Receita PDC."
            # id="write_task"
        )

        task_edit = Task(
            description=(
                "Revise o rascunho do carrossel fornecido. Valide se ele segue 100% a Receita do Carrossel PDC, incluindo o alinhamento do hook, estrutura, conteúdo e CTA com o Nível de Consciência identificado na pesquisa inicial. "
                "Corrija gramática, clareza, fluxo e garanta o tom de voz exato do PDC."
            ),
            agent=self.agents[2], # Editor
            context=[task_write], # O contexto da task_research é herdado via task_write
            expected_output="A versão final e revisada do texto do carrossel, formatada slide por slide, 100% alinhada com a Receita PDC."
            # id="edit_task"
        )
        return [task_research, task_write, task_edit]

    # Implementação do método abstrato run
    def run(self, inputs: Optional[Dict[str, Any]] = None) -> Any:
        """
        Executa o processo completo da crew CrewAI.

        Args:
            inputs (Optional[Dict[str, Any]]): Dicionário contendo as entradas
                necessárias, como {'topic': 'seu_topico_aqui'}.

        Returns:
            Any: O resultado final da execução da crew (geralmente string).
        """
        if not inputs or 'topic' not in inputs:
            err_msg = "Input 'topic' é necessário para executar a CarrosselCopyCrew."
            logger.error(err_msg)
            return f"Erro: {err_msg}"
        
        topic = inputs['topic']
        logger.info(f"Iniciando processo da CarrosselCopyCrew para o tópico: {topic}")

        try:
            crew = Crew(
                agents=self.agents,
                tasks=self.tasks, 
                process=Process.sequential,
                verbose=True
            )
            
            # Inputs para o kickoff. O nível de consciência será determinado pela task_research
            # e passado para as tasks seguintes através do contexto.
            kickoff_inputs = {'topic': topic} 
            
            logger.info(f"Executando a crew CrewAI com inputs: {kickoff_inputs}...")
            result = crew.kickoff(inputs=kickoff_inputs)
            
            logger.info(f"Processo da CarrosselCopyCrew (CrewAI) concluído para o tópico: {topic}")
            logger.debug(f"Resultado Final (CarrosselCopyCrew): {result}")
            
            # O resultado final ainda será o output da última task (task_edit)
            return result

        except Exception as e:
            logger.exception(f"Erro ao executar CarrosselCopyCrew para '{topic}': {e}")
            return f"Erro durante a execução da crew: {e}" 