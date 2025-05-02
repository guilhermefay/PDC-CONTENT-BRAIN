from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

# Forward reference if BaseAgent is in another file
# from agents.base import BaseAgent 
# Type hint only if necessary to avoid circular import
# from typing import TYPE_CHECKING
# if TYPE_CHECKING:
#     from agents.base import BaseAgent

# Importar BaseAgent para type hinting
from agents.base import BaseAgent

class BaseCrew(ABC):
    """Classe base abstrata para todas as Crews (equipes de agentes).

    Define a estrutura fundamental para orquestrar múltiplos agentes
    na execução de um fluxo de trabalho ou processo complexo.
    Força a implementação de métodos para criar agentes e tarefas específicas
    da crew, além do método principal `run`.

    Attributes:
        config (Dict[str, Any]): Dicionário de configuração passado durante a inicialização.
        agents (List[BaseAgent]): Lista das instâncias de agentes que compõem a crew.
        tasks (List[Any]): Lista das tarefas a serem executadas pela crew (o tipo exato
                           pode variar, ex: `crewai.Task` ou estrutura customizada).
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Inicializa a BaseCrew.

        Armazena a configuração e chama os métodos abstratos `_create_agents`
        e `_create_tasks` para inicializar os agentes e tarefas da crew.

        Args:
            config (Optional[Dict[str, Any]]): Um dicionário opcional contendo
                parâmetros de configuração para a crew e seus componentes.
                Defaults to None, que resulta em um dicionário vazio.
        """
        self.config = config or {}
        self.agents: List[BaseAgent] = self._create_agents()
        self.tasks: List[Any] = self._create_tasks()
        # Initialize crew or specific tools/resources
        pass

    @abstractmethod
    def _create_agents(self) -> List[BaseAgent]:
        """Método abstrato para criar e configurar as instâncias de agentes desta crew.

        Classes filhas DEVEM implementar este método para retornar a lista
        de instâncias `BaseAgent` que compõem a equipe.

        Returns:
            List[BaseAgent]: Lista das instâncias de agentes configuradas.

        Raises:
            NotImplementedError: Se não for implementado pela subclasse.
        """
        pass

    @abstractmethod
    def _create_tasks(self) -> List[Any]:
        """Método abstrato para definir as tarefas a serem executadas pela crew.

        Classes filhas DEVEM implementar este método para retornar a lista
        de tarefas. O tipo dos itens na lista pode ser específico da
        implementação (ex: `crewai.Task` ou uma estrutura de dados customizada).

        Returns:
            List[Any]: Lista das tarefas definidas para a crew.

        Raises:
            NotImplementedError: Se não for implementado pela subclasse.
        """
        pass

    @abstractmethod
    def run(self, inputs: Optional[Dict[str, Any]] = None) -> Any:
        """Executa o processo/fluxo de trabalho principal da crew.

        Classes filhas DEVEM implementar este método com a lógica de orquestração
        dos agentes e tarefas.

        Args:
            inputs (Optional[Dict[str, Any]]): Um dicionário opcional com os dados
                de entrada necessários para iniciar o fluxo de trabalho da crew.
                Defaults to None.

        Returns:
            Any: O resultado final da execução da crew (o tipo exato depende da
                 implementação da subclasse).

        Raises:
            NotImplementedError: Se não for implementado pela subclasse.
        """
        # This might involve setting up CrewAI's Crew object and kicking it off
        pass 