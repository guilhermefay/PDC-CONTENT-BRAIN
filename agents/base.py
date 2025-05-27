import logging
logger = logging.getLogger(__name__)
logger.info("--- AGENTS/BASE.PY --- CACHE_BUST_V6_BASE --- LOADING ---")

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

class BaseAgent(ABC):
    """Classe base abstrata para todos os agentes no sistema.

    Define a interface mínima que todos os agentes concretos devem implementar.
    Inclui um construtor para configuração e um método `run` abstrato.

    Attributes:
        config (Dict[str, Any]): Dicionário de configuração passado durante a inicialização.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Inicializa o BaseAgent.
        
        Args:
            config (Optional[Dict[str, Any]]): Um dicionário opcional contendo
                parâmetros de configuração para o agente. Defaults to None, que
                resulta em um dicionário vazio.
        """
        self.config = config or {}
        # Initialize common attributes if any
        pass

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """Método de execução principal para o agente.

        Classes filhas DEVEM implementar este método com a lógica principal
        do agente.

        Args:
            *args: Argumentos posicionais variáveis.
            **kwargs: Argumentos nomeados variáveis.

        Returns:
            Any: O resultado da execução do agente (o tipo exato depende da
                 implementação da subclasse).

        Raises:
            NotImplementedError: Se não for implementado pela subclasse.
        """
        pass

    # Potential common methods like load_tools, get_llm, etc.
    # def load_tools(self):
    #     pass

    # def get_llm(self):
    #     pass 