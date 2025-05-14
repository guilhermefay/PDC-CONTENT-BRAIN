import time
import random
import logging
from typing import Callable, Any, Tuple, Type, Optional
import asyncio

# Configurar logger básico para o módulo
logger = logging.getLogger(__name__)

class RetryHandler:
    """
    Utilitário para executar operações com lógica de retry e backoff exponencial.

    Esta classe encapsula a lógica de repetir uma função ou método que pode
    falhar devido a problemas transitórios (ex: erros de rede, timeouts). 
    Utiliza backoff exponencial com jitter para espaçar as tentativas e evitar
    sobrecarregar o serviço alvo.

    Attributes:
        retries (int): Número máximo de tentativas adicionais após a falha inicial.
        initial_delay (float): Tempo de espera (em segundos) antes da primeira retentativa.
        max_delay (float): Tempo máximo de espera (em segundos) entre retentativas.
        backoff_factor (float): Fator pelo qual o delay é multiplicado a cada falha.
        jitter (bool): Se True, adiciona um pequeno fator aleatório ao delay para 
                       evitar que múltiplas instâncias retentem exatamente ao mesmo tempo.
        retry_exceptions (Tuple[Type[Exception], ...]): Tupla de tipos de exceção que 
                                                        devem acionar uma retentativa.
    """

    def __init__(
        self,
        retries: int = 3,
        initial_delay: float = 0.5,
        max_delay: float = 10.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        retry_exceptions: Optional[Tuple[Type[Exception], ...]] = None
    ):
        """
        Inicializa o RetryHandler.

        Args:
            retries: Número máximo de tentativas (excluindo a primeira chamada). Padrão: 3.
            initial_delay: Delay inicial em segundos antes da primeira retentativa. Padrão: 0.5.
            max_delay: Delay máximo em segundos entre tentativas. Padrão: 10.0.
            backoff_factor: Multiplicador para o delay (exponencial). Padrão: 2.0.
            jitter: Se True, adiciona um fator aleatório ao delay. Padrão: True.
            retry_exceptions: Uma tupla de tipos de exceção que devem acionar
                              uma nova tentativa. Se None, qualquer exceção
                              derivada de `Exception` acionará retry (use com cuidado!).
                              É fortemente recomendado especificar as exceções esperadas
                              (ex: `requests.exceptions.Timeout`, `requests.exceptions.ConnectionError`).
                              Padrão: `(Exception,)`.

        Raises:
            ValueError: Se `retries` for negativo ou se `initial_delay`, `max_delay` ou
                        `backoff_factor` tiverem valores inválidos.
        """
        if retries < 0:
            raise ValueError("Número de retries não pode ser negativo.")
        if initial_delay < 0 or max_delay < 0 or backoff_factor < 1.0:
            raise ValueError("Parâmetros de delay/backoff inválidos.")

        self.retries = retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        # Se não especificado, usar Exception como base genérica (não ideal)
        self.retry_exceptions = retry_exceptions or (Exception,)
        logger.info(f"RetryHandler inicializado: retries={retries}, initial_delay={initial_delay}, max_delay={max_delay}, backoff_factor={backoff_factor}, jitter={jitter}, retry_exceptions={retry_exceptions}")


    def execute(self, operation: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Executa a operação fornecida com a lógica de retry.

        Tenta executar a `operation`. Se uma exceção listada em `retry_exceptions`
        for levantada, espera um tempo calculado (backoff + jitter) e tenta
        novamente, até o número máximo de `retries`.

        Args:
            operation: A função ou método a ser executado.
            *args: Argumentos posicionais para passar para a `operation`.
            **kwargs: Argumentos nomeados para passar para a `operation`.

        Returns:
            O resultado retornado pela `operation` se ela for bem-sucedida dentro
            do número de tentativas permitido.

        Raises:
            A última exceção encontrada se todas as tentativas falharem.
            TypeError: Se `operation` não for um callable (função/método).
            Qualquer exceção não listada em `retry_exceptions` será imediatamente
            repassada.
        """
        if not callable(operation):
            raise TypeError("'operation' deve ser um callable (função ou método).")

        attempt = 0
        # Inicializar current_delay aqui fora do loop
        current_delay = self.initial_delay 
        while attempt <= self.retries:
            try:
                # Obter nome da operação de forma segura para o log
                op_name = getattr(operation, '__name__', 'unknown_operation')
                logger.debug(f"Tentativa {attempt + 1}/{self.retries + 1} para executar {op_name}")
                return operation(*args, **kwargs)
            except self.retry_exceptions as e:
                attempt += 1
                if attempt > self.retries:
                    logger.error(f"Falha ao executar {op_name} após {self.retries + 1} tentativas: {e}")
                    raise # Re-lança a exceção final
                
                # Calcular delay REAL com backoff, jitter e max_delay
                delay = current_delay
                if self.jitter:
                    delay = random.uniform(0, delay)
                
                # Garantir que o delay não exceda max_delay
                actual_delay = min(delay, self.max_delay)

                logger.warning(f"Erro em {op_name} (tentativa {attempt}/{self.retries + 1}): {e}. Tentando novamente em {actual_delay:.2f}s...")
                time.sleep(actual_delay)
                
                # Atualizar current_delay para a *próxima* tentativa potencial
                current_delay = min(current_delay * self.backoff_factor, self.max_delay)
                
            except Exception as e:
                # Lança exceções não configuradas para retry imediatamente
                op_name_fatal = getattr(operation, '__name__', 'unknown_operation')
                logger.exception(f"Erro não retentável ao executar {op_name_fatal}: {e}")
                raise

    async def execute_async(self, operation: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Executa a operação assíncrona fornecida com a lógica de retry.

        Tenta executar a `operation` (que deve ser um awaitable). Se uma exceção listada em 
        `retry_exceptions` for levantada, espera um tempo calculado (backoff + jitter) 
        usando `await asyncio.sleep()` e tenta novamente, até o número máximo de `retries`.

        Args:
            operation: A função ou método assíncrono (awaitable) a ser executado.
            *args: Argumentos posicionais para passar para a `operation`.
            **kwargs: Argumentos nomeados para passar para a `operation`.

        Returns:
            O resultado retornado pela `operation` se ela for bem-sucedida dentro
            do número de tentativas permitido.

        Raises:
            A última exceção encontrada se todas as tentativas falharem.
            TypeError: Se `operation` não for um callable.
            Qualquer exceção não listada em `retry_exceptions` será imediatamente
            repassada.
        """
        if not callable(operation):
            # Para funções async, também é importante verificar se são coroutines, 
            # mas `callable` é um bom primeiro passo.
            # `inspect.iscoroutinefunction(operation)` seria mais preciso.
            raise TypeError("'operation' deve ser um callable (função ou método assíncrono).")

        attempt = 0
        current_delay = self.initial_delay
        while attempt <= self.retries:
            try:
                op_name = getattr(operation, '__name__', 'unknown_async_operation')
                logger.debug(f"Tentativa {attempt + 1}/{self.retries + 1} para executar async {op_name}")
                return await operation(*args, **kwargs) # Usar await para a operação assíncrona
            except self.retry_exceptions as e:
                attempt += 1
                if attempt > self.retries:
                    logger.error(f"Falha ao executar async {op_name} após {self.retries + 1} tentativas: {e}")
                    raise
                
                delay = current_delay
                if self.jitter:
                    delay = random.uniform(0, delay)
                
                actual_delay = min(delay, self.max_delay)

                logger.warning(f"Erro em async {op_name} (tentativa {attempt}/{self.retries + 1}): {e}. Tentando novamente em {actual_delay:.2f}s...")
                await asyncio.sleep(actual_delay) # Usar await asyncio.sleep()
                
                current_delay = min(current_delay * self.backoff_factor, self.max_delay)
                
            except Exception as e:
                op_name_fatal = getattr(operation, '__name__', 'unknown_async_operation')
                logger.exception(f"Erro não retentável ao executar async {op_name_fatal}: {e}")
                raise

# Exemplo de uso (pode ser movido para testes depois)
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#
#     fail_count = 0
#
#     def potentially_failing_operation(target_fails=2):
#         global fail_count
#         logger.info(f"Executando operação... Falhas até agora: {fail_count}")
#         if fail_count < target_fails:
#             fail_count += 1
#             raise ConnectionError(f"Falha simulada na conexão - tentativa {fail_count}")
#         else:
#             logger.info("Operação bem-sucedida!")
#             return "Sucesso!"
#
#     # Configurar handler para tentar novamente em ConnectionError ou TimeoutError
#     handler = RetryHandler(retries=3, initial_delay=0.2, max_delay=1.0, retry_exceptions=(ConnectionError, TimeoutError))
#
#     try:
#         result = handler.execute(potentially_failing_operation, target_fails=2)
#         logger.info(f"Resultado final: {result}")
#     except Exception as e:
#         logger.error(f"Operação falhou após todas as tentativas: {type(e).__name__}: {e}")
#
#     print("-"*20)
#
#     # Testar falha permanente
#     fail_count = 0
#     try:
#         result = handler.execute(potentially_failing_operation, target_fails=5) # Vai falhar 4 vezes
#         logger.info(f"Resultado final (falha permanente): {result}")
#     except Exception as e:
#         logger.error(f"Operação falhou após todas as tentativas (esperado): {type(e).__name__}: {e}")
#
#     print("-"*20)
#
#     # Testar exceção não esperada
#     def raises_value_error():
#         raise ValueError("Erro inesperado!")
#
#     try:
#         handler.execute(raises_value_error)
#     except ValueError as e:
#         logger.info(f"Capturou ValueError esperado imediatamente: {e}")
#     except Exception as e:
#         logger.error(f"Erro inesperado ao testar exceção não esperada: {type(e).__name__}") 