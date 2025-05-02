import pytest
import time
from unittest.mock import MagicMock, patch, call
import logging

from infra.resilience import RetryHandler

# Exceções personalizadas para teste
class NetworkTimeoutError(Exception):
    pass

class PermanentError(Exception):
    pass

# --- Testes de Inicialização ---

def test_retry_handler_init_defaults():
    handler = RetryHandler()
    assert handler.retries == 3
    assert handler.initial_delay == 0.5
    assert handler.max_delay == 10.0
    assert handler.backoff_factor == 2.0
    assert handler.jitter is True
    assert handler.retry_exceptions == (Exception,)

def test_retry_handler_init_custom():
    custom_exceptions = (NetworkTimeoutError, ConnectionError)
    handler = RetryHandler(
        retries=5,
        initial_delay=0.1,
        max_delay=5.0,
        backoff_factor=1.5,
        jitter=False,
        retry_exceptions=custom_exceptions
    )
    assert handler.retries == 5
    assert handler.initial_delay == 0.1
    assert handler.max_delay == 5.0
    assert handler.backoff_factor == 1.5
    assert handler.jitter is False
    assert handler.retry_exceptions == custom_exceptions

def test_retry_handler_init_invalid_params():
    with pytest.raises(ValueError):
        RetryHandler(retries=-1)
    with pytest.raises(ValueError):
        RetryHandler(initial_delay=-0.1)
    with pytest.raises(ValueError):
        RetryHandler(max_delay=-1.0)
    with pytest.raises(ValueError):
        RetryHandler(backoff_factor=0.9)

# --- Testes de Execução ---

@patch('time.sleep', return_value=None) # Mock time.sleep para acelerar testes
def test_execute_success_first_attempt(mock_sleep):
    handler = RetryHandler(retries=3, retry_exceptions=(NetworkTimeoutError,))
    mock_operation = MagicMock(return_value="Success", __name__="mock_op_success_first")
    
    result = handler.execute(mock_operation, "arg1", kwarg="value")
    
    assert result == "Success"
    mock_operation.assert_called_once_with("arg1", kwarg="value")
    mock_sleep.assert_not_called() # Não deve esperar se sucesso na primeira

@patch('time.sleep', return_value=None)
@patch('infra.resilience.logger') # Mock logger para verificar logs
def test_execute_success_after_retries(mock_logger, mock_sleep):
    handler = RetryHandler(retries=3, initial_delay=0.1, backoff_factor=2.0, jitter=False, retry_exceptions=(NetworkTimeoutError,))
    mock_operation = MagicMock(__name__="mock_op_success_retry")
    # Configurar para falhar 2 vezes e ter sucesso na 3ª
    mock_operation.side_effect = [
        NetworkTimeoutError("Timeout attempt 1"),
        NetworkTimeoutError("Timeout attempt 2"),
        "Success"
    ]
    
    result = handler.execute(mock_operation)
    
    assert result == "Success"
    assert mock_operation.call_count == 3
    # Verificar se sleep foi chamado com os delays corretos (sem jitter)
    # Tentativa 1 falha -> espera initial_delay (0.1)
    # Tentativa 2 falha -> espera initial_delay * backoff (0.1 * 2 = 0.2)
    expected_sleep_calls = [call(0.1), call(0.2)] 
    mock_sleep.assert_has_calls(expected_sleep_calls)
    assert mock_sleep.call_count == 2
    # Verificar logs de warning
    assert mock_logger.warning.call_count == 2

@patch('time.sleep', return_value=None)
@patch('infra.resilience.logger')
def test_execute_failure_max_retries(mock_logger, mock_sleep):
    handler = RetryHandler(retries=2, initial_delay=0.1, jitter=False, retry_exceptions=(NetworkTimeoutError,))
    mock_operation = MagicMock(side_effect=NetworkTimeoutError("Persistent Timeout"), __name__="mock_op_fail_max")
    
    with pytest.raises(NetworkTimeoutError, match="Persistent Timeout"):
        handler.execute(mock_operation)
        
    assert mock_operation.call_count == 3 # 1 inicial + 2 retries
    assert mock_sleep.call_count == 2 # Espera antes da 2ª e 3ª tentativa
    # Verificar logs
    assert mock_logger.warning.call_count == 2
    assert mock_logger.error.call_count == 1 # Log de erro final

@patch('time.sleep', return_value=None)
def test_execute_failure_non_retryable_exception(mock_sleep):
    handler = RetryHandler(retries=3, retry_exceptions=(NetworkTimeoutError,))
    mock_operation = MagicMock(side_effect=PermanentError("Cannot recover"), __name__="mock_op_fail_non_retry")
    
    with pytest.raises(PermanentError, match="Cannot recover"):
        handler.execute(mock_operation)
        
    # Deve falhar na primeira tentativa
    mock_operation.assert_called_once()
    mock_sleep.assert_not_called()

@patch('time.sleep', return_value=None)
def test_execute_failure_no_retry_exceptions_specified(mock_sleep):
    # Se retry_exceptions for None (padrão), deve tentar para qualquer Exception
    handler = RetryHandler(retries=1, initial_delay=0.1, jitter=False)
    mock_operation = MagicMock(side_effect=ValueError("Some value error"), __name__="mock_op_fail_no_spec")
    
    with pytest.raises(ValueError, match="Some value error"):
        handler.execute(mock_operation)
        
    assert mock_operation.call_count == 2 # 1 inicial + 1 retry
    mock_sleep.assert_called_once_with(0.1)
    

def test_execute_invalid_operation_type():
    handler = RetryHandler()
    with pytest.raises(TypeError, match="'operation' deve ser um callable"):
        handler.execute("not a function")

@patch('random.uniform')
@patch('time.sleep', return_value=None)
def test_execute_jitter_effect(mock_sleep, mock_uniform):
    handler = RetryHandler(retries=2, initial_delay=0.2, backoff_factor=2.0, jitter=True, retry_exceptions=(NetworkTimeoutError,))
    # Simular jitter retornando valores específicos
    mock_uniform.side_effect = [0.15, 0.35] # Valores aleatórios simulados
    mock_operation = MagicMock(__name__="mock_op_jitter")
    mock_operation.side_effect = [
        NetworkTimeoutError("Fail 1"), 
        NetworkTimeoutError("Fail 2"), 
        "Success"
    ]
    
    handler.execute(mock_operation)
    
    # Verificar se random.uniform foi chamado com os limites corretos
    # Primeira espera: uniform(0, initial_delay=0.2)
    # Segunda espera: uniform(0, current_delay=0.2*2=0.4)
    expected_uniform_calls = [call(0, 0.2), call(0, 0.4)]
    mock_uniform.assert_has_calls(expected_uniform_calls)
    
    # Verificar se sleep foi chamado com os valores retornados pelo jitter
    expected_sleep_calls = [call(0.15), call(0.35)]
    mock_sleep.assert_has_calls(expected_sleep_calls)
    assert mock_sleep.call_count == 2

@patch('time.sleep', return_value=None) 
def test_execute_max_delay_respected(mock_sleep):
    # Teste com max_delay baixo para verificar se ele limita o backoff
    handler = RetryHandler(
        retries=3, 
        initial_delay=0.1, 
        max_delay=0.25, # Limite baixo
        backoff_factor=2.0, 
        jitter=False, 
        retry_exceptions=(NetworkTimeoutError,)
    )
    mock_operation = MagicMock(__name__="mock_op_max_delay")
    mock_operation.side_effect = [
        NetworkTimeoutError("Fail 1"), 
        NetworkTimeoutError("Fail 2"), 
        NetworkTimeoutError("Fail 3"), 
        "Success"
    ]

    handler.execute(mock_operation)

    # Delays esperados (sem jitter):
    # Após falha 1: delay = initial_delay = 0.1
    # Após falha 2: delay = min(0.1 * 2, max_delay=0.25) = 0.2
    # Após falha 3: delay = min(0.2 * 2, max_delay=0.25) = 0.25 (atingiu o max_delay)
    expected_sleep_calls = [call(0.1), call(0.2), call(0.25)]
    mock_sleep.assert_has_calls(expected_sleep_calls)
    assert mock_sleep.call_count == 3 