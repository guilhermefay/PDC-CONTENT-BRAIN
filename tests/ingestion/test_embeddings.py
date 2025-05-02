import pytest
import os
from unittest.mock import patch, MagicMock
import openai

# Importar a função a ser testada
from ingestion.embeddings import generate_embeddings, logger as embeddings_logger

# Mock da estrutura de resposta da API OpenAI Embeddings (v1.x)
class MockEmbeddingData:
    def __init__(self, embedding):
        self.embedding = embedding

class MockEmbeddingResponse:
    def __init__(self, embeddings):
        self.data = [MockEmbeddingData(emb) for emb in embeddings]

# --- Testes para generate_embeddings ---

def test_generate_embeddings_success():
    """Testa a geração bem-sucedida de embeddings usando context managers."""
    test_texts = ["texto 1", "texto 2"]
    mock_embeddings = [[0.1, 0.2], [0.3, 0.4]]
    mock_response = MockEmbeddingResponse(mock_embeddings)

    # Usar context managers para os patches
    with patch('ingestion.embeddings.OPENAI_API_KEY', "fake-key") as mock_api_key, \
         patch('ingestion.embeddings.supabase', MagicMock()) as mock_supabase, \
         patch('openai.OpenAI') as mock_openai_class:
        
        # Configurar o mock do cliente OpenAI dentro do contexto
        mock_openai_client = mock_openai_class.return_value
        mock_openai_client.embeddings.create.return_value = mock_response

        result = generate_embeddings(test_texts)

        assert result == mock_embeddings
        mock_openai_class.assert_called_once_with(api_key="fake-key")
        mock_openai_client.embeddings.create.assert_called_once_with(
            model="text-embedding-3-small",
            input=test_texts
        )

@patch.dict(os.environ, {}, clear=True) # Limpar env vars, garantir que a chave está ausente
@patch('openai.OpenAI')
@patch('ingestion.embeddings.supabase', MagicMock()) 
@patch('ingestion.embeddings.logger.error') # Mock logger para verificar erro
def test_generate_embeddings_missing_api_key(mock_logger_error, mock_openai_class):
    """Testa o comportamento quando a chave OpenAI está ausente."""
    # Forçar a recarga da variável no módulo (Pytest pode cachear)
    with patch('ingestion.embeddings.OPENAI_API_KEY', None):
        test_texts = ["texto 1"]
        result = generate_embeddings(test_texts)

        assert result is None
        mock_logger_error.assert_called_with("Chave OpenAI não configurada. Não é possível gerar embeddings.")
        mock_openai_class.assert_not_called() # Cliente não deve ser instanciado
        mock_openai_class.return_value.embeddings.create.assert_not_called()

def test_generate_embeddings_api_error():
    """Testa o tratamento de erro quando a API OpenAI falha usando context managers."""
    test_texts = ["texto 1", "texto 2"]
    error_message = "API rate limit exceeded"

    with patch('ingestion.embeddings.OPENAI_API_KEY', "fake-key") as mock_api_key, \
         patch('ingestion.embeddings.supabase', MagicMock()) as mock_supabase, \
         patch('ingestion.embeddings.logger.error') as mock_logger_error, \
         patch('openai.OpenAI') as mock_openai_class:

        mock_openai_client = mock_openai_class.return_value
        mock_openai_client.embeddings.create.side_effect = openai.APIError(message=error_message, request=None, body=None)

        result = generate_embeddings(test_texts)

        assert result is None
        mock_openai_class.assert_called_once_with(api_key="fake-key")
        mock_openai_client.embeddings.create.assert_called_once_with(
            model="text-embedding-3-small",
            input=test_texts
        )
        mock_logger_error.assert_called_with(f"Erro ao gerar embeddings via OpenAI: {error_message}", exc_info=True)

@patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"})
@patch('openai.OpenAI')
@patch('ingestion.embeddings.supabase', MagicMock())
@patch('ingestion.embeddings.logger.info') # Mock info logger
def test_generate_embeddings_empty_list(mock_logger_info, mock_openai_class):
    """Testa o comportamento com uma lista de textos vazia."""
    test_texts = []
    result = generate_embeddings(test_texts)

    assert result == []
    # Verificar se a mensagem de log correta foi emitida
    mock_logger_info.assert_any_call("Lista de textos vazia, nenhum embedding para gerar.")
    # API não deve ser chamada
    mock_openai_class.return_value.embeddings.create.assert_not_called()

@patch.dict(os.environ, {"OPENAI_API_KEY": "fake-key"})
@patch('openai.OpenAI')
@patch('ingestion.embeddings.supabase', None) # Simular supabase não inicializado
@patch('ingestion.embeddings.logger.error')
def test_generate_embeddings_supabase_not_initialized(mock_logger_error, mock_openai_class):
    """Testa o comportamento quando o cliente Supabase não está inicializado."""
    test_texts = ["texto 1"]
    result = generate_embeddings(test_texts)

    assert result is None
    mock_logger_error.assert_called_with("Cliente Supabase não está inicializado. Não é possível gerar embeddings.")
    mock_openai_class.return_value.embeddings.create.assert_not_called() 