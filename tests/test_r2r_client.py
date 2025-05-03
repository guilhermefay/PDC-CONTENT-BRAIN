import pytest
import os
from unittest.mock import patch, MagicMock, ANY
import requests
import json
from unittest.mock import mock_open
from infra.resilience import RetryHandler
from requests.exceptions import Timeout, ConnectionError

# Import the class to be tested
from infra.r2r_client import R2RClientWrapper, R2RClient # Assuming R2RClient is needed for type hinting/autospec

# --- Test Cases for __init__ ---

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.R2RClient') # Mock the underlying SDK client
def test_init_success(mock_sdk_client: MagicMock):
    """Test successful initialization with environment variables set."""
    wrapper = R2RClientWrapper()
    assert wrapper.base_url == "http://test-r2r-url.com"
    assert wrapper.api_key == "test-api-key"
    # Verificar se o RetryHandler foi instanciado com defaults
    assert isinstance(wrapper.retry_handler, RetryHandler)
    assert wrapper.retry_handler.retries == 3
    assert wrapper.retry_handler.retry_exceptions == (Timeout, ConnectionError)
    mock_sdk_client.assert_called_once_with(base_url="http://test-r2r-url.com")

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com"}, clear=True) # Clear other env vars
@patch('infra.r2r_client.R2RClient')
@patch('infra.r2r_client.logger.warning') # Mock logger to check warning
def test_init_missing_api_key(mock_logger_warning: MagicMock, mock_sdk_client: MagicMock):
    """Test initialization logs a warning if API key is missing."""
    wrapper = R2RClientWrapper()
    assert wrapper.base_url == "http://test-r2r-url.com"
    assert wrapper.api_key is None
    mock_logger_warning.assert_called_once_with(
        "R2R_API_KEY not found in environment variables. Authenticated endpoints might fail."
    )
    # SDK client should still be initialized, relying on its own env var reading or defaults
    mock_sdk_client.assert_called_once_with(base_url="http://test-r2r-url.com")
    assert wrapper.client == mock_sdk_client.return_value

@patch.dict(os.environ, {"R2R_API_KEY": "test-api-key"}, clear=True) # Clear other env vars
def test_init_missing_base_url():
    """Test initialization raises ValueError if base URL is missing."""
    with pytest.raises(ValueError, match="R2R_BASE_URL is required"):
        R2RClientWrapper()

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.R2RClient')
def test_init_custom_retries(mock_sdk_client: MagicMock):
    """Test initialization with custom retry parameters."""
    # Instanciar sem os parâmetros customizados, pois agora estão no RetryHandler interno
    wrapper = R2RClientWrapper()
    assert wrapper.base_url == "http://test-r2r-url.com"
    # Verificar se o RetryHandler interno tem os valores default (não customizáveis externamente por agora)
    assert wrapper.retry_handler.retries == 3
    mock_sdk_client.assert_called_once_with(base_url="http://test-r2r-url.com")

# --- Test Cases for health ---

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.requests.get')
def test_health_success(mock_get: MagicMock):
    """Test health check success."""
    # Configure mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "OK"}
    mock_get.return_value = mock_response

    wrapper = R2RClientWrapper()
    is_healthy = wrapper.health()

    assert is_healthy is True
    mock_get.assert_called_once_with("http://test-r2r-url.com/health", timeout=10)
    mock_response.raise_for_status.assert_called_once()

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.requests.get')
def test_health_http_error(mock_get: MagicMock):
    """Test health check with HTTP error."""
    # Configure mock response to raise HTTPError
    mock_response = MagicMock()
    mock_response.status_code = 500
    http_error = requests.exceptions.HTTPError(response=mock_response)
    mock_response.raise_for_status.side_effect = http_error
    mock_get.return_value = mock_response

    wrapper = R2RClientWrapper()
    is_healthy = wrapper.health()

    assert is_healthy is False
    mock_get.assert_any_call("http://test-r2r-url.com/health", timeout=10)

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.requests.get')
def test_health_request_exception(mock_get: MagicMock):
    """Test health check with requests.exceptions.RequestException."""
    # Configure mock_get to raise RequestException directly
    mock_get.side_effect = requests.exceptions.RequestException("Connection error")

    wrapper = R2RClientWrapper()
    is_healthy = wrapper.health()

    assert is_healthy is False
    mock_get.assert_any_call("http://test-r2r-url.com/health", timeout=10)

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com/"}) # URL with trailing slash
@patch('infra.r2r_client.requests.get')
def test_health_url_trailing_slash(mock_get: MagicMock):
    """Test health check handles base URL with trailing slash."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "OK"}
    mock_get.return_value = mock_response

    wrapper = R2RClientWrapper()
    wrapper.health()
    
    # Verify the URL is correctly constructed without double slash
    mock_get.assert_called_once_with("http://test-r2r-url.com/health", timeout=10)

# --- Test Cases for search ---

# Helper para criar mock response de requests.post
def _create_mock_response(status_code: int, json_data: dict = None, text: str = None):
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    if json_data is not None:
        mock_resp.json.return_value = json_data
    mock_resp.text = text or (json.dumps(json_data) if json_data else "")
    
    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_resp)
    else:
        mock_resp.raise_for_status.return_value = None # No exception for success
    return mock_resp

@patch('infra.r2r_client.R2RClient') # Mockar a classe SDK
def test_search_success(mock_r2r_client_class: MagicMock):
    """Test successful search operation via SDK."""
    # Configurar o mock da instância retornada e seu método aninhado
    mock_sdk_instance = mock_r2r_client_class.return_value
    
    # Simular a estrutura de resposta esperada de client.retrieval.search
    # (um objeto com um atributo 'results' que tem 'chunk_search_results')
    mock_chunk_result1 = MagicMock()
    # Definir atributos diretamente, pois to_dict pode não ser chamado pelo wrapper
    mock_chunk_result1.id = "chunk1"
    mock_chunk_result1.document_id = "docA"
    mock_chunk_result1.text = "Conteúdo chunk 1"
    mock_chunk_result1.score = 0.9
    mock_chunk_result1.metadata = {}
    mock_chunk_result1.to_dict.return_value = {"id": "chunk1", "document_id": "docA", "text": "Conteúdo chunk 1", "score": 0.9, "metadata": {}} # Manter to_dict por segurança

    mock_chunk_result2 = MagicMock()
    mock_chunk_result2.id = "chunk2"
    mock_chunk_result2.document_id = "docB"
    mock_chunk_result2.text = "Conteúdo chunk 2"
    mock_chunk_result2.score = 0.8
    mock_chunk_result2.metadata = {}
    mock_chunk_result2.to_dict.return_value = {"id": "chunk2", "document_id": "docB", "text": "Conteúdo chunk 2", "score": 0.8, "metadata": {}}
    
    mock_aggregate_result = MagicMock()
    mock_aggregate_result.chunk_search_results = [mock_chunk_result1, mock_chunk_result2]
    
    mock_wrapped_response = MagicMock()
    mock_wrapped_response.results = mock_aggregate_result
    
    # Configurar o mock para o método correto
    mock_sdk_instance.retrieval.search.return_value = mock_wrapped_response

    # Testar o wrapper
    with patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-pk-sk-key"}):
        wrapper = R2RClientWrapper()
        query = "test query"
        limit = 5
        filters = {"source": "gdrive"}
        result = wrapper.search(query=query, limit=limit, filters=filters)

    # Asserts
    assert result["success"] is True
    assert len(result["results"]) == 2
    # O wrapper agora deve retornar dicts se to_dict funcionar
    assert result["results"][0]["id"] == "chunk1" 
    assert result["results"][1]["id"] == "chunk2"
    
    # Verificar a chamada ao método mockado da SDK com search_settings
    mock_sdk_instance.retrieval.search.assert_called_once_with(
        query=query, 
        search_settings={'limit': limit, 'filters': filters}
    )
    mock_r2r_client_class.assert_called_once_with(base_url="http://test-r2r-url.com")

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-pk-sk-key"})
@patch('infra.r2r_client.R2RClient')
def test_search_success_no_filters(mock_r2r_client_class: MagicMock):
    """Test successful search without filters via SDK."""
    mock_sdk_instance = mock_r2r_client_class.return_value
    # Simular resposta vazia
    mock_aggregate_result = MagicMock()
    mock_aggregate_result.chunk_search_results = []
    mock_wrapped_response = MagicMock()
    mock_wrapped_response.results = mock_aggregate_result
    mock_sdk_instance.retrieval.search.return_value = mock_wrapped_response

    wrapper = R2RClientWrapper()
    query = "no filters query"
    limit = 10
    result = wrapper.search(query=query, limit=limit) # No filters passed

    assert result["success"] is True
    assert result["results"] == [] 

    # Verificar chamada com search_settings (apenas limit)
    expected_settings = {'limit': limit}
    mock_sdk_instance.retrieval.search.assert_called_once_with(
        query=query,
        search_settings=expected_settings
    )

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-pk-sk-key"})
@patch('infra.r2r_client.R2RClient')
def test_search_sdk_exception(mock_r2r_client_class: MagicMock):
    """Test search operation failing due to SDK exception."""
    mock_sdk_instance = mock_r2r_client_class.return_value
    sdk_error_message = "Internal Server Error from SDK"
    # Fazer o método mockado levantar uma exceção genérica
    mock_sdk_instance.retrieval.search.side_effect = Exception(sdk_error_message)
    
    wrapper = R2RClientWrapper()
    result = wrapper.search(query="bad request", limit=5)

    assert result["success"] is False
    assert result["results"] == []
    # A mensagem de erro agora deve ser "SDK Error: <mensagem original>"
    assert "SDK Error" in result["error"] 
    assert sdk_error_message in result["error"]
    mock_sdk_instance.retrieval.search.assert_called_once() # Verificar que foi chamado

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com"}, clear=True)
@patch('infra.r2r_client.R2RClient') # Mockar para permitir instanciação
def test_search_missing_api_key(mock_r2r_client_class: MagicMock):
    """Test search fails if API key is missing."""
    wrapper = R2RClientWrapper()
    result = wrapper.search(query="missing key", limit=5)
    assert result["success"] is False
    # A mensagem de erro do wrapper deve ser "Authentication required"
    assert "Authentication required" in result["error"] 
    assert result["results"] == []
    # Garantir que o método da SDK NÃO foi chamado
    mock_sdk_instance = mock_r2r_client_class.return_value
    mock_sdk_instance.retrieval.search.assert_not_called()

# --- Test Cases for upload_file ---

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.R2RClient') 
@patch('os.path.exists', return_value=True) 
def test_upload_file_success(mock_exists, mock_r2r_client_class: MagicMock):
    """Test successful file upload via SDK documents.create."""
    mock_sdk_instance = mock_r2r_client_class.return_value
    
    # Simular resposta de documents.create (IngestionResponse)
    mock_ingestion_result = MagicMock()
    mock_ingestion_result.message = 'Ingestion task queued.'
    mock_ingestion_result.document_id = 'new-doc-id-from-create'
    
    mock_sdk_response = MagicMock()
    mock_sdk_response.results = mock_ingestion_result # Assumindo que a resposta tem 'results'
    
    # Configurar o mock para o método documents.create
    mock_sdk_instance.documents.create.return_value = mock_sdk_response

    wrapper = R2RClientWrapper()
    file_path = "/fake/path/file.txt"
    document_id = "existing-doc-id"
    metadata = {"user": "test_user"}
    settings = None # settings não são usados por documents.create aparentemente

    result = wrapper.upload_file(file_path, document_id, metadata, settings)

    assert result["success"] is True
    # Verificar ID e mensagem extraídos da resposta simulada
    assert result["document_id"] == 'new-doc-id-from-create' 
    assert result["message"] == 'Ingestion task queued.' 

    # Verificar chamada ao método documents.create da SDK
    mock_sdk_instance.documents.create.assert_called_once_with(
        file_path=file_path,
        metadata=metadata, 
        id=document_id
    )
    mock_r2r_client_class.assert_called_once_with(base_url="http://test-r2r-url.com")

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('os.path.exists', return_value=False) # Simular que arquivo NÃO existe
@patch('infra.r2r_client.R2RClient') # Mockar SDK (mesmo que não seja chamado)
def test_upload_file_not_found(mock_r2r_client_class: MagicMock, mock_exists):
    """Test upload failure when the file does not exist."""
    wrapper = R2RClientWrapper()
    file_path = "/non/existent/file.txt"

    result = wrapper.upload_file(file_path)

    assert result["success"] is False
    assert "File not found" in result["error"]
    mock_exists.assert_called_once_with(file_path)
    mock_r2r_client_class.return_value.upload_file.assert_not_called() # Garantir que SDK não foi chamado

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com"}, clear=True) # API Key ausente
@patch('os.path.exists', return_value=True) # Arquivo existe (mas não deve ser checado)
@patch('infra.r2r_client.R2RClient') # Mockar SDK
def test_upload_file_missing_api_key(mock_r2r_client_class: MagicMock, mock_exists):
    """Test upload failure when API key is missing."""
    wrapper = R2RClientWrapper()
    file_path = "/fake/path/file.txt"

    result = wrapper.upload_file(file_path)

    assert result["success"] is False
    assert "Authentication required" in result["error"]
    # mock_exists.assert_called_once_with(file_path) # REMOVIDO - Checagem não é alcançada
    mock_r2r_client_class.return_value.upload_file.assert_not_called()

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.R2RClient') 
@patch('os.path.exists', return_value=True)
def test_upload_file_sdk_exception(mock_exists, mock_r2r_client_class: MagicMock):
    """Test handling of an exception from the SDK during upload (documents.create)."""
    mock_sdk_instance = mock_r2r_client_class.return_value
    sdk_error_message = "Upload failed via documents.create"
    # Fazer documents.create levantar a exceção
    mock_sdk_instance.documents.create.side_effect = Exception(sdk_error_message)

    wrapper = R2RClientWrapper()
    file_path = "/fake/path/upload_error.txt"
    result = wrapper.upload_file(file_path)

    assert result["success"] is False
    assert "SDK Error" in result["error"]
    assert sdk_error_message in result["error"]
    assert mock_sdk_instance.documents.create.call_count == 1 # Retry handler tentou 1 vez

# --- Test Cases for list_documents ---

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.R2RClient') 
def test_list_documents_success(mock_r2r_client_class: MagicMock):
    """Test successful listing of documents via SDK documents.list."""
    mock_sdk_instance = mock_r2r_client_class.return_value
    
    # Simular resposta de documents.list (PaginatedR2RResult com 'results')
    mock_doc_response1 = MagicMock(id="doc1", metadata={"source": "file1.txt"})
    mock_doc_response2 = MagicMock(id="doc2", metadata={"source": "file2.pdf"})
    
    mock_paginated_result = MagicMock()
    mock_paginated_result.results = [mock_doc_response1, mock_doc_response2]
    
    mock_sdk_instance.documents.list.return_value = mock_paginated_result

    wrapper = R2RClientWrapper()
    # Testar com limit e offset (sem filters por enquanto, SDK não parece aceitar)
    limit=10
    offset=0
    result = wrapper.list_documents(limit=limit, offset=offset) 

    assert result["success"] is True
    assert len(result["documents"]) == 2
    # A resposta do wrapper deve ser a lista de objetos mockados
    assert result["documents"][0].id == "doc1"
    assert result["documents"][1].id == "doc2"

    # Verificar chamada ao método documents.list da SDK
    mock_sdk_instance.documents.list.assert_called_once_with(
        limit=limit,
        offset=offset
    )
    mock_r2r_client_class.assert_called_once_with(base_url="http://test-r2r-url.com")

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.R2RClient')
def test_list_documents_empty(mock_r2r_client_class: MagicMock):
    """Test listing documents when no documents are found."""
    mock_sdk_instance = mock_r2r_client_class.return_value
    
    # Simular resposta vazia
    mock_paginated_result = MagicMock()
    mock_paginated_result.results = []
    mock_sdk_instance.documents.list.return_value = mock_paginated_result

    wrapper = R2RClientWrapper()
    result = wrapper.list_documents() # Usar defaults

    assert result["success"] is True
    assert result["documents"] == []
    # Verificar chamada com valores padrão
    mock_sdk_instance.documents.list.assert_called_once_with(limit=100, offset=0)

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch('infra.r2r_client.R2RClient')
def test_list_documents_sdk_exception(mock_r2r_client_class: MagicMock):
    """Test handling of an exception from the SDK during list_documents."""
    mock_sdk_instance = mock_r2r_client_class.return_value
    sdk_error_message = "SDK connection failed during list"
    mock_sdk_instance.documents.list.side_effect = Exception(sdk_error_message)

    wrapper = R2RClientWrapper()
    result = wrapper.list_documents()

    assert result["success"] is False
    assert result["documents"] == []
    assert "SDK Error" in result["error"]
    assert sdk_error_message in result["error"]
    assert mock_sdk_instance.documents.list.call_count == 1 # Retry handler tentou 1 vez

# --- Test Cases for delete_document ---

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_delete_document_success_by_id(mock_r2r_client_class, mock_retry_handler_class):
    """Testa a exclusão bem-sucedida de um documento por ID via SDK."""
    doc_id = "doc-to-delete-1"
    
    # Simular resposta de documents.delete (GenericBooleanResponse)
    mock_generic_result = MagicMock()
    mock_generic_result.success = True
    mock_sdk_response = MagicMock()
    mock_sdk_response.results = mock_generic_result
    
    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.return_value = mock_sdk_response # Retry retorna o objeto simulado
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.delete_document(document_id=doc_id)

    assert result["success"] is True
    # Verificar a mensagem gerada pelo wrapper
    assert "confirmed deleted by API" in result["message"] 
    mock_retry_handler_class.assert_called_once() 
    # Verificar chamada ao método documents.delete da SDK
    mock_retry_instance.execute.assert_called_once_with(
        mock_sdk_instance.documents.delete,
        id=doc_id # Parâmetro correto para deleção por ID
    )

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_delete_document_success_by_filters(mock_r2r_client_class, mock_retry_handler_class):
    """Testa a exclusão bem-sucedida de documentos por filtros via SDK."""
    filters = {"metadata_key": "metadata_value"}
    
    mock_generic_result = MagicMock()
    mock_generic_result.success = True
    mock_sdk_response = MagicMock()
    mock_sdk_response.results = mock_generic_result

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.return_value = mock_sdk_response
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.delete_document(filters=filters)

    assert result["success"] is True
    assert "confirmed deleted by API" in result["message"]
    mock_retry_handler_class.assert_called_once()
    # Verificar chamada ao método documents.delete da SDK com filters
    mock_retry_instance.execute.assert_called_once_with(
        mock_sdk_instance.documents.delete,
        filters=filters # Assumindo que aceita 'filters'
    )

@patch('infra.r2r_client.RetryHandler')
def test_delete_document_missing_api_key(mock_retry_handler_class):
    """Testa falha ao deletar sem API key."""
    with patch.dict(os.environ, {"R2R_BASE_URL": "http://dummy-url.com"}, clear=True): 
        wrapper = R2RClientWrapper()
        result = wrapper.delete_document(document_id="any-id")

        assert result["success"] is False
        assert "Authentication required" in result["error"]
        mock_retry_handler_class.return_value.execute.assert_not_called()

def test_delete_document_no_identifier():
    """Testa falha ao deletar sem ID ou filtros."""
    with patch('infra.r2r_client.R2RClient'), \
         patch('infra.r2r_client.RetryHandler') as mock_retry_handler_class:
        wrapper = R2RClientWrapper() # Precisa mockar R2RClient para instanciar
        result = wrapper.delete_document() # NENHUM argumento

        assert result["success"] is False
        assert "No identifier provided" in result["error"]
        mock_retry_handler_class.return_value.execute.assert_not_called()

@patch('infra.r2r_client.logger.warning') 
@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_delete_document_both_id_and_filters(mock_r2r_client_class, mock_retry_handler_class, mock_logger_warning):
    """Testa o aviso e comportamento (prioriza ID) ao fornecer ID e filtros."""
    doc_id = "doc-both-1"
    filters = {"user": "test"}
    
    mock_generic_result = MagicMock()
    mock_generic_result.success = True
    mock_sdk_response = MagicMock()
    mock_sdk_response.results = mock_generic_result

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.return_value = mock_sdk_response
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.delete_document(document_id=doc_id, filters=filters)

    assert result["success"] is True
    assert "confirmed deleted by API" in result["message"]
    mock_logger_warning.assert_called_once_with( # Verifica o warning
        "Both document_id and filters provided for deletion. Behavior might be undefined. Prefer using only one."
    )
    # Verifica que chamou documents.delete SÓ com ID
    mock_retry_instance.execute.assert_called_once_with(
        mock_sdk_instance.documents.delete,
        id=doc_id 
    )

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_delete_document_network_error(mock_r2r_client_class, mock_retry_handler_class):
    """Testa falha ao deletar devido a erro de rede."""
    doc_id = "doc-net-error"
    error = Timeout("Timeout deleting")

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.side_effect = error
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.delete_document(document_id=doc_id)

    assert result["success"] is False
    assert "Network Error after retries" in result["error"]
    assert "Timeout deleting" in result["error"]
    mock_retry_instance.execute.assert_called_once()

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_delete_document_sdk_error(mock_r2r_client_class, mock_retry_handler_class):
    """Testa falha ao deletar devido a erro genérico do SDK."""
    filters = {"tag": "old"}
    error = Exception("Could not delete")

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.side_effect = error
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.delete_document(filters=filters)

    assert result["success"] is False
    assert "SDK Error" in result["error"]
    assert "Could not delete" in result["error"]
    mock_retry_instance.execute.assert_called_once()

# --- Test Cases for list_document_chunks ---

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_list_document_chunks_success(mock_r2r_client_class, mock_retry_handler_class):
    """ Testa sucesso ao listar chunks (assumindo client.document_chunks) """
    mock_sdk_instance = mock_r2r_client_class.return_value
    # Assumir que document_chunks retorna uma lista de objetos chunk com to_dict
    mock_chunk1 = MagicMock(); mock_chunk1.to_dict.return_value = {'id':'c1'}
    mock_chunk2 = MagicMock(); mock_chunk2.to_dict.return_value = {'id':'c2'}
    sdk_response = [mock_chunk1, mock_chunk2] # Resposta esperada direta
    mock_sdk_instance.document_chunks.return_value = sdk_response # Mockar document_chunks

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.return_value = sdk_response # Retry retorna a mesma lista

    wrapper = R2RClientWrapper()
    doc_id = "doc1"
    limit = 100
    offset = 0
    result = wrapper.list_document_chunks(document_id=doc_id, limit=limit, offset=offset)

    assert result["success"] is True
    assert len(result["chunks"]) == 2
    assert result["chunks"][0]['id'] == 'c1'
    
    # Verificar chamada ao método document_chunks da SDK
    mock_retry_instance.execute.assert_called_once_with(
        mock_sdk_instance.document_chunks, 
        document_id=doc_id,
        limit=limit, 
        offset=offset   
    )

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_list_document_chunks_success_dict_response(mock_r2r_client_class, mock_retry_handler_class):
    mock_sdk_instance = mock_r2r_client_class.return_value
    sdk_response = {"chunks": [{"id": "chunk-3", "text": "abc"}]} # Resposta como dict
    mock_sdk_instance.document_chunks.return_value = sdk_response
    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.return_value = sdk_response
    wrapper = R2RClientWrapper()
    doc_id = "doc-456"
    result = wrapper.list_document_chunks(document_id=doc_id)

    assert result["success"] is True
    assert len(result["chunks"]) == 1
    assert result["chunks"][0] == {"id": "chunk-3", "text": "abc"}

    mock_retry_handler_class.assert_called_once()
    mock_retry_instance.execute.assert_called_once_with(
        mock_sdk_instance.document_chunks,
        document_id=doc_id,
        limit=100, # Default
        offset=0 # Default
    )

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_list_document_chunks_success_empty(mock_r2r_client_class, mock_retry_handler_class):
    """Testa sucesso com lista de chunks vazia."""
    doc_id = "doc-789"
    sdk_response = []

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.return_value = sdk_response
    
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.list_document_chunks(document_id=doc_id)

    assert result["success"] is True
    assert result["chunks"] == []
    mock_retry_handler_class.assert_called_once()
    mock_retry_instance.execute.assert_called_once()

@patch('infra.r2r_client.RetryHandler') # Mock para evitar erro de inicialização
def test_list_document_chunks_missing_api_key(mock_retry_handler_class):
    """Testa falha ao listar chunks sem API key."""
    doc_id = "doc-111"
    # Garantir que BASE_URL exista, mas API_KEY não
    with patch.dict(os.environ, {"R2R_BASE_URL": "http://dummy-url.com"}, clear=True): 
        wrapper = R2RClientWrapper() # Agora deve inicializar sem erro de BASE_URL
        # Verificar se api_key é None após init
        assert wrapper.api_key is None 
        
        result = wrapper.list_document_chunks(document_id=doc_id)

        assert result["success"] is False
        assert result["chunks"] == []
        assert "Authentication required" in result["error"]
        # Verificar que execute NÃO foi chamado
        mock_retry_instance = mock_retry_handler_class.return_value
        mock_retry_instance.execute.assert_not_called()

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_list_document_chunks_network_error(mock_r2r_client_class, mock_retry_handler_class):
    """Testa falha ao listar chunks devido a erro de rede."""
    doc_id = "doc-222"
    error = Timeout("Connection timed out")

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.side_effect = error
    
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.list_document_chunks(document_id=doc_id)

    assert result["success"] is False
    assert result["chunks"] == []
    assert "Network Error after retries" in result["error"]
    assert "Connection timed out" in result["error"]
    mock_retry_handler_class.assert_called_once()
    mock_retry_instance.execute.assert_called_once()

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_list_document_chunks_sdk_error(mock_r2r_client_class, mock_retry_handler_class):
    """Testa falha ao listar chunks devido a erro genérico do SDK."""
    doc_id = "doc-333"
    error = Exception("Unknown SDK error")

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.side_effect = error
    
    mock_sdk_instance = mock_r2r_client_class.return_value

    wrapper = R2RClientWrapper()
    result = wrapper.list_document_chunks(document_id=doc_id)

    assert result["success"] is False
    assert result["chunks"] == []
    assert "SDK Error" in result["error"]
    assert "Unknown SDK error" in result["error"]
    mock_retry_handler_class.assert_called_once()
    mock_retry_instance.execute.assert_called_once()

# --- Test Cases for get_documents_overview ---

@patch('infra.r2r_client.RetryHandler')
@patch('infra.r2r_client.R2RClient')
def test_get_documents_overview_success(mock_r2r_client_class, mock_retry_handler_class):
    """ Teste para get_documents_overview usando client.documents_overview """
    mock_sdk_instance = mock_r2r_client_class.return_value
    overview_data = {"total": 10}
    mock_sdk_instance.documents_overview.return_value = overview_data # Mockar documents_overview

    mock_retry_instance = mock_retry_handler_class.return_value
    mock_retry_instance.execute.return_value = overview_data
    
    wrapper = R2RClientWrapper()
    result = wrapper.get_documents_overview()
    
    assert result['success'] is True
    assert result['overview'] == overview_data
    # Verificar chamada a documents_overview
    mock_retry_instance.execute.assert_called_once_with(mock_sdk_instance.documents_overview)

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com"}, clear=True)
@patch("infra.r2r_client.R2RClient")
def test_rag_missing_api_key(mock_r2r_client_class: MagicMock):
    """ Test RAG fails if API key is missing. """
    wrapper = R2RClientWrapper()
    result = wrapper.rag(query="no key")
    
    assert result["success"] is False
    assert result["response"] is None
    # <<< CORRIGIDO: Verificar se começa com "Authentication required" >>>
    assert result["error"].startswith("Authentication required") 
    mock_sdk_instance = mock_r2r_client_class.return_value
    mock_sdk_instance.retrieval.rag.assert_not_called()

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-pk-sk-key"})
@patch("infra.r2r_client.R2RClient")
def test_rag_sdk_exception(mock_r2r_client_class: MagicMock):
    """ Test RAG fails due to SDK exception. """
    mock_sdk_instance = mock_r2r_client_class.return_value
    sdk_error_message = "RAG Generation Failed in SDK"
    mock_sdk_instance.retrieval.rag.side_effect = Exception(sdk_error_message)

    wrapper = R2RClientWrapper()
    result = wrapper.rag(query="test")

    assert result["success"] is False
    assert result["response"] is None
    assert "SDK Error" in result["error"]
    assert sdk_error_message in result["error"]
    mock_sdk_instance.retrieval.rag.assert_called_once()

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-pk-sk-key"})
@patch("infra.r2r_client.R2RClient") # Mockar SDK
def test_rag_success(mock_r2r_client_class: MagicMock):
    """Test successful RAG operation via SDK retrieval.rag."""
    mock_sdk_instance = mock_r2r_client_class.return_value

    # Simular resposta do client.retrieval.rag (WrappedRAGResponse -> RAGResponse -> AggregateSearchResult -> ChunkSearchResult)
    mock_llm_answer = "Generated answer based on context."
    mock_chunk_result_rag = MagicMock()
    mock_chunk_result_rag.id="chunk_rag_1"; mock_chunk_result_rag.document_id="docC"; mock_chunk_result_rag.text="Context for RAG"; mock_chunk_result_rag.score=0.85; mock_chunk_result_rag.metadata={}
    mock_chunk_result_rag.to_dict.return_value = {"id": "chunk_rag_1", "document_id": "docC", "text": "Context for RAG", "score": 0.85, "metadata": {}}
    
    mock_aggregate_search_rag = MagicMock()
    mock_aggregate_search_rag.chunk_search_results = [mock_chunk_result_rag]
    
    mock_rag_response_internal = MagicMock()
    mock_rag_response_internal.generated_answer = mock_llm_answer
    mock_rag_response_internal.search_results = mock_aggregate_search_rag
    
    mock_wrapped_rag_response = MagicMock()
    mock_wrapped_rag_response.results = mock_rag_response_internal
    
    mock_sdk_instance.retrieval.rag.return_value = mock_wrapped_rag_response

    # Testar o wrapper
    wrapper = R2RClientWrapper()
    query="What is RAG?"
    limit=2
    gen_config = {"model": "gpt-test"}
    result = wrapper.rag(query=query, limit=limit, generation_config=gen_config)

    assert result["success"] is True
    assert result["response"] == mock_llm_answer
    assert len(result["results"]) == 1
    assert result["results"][0]["id"] == "chunk_rag_1"

    # Verificar chamada ao método retrieval.rag da SDK
    mock_sdk_instance.retrieval.rag.assert_called_once_with(
        query=query,
        rag_generation_config=gen_config,
        search_settings={'limit': limit, 'filters': {}} # Verifica se search_settings foi construído corretamente
    )
    mock_r2r_client_class.assert_called_once()

# --- Adicionar/Adaptar outros testes de falha para RAG --- 