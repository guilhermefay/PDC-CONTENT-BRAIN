import os
import json
from unittest.mock import patch, MagicMock

import pytest
import requests
from requests.exceptions import Timeout, ConnectionError

from infra.r2r_client import R2RClientWrapper

# ---------------------------------------------------------------------------
# Helper para criar mock response de requests.post/get
# ---------------------------------------------------------------------------

def _mock_response(status_code: int = 200, json_data: dict | None = None, text: str | None = None):
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = status_code
    if json_data is not None:
        mock_resp.json.return_value = json_data
    mock_resp.text = text or (json.dumps(json_data) if json_data else "")

    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_resp)
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp

# ---------------------------------------------------------------------------
# Tests for RAG endpoint (direct requests)
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-pk-sk-key"})
@patch("infra.r2r_client.R2RClient")
def test_rag_success(mock_r2r_client_class):
    """ Tests successful RAG call, ensuring mock intercepts SDK call. """
    # Configurar o mock da *instância* e seus métodos aninhados
    mock_sdk_instance = mock_r2r_client_class.return_value

    # Simular a estrutura de resposta completa
    mock_llm_answer = "Answer to question"
    mock_chunk = MagicMock(id='c1'); mock_chunk.to_dict.return_value = {"text": "chunk 1", "id": "c1"}
    mock_agg_search = MagicMock(); mock_agg_search.chunk_search_results = [mock_chunk]
    mock_rag_internal = MagicMock(); mock_rag_internal.generated_answer=mock_llm_answer; mock_rag_internal.search_results=mock_agg_search
    mock_wrapped_resp = MagicMock(); mock_wrapped_resp.results = mock_rag_internal

    # Configurar o método retrieval.rag mockado para retornar a resposta simulada
    mock_sdk_instance.retrieval.rag.return_value = mock_wrapped_resp

    wrapper = R2RClientWrapper()
    result = wrapper.rag(query="What is RAG?", limit=2)

    # Verificar se success é True e a resposta é a esperada
    assert result["success"] is True
    assert result["response"] == mock_llm_answer
    assert len(result["results"]) == 1
    assert result["results"][0]["id"] == "c1"
    
    # Verificar se o método mockado foi chamado corretamente
    mock_sdk_instance.retrieval.rag.assert_called_once()
    # Verificar argumentos se necessário (como no teste principal)

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com"}, clear=True)
@patch("infra.r2r_client.R2RClient")
def test_rag_missing_api_key(mock_r2r_client_class):
    """ Tests RAG fails correctly when API key is missing. """
    wrapper = R2RClientWrapper()
    result = wrapper.rag(query="no key")

    assert result["success"] is False
    assert result["error"].startswith("Authentication required")
    # Garantir que a SDK não foi chamada
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
@patch("infra.r2r_client.requests.post")
def test_rag_http_error(mock_post):
    mock_post.return_value = _mock_response(500, {"detail": "Internal"})

    wrapper = R2RClientWrapper()
    result = wrapper.rag(query="fail", limit=1)

    assert result["success"] is False
    assert "error" in result

# ---------------------------------------------------------------------------
# Tests for agentic_rag via SDK (RetryHandler mocked)
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch("infra.r2r_client.RetryHandler")
@patch("infra.r2r_client.R2RClient")
def test_agentic_rag_success(mock_r2r_client_cls, mock_retry_handler_cls):
    # Mock retry_handler.execute to return a dict
    mock_retry_instance = MagicMock()
    mock_retry_instance.execute.return_value = {"assistant": "hello"}
    mock_retry_handler_cls.return_value = mock_retry_instance

    wrapper = R2RClientWrapper()
    result = wrapper.agentic_rag(message={"role": "user", "content": "hi"})

    assert result["success"] is True
    assert result["response"]["assistant"] == "hello"
    mock_retry_instance.execute.assert_called_once()

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com"}, clear=True)
@patch("infra.r2r_client.RetryHandler")
def test_agentic_rag_missing_api_key(mock_retry_handler_cls):
    wrapper = R2RClientWrapper()
    result = wrapper.agentic_rag(message={"role": "user", "content": "hi"})

    assert result["success"] is False
    assert result["error"] == "Authentication required"

# ---------------------------------------------------------------------------
# list_documents unexpected type branch (response neither list nor dict)
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch("infra.r2r_client.RetryHandler")
@patch("infra.r2r_client.R2RClient")
def test_list_documents_unexpected_type(mock_r2r_client_cls, mock_retry_handler_cls):
    mock_retry_instance = MagicMock()
    mock_retry_instance.execute.return_value = 123  # unexpected int
    mock_retry_handler_cls.return_value = mock_retry_instance

    wrapper = R2RClientWrapper()
    res = wrapper.list_documents()

    assert res["success"] is True
    assert res["documents"] == []  # fallback empty list

# ---------------------------------------------------------------------------
# list_document_chunks unexpected type branch (response neither list nor dict)
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch("infra.r2r_client.RetryHandler")
@patch("infra.r2r_client.R2RClient")
def test_list_document_chunks_unexpected_type(mock_r2r_client_cls, mock_retry_handler_cls):
    mock_retry_instance = MagicMock()
    mock_retry_instance.execute.return_value = "oops"  # unexpected str
    mock_retry_handler_cls.return_value = mock_retry_instance

    wrapper = R2RClientWrapper()
    res = wrapper.list_document_chunks(document_id="doc123")

    assert res["success"] is True
    assert res["chunks"] == []

# ---------------------------------------------------------------------------
# health timeout network error path via RetryHandler (execute raising Timeout)
# ---------------------------------------------------------------------------

@patch.dict(os.environ, {"R2R_BASE_URL": "http://test-r2r-url.com", "R2R_API_KEY": "test-api-key"})
@patch("infra.r2r_client.requests.get")
def test_health_timeout(mock_get):
    mock_get.side_effect = Timeout("timeout")

    wrapper = R2RClientWrapper()
    healthy = wrapper.health()

    assert healthy is False 