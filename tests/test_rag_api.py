import pytest
from fastapi.testclient import TestClient
from fastapi import status
from unittest.mock import patch, MagicMock
import jwt
import os
from datetime import datetime, timedelta, UTC

from api.rag_api import app

client = TestClient(app)

def create_test_token(sub: str, role: str = "authenticated", exp_minutes: int = 30) -> str:
    """
    Cria um token JWT de teste
    """
    jwt_secret = os.getenv("SUPABASE_JWT_SECRET", "test-secret-for-unit-tests")
    
    payload = {
        "sub": sub,
        "role": role,
        "aud": "authenticated",
        "exp": datetime.now(UTC) + timedelta(minutes=exp_minutes),
        "iat": datetime.now(UTC)
    }
    
    return jwt.encode(payload, jwt_secret, algorithm="HS256")

def test_health_check():
    """Testa o endpoint de health check"""
    response = client.get("/health")
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data
    assert "dependencies" in data

def test_query_without_token():
    """Testa que o endpoint requer autenticação"""
    response = client.post(
        "/query",
        json={"query": "test query", "top_k": 5}
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

def test_query_with_expired_token():
    """Testa que tokens expirados são rejeitados"""
    expired_token = create_test_token("test-user", exp_minutes=-30)
    headers = {"Authorization": f"Bearer {expired_token}"}
    
    response = client.post(
        "/query",
        headers=headers,
        json={"query": "test query", "top_k": 5}
    )
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert "expired" in response.json()["detail"].lower()

def test_query_with_invalid_token():
    """Testa que tokens inválidos são rejeitados"""
    headers = {"Authorization": "Bearer invalid-token"}
    
    response = client.post(
        "/query",
        headers=headers,
        json={"query": "test query", "top_k": 5}
    )
    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert "invalid" in response.json()["detail"].lower()

def test_query_invalid_request():
    """Testa validação do request body"""
    token = create_test_token("test-user")
    headers = {"Authorization": f"Bearer {token}"}
    
    # Query muito curta
    response = client.post(
        "/query",
        headers=headers,
        json={"query": "", "top_k": 5}
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    # top_k inválido
    response = client.post(
        "/query",
        headers=headers,
        json={"query": "test", "top_k": 0}
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

    response = client.post(
        "/query",
        headers=headers,
        json={"query": "test", "top_k": 21}
    )
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY

@patch('api.rag_api.r2r_client', autospec=True)
def test_query_success_with_mock(mock_r2r_client: MagicMock):
    """Testa um request válido usando um mock para o R2R Client"""
    mock_search_response = {
        "success": True,
        "results": [
            {"text": "Conteúdo do documento 1", "metadata": {"id": "doc1", "source": "test.txt", "access_level": "public"}, "similarity": 0.95},
            {"text": "Conteúdo do documento 2", "metadata": {"id": "doc2", "source": "another.pdf", "access_level": "internal"}, "similarity": 0.88},
        ]
    }
    mock_r2r_client.search.return_value = mock_search_response 

    test_user_id = "test-user-123"
    token = create_test_token(test_user_id) 
    headers = {"Authorization": f"Bearer {token}"}
    query_data = {"query": "Como estudar programação?", "top_k": 5}

    response = client.post(
        "/query",
        headers=headers,
        json=query_data
    )
    
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert "results" in data
    assert "total_found" in data
    assert "query_time_ms" in data
    assert data["total_found"] == 2
    assert len(data["results"]) == 2
    
    mock_r2r_client.search.assert_called_once_with(
        query=query_data["query"],
        limit=query_data["top_k"]
    )

    assert data["results"][0]["content"] == "Conteúdo do documento 1"
    assert data["results"][0]["metadata"]["id"] == "doc1"
    assert data["results"][0]["similarity"] == 0.95
    assert data["results"][0]["metadata"]["access_level"] == "public"
    assert data["results"][1]["content"] == "Conteúdo do documento 2"
    assert data["results"][1]["metadata"]["id"] == "doc2"
    assert data["results"][1]["similarity"] == 0.88
    assert data["results"][1]["metadata"]["access_level"] == "internal"
    
    for result in data["results"]:
        assert "content" in result
        assert "metadata" in result
        assert "access_level" in result["metadata"]
        assert "similarity" in result
        assert isinstance(result["similarity"], float)
        assert 0 <= result["similarity"] <= 1

@patch('api.rag_api.r2r_client', autospec=True)
def test_query_r2r_error(mock_r2r_client: MagicMock):
    mock_r2r_client.search.return_value = {
        "success": False,
        "error": "Simulated R2R API Error"
    }

    token = create_test_token("test-user-error") 
    headers = {"Authorization": f"Bearer {token}"}
    query_data = {"query": "Erro simulado", "top_k": 3}

    response = client.post(
        "/query",
        headers=headers,
        json=query_data
    )

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    error_data = response.json()
    assert "detail" in error_data
    assert "Simulated R2R API Error" in error_data["detail"]
    
    mock_r2r_client.search.assert_called_once_with(
        query=query_data["query"],
        limit=query_data["top_k"]
    )

@patch('api.rag_api.r2r_client', new=None)
def test_query_r2r_unavailable():
    token = create_test_token("test-user-unavailable") 
    headers = {"Authorization": f"Bearer {token}"}
    query_data = {"query": "Serviço indisponível", "top_k": 3}

    response = client.post(
        "/query",
        headers=headers,
        json=query_data
    )

    assert response.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    error_data = response.json()
    assert "detail" in error_data
    assert "R2R service client is not available" in error_data["detail"] 