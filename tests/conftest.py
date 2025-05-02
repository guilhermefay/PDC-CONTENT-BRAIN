import pytest
from agents.annotator_agent import AnnotatorAgent
import os

class DummyAnnotator(AnnotatorAgent):
    """Subclasse que sobrepõe a chamada OpenAI para evitar requests reais."""
    def __init__(self, mapping):
        super().__init__(model="dummy")
        self._mapping = mapping

    def _call_openai(self, content_snippet):  # noqa: D401
        """Retorna resposta fixa conforme mapeamento ou um JSON default válido."""
        return self._mapping.get(content_snippet, {
            "keep": True,
            "tags": ["general"],
            "reason": "Resposta simulada padrão."
        })

    def process_chunk(self, content_snippet):
        """Simula o processamento de um chunk chamando o _call_openai mockado."""
        if not content_snippet or not isinstance(content_snippet, str) or len(content_snippet.strip()) < 1:
            return {"keep": False, "tags": ["invalid_snippet"], "reason": "Trecho inválido no mock."}
        
        return self._call_openai(content_snippet)

@pytest.fixture
def dummy_annotator_factory():
    """Retorna uma fábrica que cria DummyAnnotator com mapeamentos customizados."""
    def _create(mapping=None):
        return DummyAnnotator(mapping or {})
    return _create

@pytest.fixture
def mock_gdrive_api_responses():
    """Fixture para simular respostas da API do Google Drive."""
    # Simula uma estrutura de arquivos simples em diferentes pastas
    responses = {
        '/aulas/aula1.gdoc': {'id': 'id_aula1', 'mimeType': 'application/vnd.google-apps.document'},
        '/emails/email_importante.txt': {'id': 'id_email1', 'mimeType': 'text/plain'},
        '/copys/copy_venda.docx': {'id': 'id_copy1', 'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'},
        '/posts/post_blog.pdf': {'id': 'id_post1', 'mimeType': 'application/pdf'},
    }
    # Função interna para simular a chamada API
    def _get_file_metadata(file_path):
        return responses.get(file_path)
    return _get_file_metadata # Retorna a função de mock

@pytest.fixture
def mock_docling_extraction_results():
    """Fixture para simular resultados da extração do Docling."""
    results = {
        'id_aula1': "Conteúdo extraído da aula 1.",
        'id_email1': "Este é o conteúdo do email importante.",
        'id_copy1': "Texto da copy de vendas.",
        'id_post1': "Conteúdo do post em PDF.",
        'id_duplicate': "Conteúdo duplicado para teste.",
        'id_emoji': "Texto com muitos emojis 😀😃😄😁😆😅😂🤣😊😇🙂🙃😉😌😍🥰."
    }
    def _get_extraction(file_id):
        return results.get(file_id, "Conteúdo padrão não encontrado.")
    return _get_extraction

@pytest.fixture(params=[
    ('.docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'Conteúdo Docx'),
    ('.gdoc', 'application/vnd.google-apps.document', 'Conteúdo GDoc'),
    ('.pdf', 'application/pdf', 'Conteúdo PDF'),
    ('.txt', 'text/plain', 'Conteúdo TXT')
])
def sample_file_types(request):
    """Fixture parametrizada para fornecer tipos de arquivos de exemplo."""
    # Retorna uma tupla: (extensão, mime_type, conteúdo_simulado)
    return request.param

@pytest.fixture
def generate_test_data():
    """Gera dados de teste simples (agora como fixture)."""
    # Retorna a própria função para que possa ser chamada com parâmetros no teste
    def _generate(content_type="text", length=100):
        if content_type == "text":
            return "Palavra " * length
        elif content_type == "emoji":
            return "😀 " * length
        return "Conteúdo de teste padrão."
    return _generate

# Configuração básica para pytest-env (se necessário, mas geralmente automático)
# Se um arquivo pytest.ini ou pyproject.toml não existir, pode ser necessário
# forçar o carregamento do .env explicitamente em alguns casos, mas
# vamos assumir que funciona por padrão por enquanto. 