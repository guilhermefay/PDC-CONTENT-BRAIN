import pytest
import os
from unittest.mock import patch, MagicMock, mock_open
from googleapiclient.errors import HttpError
from unittest.mock import call

# Importar funções e classes do módulo a ser testado
from ingestion.gdrive_ingest import (
    authenticate_gdrive,
    # list_files_in_folder, # REMOVIDO - Função não existe
    # download_google_doc_as_text, # REMOVIDO - Função não existe mais
    export_and_download_gdoc, # Função que substituiu parte da lógica?
    download_file,
    ingest_all_gdrive_content, # Função principal
    ingest_gdrive_folder # IMPORTAR esta para testes mais focados
)

# --- Testes para authenticate_gdrive ---

@patch.dict(os.environ, {'GOOGLE_SERVICE_ACCOUNT_JSON': '/fake/path/creds.json'})
@patch('ingestion.gdrive_ingest.os.path.exists')
@patch('ingestion.gdrive_ingest.service_account.Credentials.from_service_account_file')
@patch('ingestion.gdrive_ingest.build')
def test_authenticate_gdrive_success(
    mock_build: MagicMock,
    mock_from_service_account: MagicMock,
    mock_exists: MagicMock
):
    """Testa autenticação bem-sucedida."""
    mock_exists.return_value = True # Simula que o arquivo existe
    mock_credentials = MagicMock() # Mock do objeto de credenciais
    mock_from_service_account.return_value = mock_credentials
    mock_service = MagicMock() # Mock do objeto de serviço retornado por build
    mock_build.return_value = mock_service

    service = authenticate_gdrive()

    mock_exists.assert_called_once_with('/fake/path/creds.json')
    mock_from_service_account.assert_called_once_with(
        '/fake/path/creds.json',
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    mock_build.assert_called_once_with('drive', 'v3', credentials=mock_credentials)
    assert service == mock_service # Verifica se o serviço mockado foi retornado

@patch.dict(os.environ, {'GOOGLE_SERVICE_ACCOUNT_JSON': '/fake/path/creds.json'})
@patch('ingestion.gdrive_ingest.os.path.exists')
def test_authenticate_gdrive_file_not_found(mock_exists: MagicMock):
    """Testa falha quando o arquivo de credenciais não existe."""
    mock_exists.return_value = False # Simula que o arquivo NÃO existe

    with pytest.raises(ValueError, match="Caminho para GOOGLE_SERVICE_ACCOUNT_JSON não definido ou inválido."):
        authenticate_gdrive()

    mock_exists.assert_called_once_with('/fake/path/creds.json')

@patch.dict(os.environ, {}, clear=True) # Sem a variável de ambiente
def test_authenticate_gdrive_env_var_missing():
    """Testa falha quando a variável de ambiente não está definida."""
    with pytest.raises(ValueError, match="Caminho para GOOGLE_SERVICE_ACCOUNT_JSON não definido ou inválido."):
        authenticate_gdrive()

@patch.dict(os.environ, {'GOOGLE_SERVICE_ACCOUNT_JSON': '/fake/path/creds.json'})
@patch('ingestion.gdrive_ingest.os.path.exists', return_value=True)
@patch('ingestion.gdrive_ingest.service_account.Credentials.from_service_account_file')
@patch('ingestion.gdrive_ingest.build')
def test_authenticate_gdrive_build_error(
    mock_build: MagicMock,
    mock_from_service_account: MagicMock,
    mock_exists: MagicMock
):
    """Testa falha durante a chamada a build()."""
    mock_credentials = MagicMock()
    mock_from_service_account.return_value = mock_credentials
    mock_build.side_effect = HttpError(MagicMock(status=403), b"Forbidden") # Simula HttpError

    with pytest.raises(HttpError):
        authenticate_gdrive()

    mock_build.assert_called_once()

# --- Testes para download_file ---

def test_download_file_success():
    """Testa download de arquivo normal (não Google Doc)."""
    # TODO: Implementar teste
    pytest.skip("Teste não implementado")

def test_download_file_api_error():
    """Testa erro na API durante download de arquivo normal."""
    # TODO: Implementar teste
    pytest.skip("Teste não implementado")

# --- Testes para export_and_download_gdoc ---
# Adicionar testes para esta função se necessário

# --- Testes para ingest_gdrive_folder ---
# Adicionar testes focados em ingest_gdrive_folder aqui, cobrindo listagem/paginação
@patch('ingestion.gdrive_ingest.export_and_download_gdoc')
@patch('ingestion.gdrive_ingest.download_file')
@patch('ingestion.gdrive_ingest.extract_text_from_file')
@patch('ingestion.gdrive_ingest.os.makedirs')
@patch('ingestion.gdrive_ingest.open', new_callable=mock_open)
def test_ingest_gdrive_folder_list_pagination(mock_open, mock_makedirs, mock_extract, mock_download, mock_export):
    """Testa a lógica de listagem e paginação dentro de ingest_gdrive_folder."""
    mock_service = MagicMock()
    temp_dir = "/fake/temp/list"
    folder_id = "folder_page_test"
    folder_name = "PagedFolder"

    # Configurar o mock da chamada list().execute() para simular paginação
    mock_files_resource = MagicMock()
    mock_list_method = MagicMock()
    mock_service.files.return_value = mock_files_resource
    mock_files_resource.list.return_value = mock_list_method

    page1_response = {
        'files': [
            {'id': 'file_page1', 'name': 'Page1 Doc.txt', 'mimeType': 'text/plain', 'size': 100},
        ],
        'nextPageToken': 'token_page_2'
    }
    page2_response = {
        'files': [
            {'id': 'file_page2', 'name': 'Page2 Doc.docx', 'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'size': 200}
        ],
        # Sem nextPageToken
    }
    mock_list_method.execute.side_effect = [page1_response, page2_response]

    # Configurar mocks de download/extract para retornar algo simples
    mock_download.side_effect = [b"content1", b"content2"]
    mock_extract.side_effect = ["text1", "text2"]

    # Executar a função sob teste
    results = ingest_gdrive_folder(mock_service, folder_name, folder_id, temp_dir, dry_run=False)

    # Verificar se list() foi chamado corretamente com paginação
    expected_query = f"'{folder_id}' in parents and trashed=false"
    expected_fields = "nextPageToken, files(id, name, mimeType, size)"

    # Verificar argumentos da PRIMEIRA chamada a list() acessando call_args_list
    assert len(mock_files_resource.list.call_args_list) >= 1
    first_call_args = mock_files_resource.list.call_args_list[0]
    assert first_call_args == call(
        q=expected_query,
        spaces='drive',
        fields=expected_fields,
        pageToken=None,
        pageSize=100
    )

    # Verificar que o método execute() foi chamado duas vezes (uma para cada página)
    assert mock_list_method.execute.call_count == 2

    # Verificar se os arquivos de ambas as páginas foram processados
    assert len(results) == 2
    assert results[0]['id'] == 'file_page1'
    assert results[1]['id'] == 'file_page2'
    mock_download.assert_has_calls([call(mock_service, 'file_page1'), call(mock_service, 'file_page2')])
    mock_extract.assert_has_calls([
        call('text/plain', b'content1', 'Page1 Doc.txt'),
        call('application/vnd.openxmlformats-officedocument.wordprocessingml.document', b'content2', 'Page2 Doc.docx')
    ])
    # assert mock_open.call_count == 2 # REMOVIDA - open não é chamado para documentos

# --- Testes para ingest_all_gdrive_content ---

@patch.dict(os.environ, {
    'GOOGLE_SERVICE_ACCOUNT_JSON': '/fake/creds.json',
    'GDRIVE_ROOT_FOLDER_ID': 'root_folder_123',
    'GDRIVE_MARKETING_FOLDER_ID': 'mkt_folder_456' # Simular ambas as pastas
})
@patch('ingestion.gdrive_ingest.authenticate_gdrive')
# @patch('ingestion.gdrive_ingest.list_files_in_folder') # REMOVIDO patch incorreto
@patch('ingestion.gdrive_ingest.ingest_gdrive_folder') # Mockar a função que É chamada
@patch('ingestion.gdrive_ingest.tempfile.mkdtemp')
# @patch('ingestion.gdrive_ingest.shutil.rmtree') # REMOVIDO
@patch('ingestion.gdrive_ingest.os.makedirs') # Adicionado para garantir a criação do diretório
@patch('ingestion.gdrive_ingest.docling_converter') # Mock do conversor Docling
def test_ingest_all_gdrive_content_success(
    mock_docling_converter,
    mock_makedirs,
    # mock_rmtree, # REMOVIDO
    mock_mkdtemp,
    mock_ingest_folder, # Nome do mock atualizado
    mock_authenticate
):
    """Testa a ingestão completa bem-sucedida de múltiplas pastas."""
    # --- Configuração dos Mocks ---
    mock_service = MagicMock()
    mock_authenticate.return_value = mock_service
    mock_mkdtemp.return_value = "/fake/temp/dir"
    mock_docling_converter.is_initialized = True # Simular que está pronto

    # Mock da resposta de ingest_gdrive_folder para cada chamada
    # (Agora mockamos o resultado da ingestão da pasta inteira)
    ingest_result_root = [
        {"type": "document", "id": "gdoc1", "name": "Doc Root", "content": "text root", "metadata": {"access_level": "root"}},
        {"type": "video", "id": "vid1", "name": "Video Root", "path": "/path/vid1", "metadata": {"access_level": "root"}}
    ]
    ingest_result_mkt = [
        {"type": "document", "id": "pdf1", "name": "Report Mkt", "content": "text mkt", "metadata": {"access_level": "marketing"}}
    ]
    mock_ingest_folder.side_effect = [ingest_result_root, ingest_result_mkt]

    # --- Execução ---
    all_ingested_data, temp_dir_returned = ingest_all_gdrive_content(dry_run=False)

    # --- Verificações ---
    mock_authenticate.assert_called_once()
    mock_mkdtemp.assert_called_once()

    # Verificar chamadas a ingest_gdrive_folder
    assert mock_ingest_folder.call_count == 2
    
    # Verificar individualmente (ordem pode importar dependendo do dict.items())
    call_args_list = mock_ingest_folder.call_args_list
    call1_args = call_args_list[0].args
    call2_args = call_args_list[1].args

    # Verificar se ambas as pastas esperadas foram chamadas, independentemente da ordem
    expected_calls_set = {
        ('arquivos_pdc', 'root_folder_123'), 
        ('marketing_digital', 'mkt_folder_456')
    }
    actual_calls_set = {
        (call1_args[1], call1_args[2]), # (folder_name, folder_id)
        (call2_args[1], call2_args[2])  # (folder_name, folder_id)
    }
    assert actual_calls_set == expected_calls_set

    # Verificar argumentos comuns para ambas as chamadas
    assert call1_args[0] == mock_service # service
    assert call1_args[3] == "/fake/temp/dir" # temp_dir_path
    assert call1_args[4] == False # dry_run
    assert call2_args[0] == mock_service # service
    assert call2_args[3] == "/fake/temp/dir" # temp_dir_path
    assert call2_args[4] == False # dry_run

    # Verificar o resultado combinado
    assert len(all_ingested_data) == 3 # 2 do root + 1 do mkt
    assert all_ingested_data[0]["id"] == "gdoc1"
    assert all_ingested_data[1]["id"] == "vid1"
    assert all_ingested_data[2]["id"] == "pdf1"
    # Verificar se access_level está presente e correto
    assert "metadata" in all_ingested_data[0]
    assert all_ingested_data[0]["metadata"]["access_level"] == "root"
    assert "metadata" in all_ingested_data[1]
    assert all_ingested_data[1]["metadata"]["access_level"] == "root"
    assert "metadata" in all_ingested_data[2]
    assert all_ingested_data[2]["metadata"]["access_level"] == "marketing"
    assert temp_dir_returned == "/fake/temp/dir" # Verificar se o diretório temp é retornado

    # mock_rmtree.assert_called_once_with("/fake/temp/dir") # REMOVIDO

# Adicionar mais testes para ingest_all_gdrive_content (dry_run, erros, etc.)
