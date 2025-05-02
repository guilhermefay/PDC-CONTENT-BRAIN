# tests/ingestion/test_local_ingest.py

import pytest
import os
from unittest.mock import patch, MagicMock, mock_open
from unittest.mock import call

# Importar a função a ser testada
from ingestion.local_ingest import ingest_local_directory

# Testes para ingest_local_directory

@patch('ingestion.local_ingest.os.path.isdir')
@patch('ingestion.local_ingest.os.listdir')
@patch('ingestion.local_ingest.os.path.isfile')
@patch('builtins.open', new_callable=mock_open, read_data="Mock file content")
@patch('ingestion.local_ingest.logger') # Mock logger para evitar output nos testes
def test_ingest_local_directory_success(
    mock_logger: MagicMock,
    mock_file_open: MagicMock,
    mock_isfile: MagicMock,
    mock_listdir: MagicMock,
    mock_isdir: MagicMock
):
    """Testa a ingestão bem-sucedida de arquivos locais."""
    # Configurar Mocks
    test_dir = "/fake/dir"
    mock_isdir.return_value = True
    mock_listdir.return_value = ["file1.txt", "file2.md", "subdir", "file3.txt", "other.log"]
    
    # os.path.isfile precisa retornar True para os arquivos que queremos ler
    def isfile_side_effect(path):
        if path.endswith((".txt", ".md")):
            return True
        return False
    mock_isfile.side_effect = isfile_side_effect
    
    # Chamar a função
    allowed_ext = ['.txt', '.md']
    result = ingest_local_directory(test_dir, allowed_extensions=allowed_ext)
    
    # Verificar Asserts
    mock_isdir.assert_called_once_with(test_dir)
    mock_listdir.assert_called_once_with(test_dir)
    
    # Verificar isfile foi chamado para todos os itens listados
    expected_isfile_calls = [
        call(os.path.join(test_dir, f)) 
        for f in ["file1.txt", "file2.md", "subdir", "file3.txt", "other.log"]
    ]
    mock_isfile.assert_has_calls(expected_isfile_calls, any_order=True)
    assert mock_isfile.call_count == 5

    # Verificar que open foi chamado para os arquivos corretos
    expected_open_calls = [
        call(os.path.join(test_dir, "file1.txt"), 'r', encoding='utf-8'),
        call(os.path.join(test_dir, "file2.md"), 'r', encoding='utf-8'),
        call(os.path.join(test_dir, "file3.txt"), 'r', encoding='utf-8'),
    ]
    mock_file_open.assert_has_calls(expected_open_calls, any_order=True)
    assert mock_file_open.call_count == 3 # Apenas 3 arquivos válidos

    # Verificar o resultado retornado
    assert len(result) == 3
    assert result[0]["content"] == "Mock file content"
    assert result[0]["metadata"]["source_filename"] == "file1.txt"
    assert result[0]["metadata"]["source_type"] == "local_file"
    assert result[1]["metadata"]["source_filename"] == "file2.md"
    assert result[2]["metadata"]["source_filename"] == "file3.txt"
    
    # Verificar logs (opcional)
    # mock_logger.info.assert_any_call("Processing local file: file1.txt")

@patch('ingestion.local_ingest.os.path.isdir')
@patch('ingestion.local_ingest.os.listdir') # Mockar mesmo que não deva ser chamado
@patch('ingestion.local_ingest.logger')
def test_ingest_local_directory_not_found(
    mock_logger: MagicMock,
    mock_listdir: MagicMock,
    mock_isdir: MagicMock
):
    """Testa o comportamento quando o diretório não existe."""
    test_dir = "/non/existent/dir"
    mock_isdir.return_value = False
    
    result = ingest_local_directory(test_dir)
    
    # Verificar Asserts
    mock_isdir.assert_called_once_with(test_dir)
    mock_listdir.assert_not_called() # Não deve tentar listar o diretório
    assert result == [] # Deve retornar lista vazia
    mock_logger.error.assert_called_once_with(f"Directory not found: {test_dir}")

@patch('ingestion.local_ingest.os.path.isdir')
@patch('ingestion.local_ingest.os.listdir')
@patch('ingestion.local_ingest.os.path.isfile')
@patch('builtins.open', new_callable=mock_open)
@patch('ingestion.local_ingest.logger')
def test_ingest_local_directory_read_error(
    mock_logger: MagicMock,
    mock_file_open: MagicMock,
    mock_isfile: MagicMock,
    mock_listdir: MagicMock,
    mock_isdir: MagicMock
):
    """Testa o tratamento de erro ao ler um arquivo."""
    # Configurar Mocks
    test_dir = "/fake/dir"
    mock_isdir.return_value = True
    mock_listdir.return_value = ["good_file.txt", "bad_file.txt", "another_good.txt"]
    mock_isfile.return_value = True # Assumir que todos são arquivos .txt válidos
    
    # Configurar mock_open para falhar no segundo arquivo
    def open_side_effect(path, *args, **kwargs):
        if "bad_file.txt" in path:
            raise OSError("Permission denied")
        else:
            # Usar mock_open padrão para os outros arquivos
            # Precisamos recriar o comportamento do mock_open para leitura
            m = mock_open(read_data="Good content").return_value
            return m
            
    mock_file_open.side_effect = open_side_effect
    
    # Chamar a função
    result = ingest_local_directory(test_dir, allowed_extensions=['.txt'])
    
    # Verificar Asserts
    mock_isdir.assert_called_once_with(test_dir)
    mock_listdir.assert_called_once_with(test_dir)
    assert mock_isfile.call_count == 3

    # Verificar que open foi chamado para todos os arquivos
    expected_open_calls = [
        call(os.path.join(test_dir, "good_file.txt"), 'r', encoding='utf-8'),
        call(os.path.join(test_dir, "bad_file.txt"), 'r', encoding='utf-8'),
        call(os.path.join(test_dir, "another_good.txt"), 'r', encoding='utf-8'),
    ]
    mock_file_open.assert_has_calls(expected_open_calls, any_order=True)
    assert mock_file_open.call_count == 3

    # Verificar o resultado retornado (apenas arquivos bons)
    assert len(result) == 2 
    assert result[0]["metadata"]["source_filename"] == "good_file.txt"
    assert result[0]["content"] == "Good content"
    assert result[1]["metadata"]["source_filename"] == "another_good.txt"
    assert result[1]["content"] == "Good content"
    
    # Verificar log de erro
    mock_logger.error.assert_called_once()
    args_error, kwargs_error = mock_logger.error.call_args
    assert "Error reading file bad_file.txt" in args_error[0]
    assert "Permission denied" in args_error[0]

@patch('ingestion.local_ingest.os.path.isdir')
@patch('ingestion.local_ingest.os.listdir')
@patch('ingestion.local_ingest.os.path.isfile')
@patch('builtins.open', new_callable=mock_open, read_data="Content")
@patch('ingestion.local_ingest.logger')
def test_ingest_local_directory_dry_run_limit(
    mock_logger: MagicMock,
    mock_file_open: MagicMock,
    mock_isfile: MagicMock,
    mock_listdir: MagicMock,
    mock_isdir: MagicMock
):
    """Testa a funcionalidade de dry_run_limit."""
    # Configurar Mocks
    test_dir = "/fake/dir"
    mock_isdir.return_value = True
    # Lista com mais arquivos que o limite
    mock_listdir.return_value = ["file1.txt", "file2.txt", "file3.txt", "file4.txt"]
    mock_isfile.return_value = True # Todos são .txt válidos
    
    limit = 2
    
    # Chamar a função com dry_run e dry_run_limit
    result = ingest_local_directory(
        test_dir, 
        allowed_extensions=['.txt'], 
        dry_run=True, 
        dry_run_limit=limit
    )
    
    # Verificar Asserts
    mock_isdir.assert_called_once_with(test_dir)
    mock_listdir.assert_called_once_with(test_dir)
    
    # Verificar que isfile e open foram chamados apenas para o limite
    assert mock_isfile.call_count == limit
    assert mock_file_open.call_count == limit
    
    # Verificar que os arquivos processados foram os primeiros N
    expected_open_calls = [
        call(os.path.join(test_dir, "file1.txt"), 'r', encoding='utf-8'),
        call(os.path.join(test_dir, "file2.txt"), 'r', encoding='utf-8'),
    ]
    # Usar mock_calls para verificar a ordem também, se necessário
    # mock_file_open.assert_has_calls(expected_open_calls, any_order=False)
    # CORRIGIDO: Verificar apenas a contagem de chamadas, pois assert_has_calls falha com __enter__ etc.
    assert mock_file_open.call_count == limit

    # Verificar o resultado (deve estar vazio em dry_run)
    assert len(result) == limit
    assert result[0]["metadata"]["source_filename"] == "file1.txt"
    assert result[1]["metadata"]["source_filename"] == "file2.txt"

    # Verificar log sobre o limite atingido
    mock_logger.info.assert_any_call(f"Dry run limit ({limit}) reached. Stopping ingestion.")

# Adicionar mais testes conforme necessário (e.g., dry_run sem limite) 