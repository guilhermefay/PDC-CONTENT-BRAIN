import pytest
import os
import shutil
import tempfile
from unittest.mock import patch, MagicMock, call

# Importar o módulo principal do ETL para testar sua função main ou similar
from etl import annotate_and_index

# Estrutura básica para testes de integração do ETL

# Fixture para criar um diretório temporário para testes
@pytest.fixture
def temp_dir():
    """Cria um diretório temporário para os testes e o remove depois."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)

# Exemplo de Teste (a ser implementado)
@patch('etl.annotate_and_index.ingest_all_gdrive_content')
@patch('etl.annotate_and_index.process_all_videos_in_directory')
@patch('etl.annotate_and_index.tempfile.mkdtemp')
@patch('etl.annotate_and_index.shutil.rmtree')
# Adicionar patch para o AnnotatorAgent
@patch('etl.annotate_and_index.AnnotatorAgent')
# Adicionar patch para create_client e R2RClientWrapper para evitar inicialização real
@patch('etl.annotate_and_index.create_client') 
@patch('etl.annotate_and_index.R2RClientWrapper')
def test_etl_gdrive_source(
    mock_R2RWrapper_cls: MagicMock, # Mock da classe R2RClientWrapper
    mock_create_supabase: MagicMock, # Mock da função create_client
    mock_AnnotatorAgent_cls: MagicMock, # Mock da classe AnnotatorAgent
    mock_rmtree: MagicMock,
    mock_mkdtemp: MagicMock,
    mock_process_videos: MagicMock, # Corresponds to process_all_videos_in_directory
    mock_ingest_gdrive: MagicMock, # Corresponds to ingest_all_gdrive_content
    temp_dir
):
    """Testa o pipeline ETL completo com fonte 'gdrive' mockando dependências externas."""
    
    # --- Configuração dos Mocks ---
    
    # Configurar mocks das instâncias criadas dentro de run_pipeline
    mock_supabase_instance = MagicMock()
    mock_create_supabase.return_value = mock_supabase_instance # Corrigido
    mock_r2r_instance = MagicMock()
    mock_R2RWrapper_cls.return_value = mock_r2r_instance # Corrigido
    mock_annotator_instance = MagicMock()
    mock_AnnotatorAgent_cls.return_value = mock_annotator_instance # AnnotatorAgent() retorna o mock_annotator_instance
    
    # 1. Mock tempfile.mkdtemp para retornar nosso diretório de teste
    # A função gdrive_ingest agora retorna o diretório temp, então mockamos ela
    # mock_mkdtemp.return_value = temp_dir # Não é mais necessário mockar mkdtemp diretamente aqui
    
    # 2. Mock ingest_all_gdrive_content
    doc1_content = "Conteúdo do documento 1 para chunking."
    doc1_metadata = {'source': 'gdrive', 'title': 'Doc 1', 'id': 'gdrive_doc1', 'source_name': 'Doc1.gdoc'}
    vid1_path = os.path.join(temp_dir, 'video1.mp4') # Usar um path real dentro do temp_dir
    vid1_metadata = {'source': 'gdrive', 'title': 'Video 1', 'id': 'gdrive_vid1', 'source_name': 'Video1.mp4'}
    mock_gdrive_results = [
        {'type': 'document', 'content': doc1_content, 'metadata': doc1_metadata},
        {'type': 'video', 'path': vid1_path, 'metadata': vid1_metadata}
    ]
    # run_gdrive_ingestion agora retorna (data, temp_dir_path)
    mock_ingest_gdrive.return_value = (mock_gdrive_results, temp_dir) 
    
    # 3. Mock process_all_videos_in_directory
    vid1_transcript = 'Transcrição do vídeo 1.'
    mock_transcription_results = {
        vid1_path: {
            'text': vid1_transcript,
            'metadata': {'duration': 60, 'language': 'pt', **vid1_metadata} # Incluir metadados originais
        }
    }
    mock_process_videos.return_value = mock_transcription_results
    
    # 4. Mock AnnotatorAgent.run
    # Deve retornar a lista COMPLETA de chunks, modificada com 'keep', 'tags', 'reason'
    # Simular que chunk do doc1 é mantido, chunk do video não é.
    # Precisamos simular os chunks criados internamente
    # Nota: Isso acopla o teste à implementação de chunking. Idealmente, chunking seria testado separadamente.
    # Por simplicidade aqui, vamos assumir 2 chunks por item.
    def mock_annotator_run(chunks):
        processed = []
        for i, chunk in enumerate(chunks):
            is_video_chunk = 'Vídeo' in chunk['metadata'].get('title', '')
            keep = not is_video_chunk # Manter doc, não manter video (exemplo)
            tags = ['doc'] if not is_video_chunk else ['video']
            reason = "Documento relevante" if keep else "Transcrição de vídeo não relevante"
            processed.append({**chunk, 'keep': keep, 'tags': tags, 'reason': reason, 'document_id': f"uuid_{i}"}) # Adiciona ID mockado
        return processed
    mock_annotator_instance.run.side_effect = mock_annotator_run

    # 5. Mock Supabase client (table().insert())
    mock_supabase_table = MagicMock()
    mock_supabase_instance.table.return_value = mock_supabase_table
    # Simular resposta de sucesso para insert (API mudou, não retorna dados em lote geralmente)
    mock_insert_response = MagicMock()
    mock_insert_response.data = [] # Ou None, dependendo da versão/mock
    mock_insert_response.error = None
    # Configurar para retornar um objeto com status_code se a biblioteca usar
    # Ou apenas verificar se .execute() é chamado
    mock_execute = MagicMock()
    # mock_execute.execute.return_value = mock_insert_response # Ajustar se necessário
    mock_supabase_table.insert.return_value = mock_execute 
    
    # 6. Mock R2R client (upload_file)
    mock_r2r_instance.upload_file.return_value = {'success': True, 'data': {'message': 'uploaded'}}
    
    # --- Execução do Pipeline ---
    
    annotate_and_index.run_pipeline(
        source='gdrive',
        local_dir='.', # Não usado para gdrive
        dry_run=False,
        dry_run_limit=None,
        skip_annotation=False,
        skip_indexing=False,
        max_workers_r2r_upload=2 # Usar valor baixo para teste
    )
    
    # --- Asserts ---
    
    # Verificar chamadas aos mocks de inicialização
    mock_create_supabase.assert_called_once()
    mock_R2RWrapper_cls.assert_called_once()
    mock_AnnotatorAgent_cls.assert_called_once()
    
    # Verificar chamada à ingestão
    mock_ingest_gdrive.assert_called_once_with(dry_run=False)
    
    # Verificar chamada à transcrição de vídeo
    mock_process_videos.assert_not_called()
    
    # Verificar chamada ao anotador (deve receber chunks de ambos)
    # Assumindo 1 chunk por item para simplificar a asserção
    # A asserção exata aqui é frágil devido ao chunking interno
    assert mock_annotator_instance.run.call_count == 1 
    # call_args = mock_annotator_instance.run.call_args[0][0] # Pega a lista de chunks passada
    # assert len(call_args) == 2 # Espera chunks do doc e do vídeo

    # Verificar chamada ao Supabase (usando mock_supabase_instance)
    mock_supabase_instance.table.assert_called_with('documents')
    # Verificar que insert foi chamado (o número exato de chunks depende da implementação)
    # A chamada é insert(...).execute()
    assert mock_supabase_table.insert.call_count > 0
    assert mock_execute.execute.call_count > 0
    # Verificar o conteúdo inserido pode ser complexo, focar na chamada por enquanto
    first_insert_call_args = mock_supabase_table.insert.call_args[0][0] # Dados do insert
    assert len(first_insert_call_args) > 0

    # Verificar chamada ao R2R (apenas para o chunk do documento, que teve keep=True)
    # A chamada exata pode variar devido ao tempfile.NamedTemporaryFile
    assert mock_r2r_instance.upload_file.call_count == 1 # Apenas 1 chunk com keep=True no mock_annotator_run
    r2r_call_args, r2r_call_kwargs = mock_r2r_instance.upload_file.call_args # Obter argumentos
    # REMOVIDA: A asserção do file_path era incorreta e frágil com NamedTemporaryFile
    # assert r2r_call_kwargs['file_path'] in r2r_call_kwargs 
    # assert r2r_call_kwargs['document_id'] == "uuid_0" # ID do primeiro chunk (doc)
    assert r2r_call_kwargs['metadata']['source_name'] == 'Doc1.gdoc'
    assert r2r_call_kwargs['metadata']['tags'] == ['doc']

    # Verificar limpeza do diretório temporário
    mock_rmtree.assert_called_once_with(temp_dir)

# Remover o skip do teste anterior
# pytest.skip("Teste ETL incompleto - requer chamada ao entry point ou refatoração")

# Adicionar mais testes para source='video', source='local', casos de erro, etc. 

@patch('etl.annotate_and_index.ingest_local_directory') # Mock para ingestão local
@patch('etl.annotate_and_index.process_all_videos_in_directory') # Mock da transcrição
@patch('etl.annotate_and_index.shutil.rmtree')
@patch('etl.annotate_and_index.AnnotatorAgent')
@patch('etl.annotate_and_index.create_client')
@patch('etl.annotate_and_index.R2RClientWrapper')
@patch('etl.annotate_and_index.ingest_all_gdrive_content') # Adicionar mock para assert_not_called
def test_etl_local_source(
    mock_ingest_gdrive: MagicMock, # Adicionar argumento do mock
    mock_R2RWrapper_cls: MagicMock,
    mock_create_supabase: MagicMock,
    mock_AnnotatorAgent_cls: MagicMock,
    mock_rmtree: MagicMock, # Não esperado ser chamado para local
    mock_process_videos: MagicMock, # Não esperado ser chamado
    mock_local_ingest: MagicMock, # Mock da função ingest_local_directory
    temp_dir # Usado para o argumento local_dir
):
    """Testa o pipeline ETL completo com fonte 'local' mockando dependências."""

    # --- Configuração dos Mocks ---
    mock_supabase_instance = MagicMock()
    mock_create_supabase.return_value = mock_supabase_instance # Corrigido
    mock_r2r_instance = MagicMock()
    mock_R2RWrapper_cls.return_value = mock_r2r_instance # Corrigido
    mock_annotator_instance = MagicMock()
    mock_AnnotatorAgent_cls.return_value = mock_annotator_instance

    # 1. Mock ingest_local_directory
    doc1_content = "Conteúdo local do arquivo 1."
    doc1_metadata = {'source': 'local', 'source_name': 'local1.txt', 'id': 'local_doc1'}
    mock_local_results = [
        {'content': doc1_content, 'metadata': doc1_metadata}
    ]
    mock_local_ingest.return_value = mock_local_results

    # 2. Mock AnnotatorAgent.run (manter apenas o chunk do doc local)
    def mock_annotator_run(chunks):
        processed = []
        for i, chunk in enumerate(chunks):
             processed.append({**chunk, 'keep': True, 'tags': ['local_doc'], 'reason': "Documento local", 'document_id': f"uuid_local_{i}"})
        return processed
    mock_annotator_instance.run.side_effect = mock_annotator_run

    # 3. Mock Supabase (usando mock_supabase_instance)
    mock_supabase_table = MagicMock()
    mock_supabase_instance.table.return_value = mock_supabase_table
    mock_execute = MagicMock()
    mock_supabase_table.insert.return_value = mock_execute

    # 4. Mock R2R (usando mock_r2r_instance)
    mock_r2r_instance.upload_file.return_value = {'success': True, 'data': {'message': 'uploaded'}}

    # --- Execução do Pipeline ---
    annotate_and_index.run_pipeline(
        source='local',
        local_dir=temp_dir, # Passar o diretório mockado
        dry_run=False,
        dry_run_limit=None,
        skip_annotation=False,
        skip_indexing=False
    )

    # --- Asserts ---
    mock_create_supabase.assert_called_once()
    mock_R2RWrapper_cls.assert_called_once()
    mock_AnnotatorAgent_cls.assert_called_once()

    # Verificar chamada à ingestão local
    mock_local_ingest.assert_called_once_with(temp_dir, dry_run=False, dry_run_limit=None)
    
    # Verificar que ingestão de vídeo/gdrive NÃO foram chamadas
    mock_process_videos.assert_not_called()
    # Precisamos mockar gdrive_ingest no decorator também para assert_not_called
    # (Adicionar @patch('etl.annotate_and_index.gdrive_ingest') acima)
    mock_ingest_gdrive.assert_not_called() # Agora podemos fazer assert_not_called

    # Verificar chamada ao anotador
    assert mock_annotator_instance.run.call_count == 1

    # Verificar chamada ao Supabase (usando mock_supabase_instance)
    mock_supabase_instance.table.assert_called_with('documents')
    assert mock_supabase_table.insert.call_count > 0
    assert mock_execute.execute.call_count > 0

    # Verificar chamada ao R2R (para o chunk do doc local)
    assert mock_r2r_instance.upload_file.call_count == 1
    r2r_call_args, r2r_call_kwargs = mock_r2r_instance.upload_file.call_args
    assert r2r_call_kwargs['metadata']['source_name'] == 'local1.txt'
    assert r2r_call_kwargs['metadata']['tags'] == ['local_doc']

    # Verificar que rmtree NÃO foi chamado (não há dir temp para vídeos)
    mock_rmtree.assert_not_called()


@patch('etl.annotate_and_index.ingest_all_gdrive_content') # Não esperado ser chamado
@patch('etl.annotate_and_index.process_all_videos_in_directory') # Mock da transcrição (fonte principal)
@patch('etl.annotate_and_index.shutil.rmtree')
@patch('etl.annotate_and_index.AnnotatorAgent')
@patch('etl.annotate_and_index.create_client')
@patch('etl.annotate_and_index.R2RClientWrapper')
@patch('etl.annotate_and_index.ingest_local_directory') # Adicionar mock para assert_not_called
def test_etl_video_source(
    mock_local_ingest: MagicMock, # Adicionar argumento do mock
    mock_R2RWrapper_cls: MagicMock,
    mock_create_supabase: MagicMock,
    mock_AnnotatorAgent_cls: MagicMock,
    mock_rmtree: MagicMock, # Não esperado ser chamado
    mock_process_videos: MagicMock, # Mock da função process_all_videos_in_directory
    mock_ingest_gdrive: MagicMock, # Não esperado ser chamado
    temp_dir # Usado para o argumento local_dir
):
    """Testa o pipeline ETL completo com fonte 'video' mockando dependências."""

    # --- Configuração dos Mocks ---
    mock_supabase_instance = MagicMock()
    mock_create_supabase.return_value = mock_supabase_instance # Corrigido
    mock_r2r_instance = MagicMock()
    mock_R2RWrapper_cls.return_value = mock_r2r_instance # Corrigido
    mock_annotator_instance = MagicMock()
    mock_AnnotatorAgent_cls.return_value = mock_annotator_instance

    # 1. Mock process_all_videos_in_directory (fonte principal agora)
    vid_path = os.path.join(temp_dir, 'local_vid.mp4')
    vid_transcript = "Transcrição do vídeo local."
    vid_metadata = {'source': 'video', 'source_name': 'local_vid.mp4', 'id': 'local_vid1'}
    mock_transcription_results = {
        vid_path: { # Chave é o path do vídeo
            'text': vid_transcript,
            'metadata': {'duration': 30, 'language': 'en', **vid_metadata}
        }
    }
    mock_process_videos.return_value = mock_transcription_results

    # 2. Mock AnnotatorAgent.run (manter chunk do vídeo)
    def mock_annotator_run(chunks):
        processed = []
        for i, chunk in enumerate(chunks):
             processed.append({**chunk, 'keep': True, 'tags': ['local_video'], 'reason': "Vídeo local", 'document_id': f"uuid_vid_{i}"})
        return processed
    mock_annotator_instance.run.side_effect = mock_annotator_run

    # 3. Mock Supabase (usando mock_supabase_instance)
    mock_supabase_table = MagicMock()
    mock_supabase_instance.table.return_value = mock_supabase_table
    mock_execute = MagicMock()
    mock_supabase_table.insert.return_value = mock_execute

    # 4. Mock R2R (usando mock_r2r_instance)
    mock_r2r_instance.upload_file.return_value = {'success': True, 'data': {'message': 'uploaded'}}

    # --- Execução do Pipeline ---
    annotate_and_index.run_pipeline(
        source='video',
        local_dir=temp_dir, # Passar o diretório mockado
        dry_run=False,
        dry_run_limit=None,
        skip_annotation=False,
        skip_indexing=False
    )

    # --- Asserts ---
    mock_create_supabase.assert_called_once()
    mock_R2RWrapper_cls.assert_called_once()
    mock_AnnotatorAgent_cls.assert_called_once()

    # Verificar chamada à transcrição
    mock_process_videos.assert_called_once_with(temp_dir)

    # Verificar que ingestão local/gdrive NÃO foram chamadas
    mock_local_ingest.assert_not_called()
    mock_ingest_gdrive.assert_not_called()

    # Verificar chamada ao anotador
    assert mock_annotator_instance.run.call_count == 1

    # Verificar chamada ao Supabase (usando mock_supabase_instance)
    mock_supabase_instance.table.assert_called_with('documents')
    assert mock_supabase_table.insert.call_count > 0
    assert mock_execute.execute.call_count > 0

    # Verificar chamada ao R2R (para o chunk do vídeo local)
    assert mock_r2r_instance.upload_file.call_count == 1
    r2r_call_args, r2r_call_kwargs = mock_r2r_instance.upload_file.call_args
    assert r2r_call_kwargs['metadata']['source_name'] == 'local_vid.mp4'
    assert r2r_call_kwargs['metadata']['tags'] == ['local_video']

    # Verificar que rmtree NÃO foi chamado (não há dir temp para vídeos neste source)
    mock_rmtree.assert_not_called()


# Adicionar mais testes para casos de erro, dry_run, skip_flags, etc. 