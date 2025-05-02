# tests/integration/test_full_etl.py

import pytest
from unittest.mock import patch, MagicMock, ANY
import os

# Import the function to be tested
from etl.annotate_and_index import run_pipeline

# We no longer need to import the actual clients for type hinting if we patch correctly
# from supabase import Client as SupabaseClient 
# from infra.r2r_client import R2RClientWrapper


@pytest.mark.integration
# Patch the dependencies WHERE THEY ARE USED/IMPORTED within run_pipeline or its callees
@patch('etl.annotate_and_index.ingest_all_gdrive_content', autospec=True)
@patch('etl.annotate_and_index.process_all_videos_in_directory', autospec=True) # Assumes assemblyai logic is here
# ---- PATCH TARGETS CORRECTED ----
@patch('etl.annotate_and_index.create_client', autospec=True) # Patch create_client where it's called in run_pipeline
@patch('etl.annotate_and_index.R2RClientWrapper', autospec=True) # Patch the class constructor
@patch('etl.annotate_and_index.AnnotatorAgent', autospec=True) # Patch the class constructor
def test_full_etl_pipeline_success(
    # Mocks are now for the *patched objects/functions*
    MockAnnotatorAgent: MagicMock,          # Mock for the AnnotatorAgent class
    MockR2RClientWrapper: MagicMock,        # Mock for the R2RClientWrapper class
    mock_create_supabase_client: MagicMock, # Mock for the create_client function
    mock_process_videos: MagicMock, 
    mock_ingest_gdrive: MagicMock
):
    """
    Test the full ETL pipeline execution with mocked external services/functions.
    Covers the scenario where all steps succeed.
    """
    # --- Mock Configuration ---
    # Mock GDrive Ingestion function (remains the same)
    # **Crucially, this mock now returns the *transcribed* content directly**
    # and metadata expected by process_single_source_document
    mock_gdrive_audio_transcript = "Este é o texto transcrito do arquivo de áudio mockado."
    mock_ingested_files_list = [
        {
            # Data structure expected by process_single_source_document
            'content': mock_gdrive_audio_transcript, # The transcript!
            'metadata': {
                'source_name': 'audio1.mp3', # Original filename
                'origin': 'gdrive',
                'gdrive_id': 'gdrive_audio1', # GDrive file ID
                'mimeType': 'audio/mpeg', # Original mimeType
                'temp_path': '/tmp/mock_audio1.mp3' # Path needed for R2R upload
            }
        },
        {
            # Non-audio file, content might be empty or actual text
            'content': "Conteúdo de texto simples.",
            'metadata': {
                'source_name': 'document.txt',
                'origin': 'gdrive',
                'gdrive_id': 'gdrive_doc1',
                'mimeType': 'text/plain',
                'temp_path': '/tmp/mock_doc1.txt'
            }
        }
    ]
    # ** FIX: Return a tuple (list_of_files, temp_dir_path) **
    mock_ingest_gdrive.return_value = (mock_ingested_files_list, "/tmp/mock_video_dir") 

    # Mock Video/AssemblyAI Processing function (remains the same)
    mock_process_videos.return_value = []

    # --- Configure Mocks Returned by Patched Objects ---
    # Get the *instance* returned by the patched Supabase create_client function
    mock_supabase_instance = mock_create_supabase_client.return_value 

    # Get the *instance* returned by the patched R2RClientWrapper constructor
    mock_r2r_client_instance = MockR2RClientWrapper.return_value

    # Get the *instance* returned by the patched AnnotatorAgent constructor
    mock_annotator_instance = MockAnnotatorAgent.return_value
    # Mock the 'run' method of the annotator instance
    # Simulate annotation skipping by marking all chunks with keep=True
    def mock_annotator_run(chunks):
        return [{**chunk, 'keep': True, 'tags': ['interno', 'tecnico'], 'reason': 'Anotação pulada via flag'} for chunk in chunks]
    mock_annotator_instance.run.side_effect = mock_annotator_run

    # Configure the Supabase Instance Mock (as before, but using the instance from create_client)
    mock_processed_files_table = MagicMock(name='processed_files_table_mock')
    mock_select_query = MagicMock(name='select_query_mock')
    mock_select_query.execute.return_value = MagicMock(count=0, name='select_execute_result')
    mock_eq_filter = MagicMock(name='eq_filter_mock')
    mock_eq_filter.execute.return_value = MagicMock(count=0, name='eq_execute_result')
    mock_processed_files_table.select.return_value.eq.return_value = mock_eq_filter
    mock_upsert_call = MagicMock(name='upsert_call_mock')
    mock_upsert_call.execute.return_value = MagicMock(data=[{'upserted': 'data'}], name='upsert_execute_result')
    mock_processed_files_table.upsert.return_value = mock_upsert_call

    mock_documents_table = MagicMock(name='documents_table_mock')
    mock_insert_call = MagicMock(name='insert_call_mock')
    mock_insert_call.execute.return_value = MagicMock(data=[{'inserted': 'data'}], name='insert_execute_result')
    mock_documents_table.insert.return_value = mock_insert_call

    def table_side_effect(table_name):
        if table_name == 'processed_files':
            return mock_processed_files_table
        elif table_name == 'documents':
            return mock_documents_table
        else:
            return MagicMock(name=f'{table_name}_table_mock')

    mock_supabase_instance.table.side_effect = table_side_effect

    # Configure the R2R Instance Mock (as before, but using the instance from constructor)
    mock_r2r_client_instance.upload_file.return_value = {'success': True, 'document_id': 'r2r_doc1'}
    
    # --- Execute Pipeline ---
    run_pipeline(
        source='gdrive',
        local_dir=None,
        dry_run=False,
        dry_run_limit=None,
        skip_annotation=False, # Set to False to test the mocked annotator run
        skip_indexing=False,
        max_workers_r2r_upload=1
    )

    # --- Assertions ---
    # Verify GDrive Ingestion call
    mock_ingest_gdrive.assert_called_once()

    # Verify Video Processing was NOT called
    mock_process_videos.assert_not_called()

    # Verify Constructors/Functions were called
    mock_create_supabase_client.assert_called_once()
    MockR2RClientWrapper.assert_called_once()
    MockAnnotatorAgent.assert_called_once()
    mock_annotator_instance.run.assert_called()

    # Verify Supabase calls
    # Check if audio file was checked
    mock_supabase_instance.table.assert_any_call('processed_files')
    mock_processed_files_table.select.assert_any_call('file_id', count='exact')
    mock_processed_files_table.select.return_value.eq.assert_any_call('file_id', 'gdrive_audio1')
    mock_processed_files_table.select.return_value.eq.assert_any_call('file_id', 'gdrive_doc1')
    # Check execute was called twice on the select query object
    assert mock_eq_filter.execute.call_count == 2

    # Verify calls to 'documents' table for inserting content chunks
    mock_supabase_instance.table.assert_any_call('documents') # Check table was called
    mock_documents_table.insert.assert_called() # Check insert was called
    assert mock_documents_table.insert.call_count == 2
    mock_insert_call.execute.assert_called() # Check execute was called after insert
    assert mock_insert_call.execute.call_count == 2

    # Verify calls to 'processed_files' table for marking as processed (upsert)
    mock_supabase_instance.table.assert_any_call('processed_files') # Ensure table was called again for upsert
    mock_processed_files_table.upsert.assert_any_call({
        'file_id': 'gdrive_audio1',
        'status': 'processed',
        'source': 'gdrive',
        'last_processed_at': ANY
    })
    mock_processed_files_table.upsert.assert_any_call({
        'file_id': 'gdrive_doc1',
        'status': 'processed',
        'source': 'gdrive',
        'last_processed_at': ANY
    })
    assert mock_processed_files_table.upsert.call_count == 2
    mock_upsert_call.execute.assert_called() # Check execute was called after upsert
    assert mock_upsert_call.execute.call_count == 2

    # Verify R2R call (should only happen for chunks marked 'keep' by annotation)
    # Since we skipped annotation with keep=True, R2R should be called for both files' chunks
    mock_gdrive_audio_transcript = "Este é o texto transcrito do arquivo de áudio mockado." # Redefine for clarity
    mock_text_content = "Conteúdo de texto simples." # Redefine for clarity
    
    # Check call for audio file chunk
    # **NOTE:** Using ANY for file_path as it's a temporary file.
    # **NOTE:** Using ANY for document_id as it's generated internally in process_single_source_document
    # **NOTE:** Calculate token count more accurately if possible or use ANY
    from etl.annotate_and_index import count_tokens # Import for assertion
    
    mock_r2r_client_instance.upload_file.assert_any_call(
        document_id=ANY, # Document ID is now generated inside the function
        file_path=ANY, # Path is temporary
        metadata={
            'source_name': 'audio1.mp3',
            'origin': 'gdrive',
            'gdrive_id': 'gdrive_audio1',
            'mimeType': 'audio/mpeg',
            'temp_path': '/tmp/mock_audio1.mp3',
            'chunk_index': 0, # Assuming simple chunking
            'tags': ['interno', 'tecnico'], # From skipped annotation
            'reason': 'Anotação pulada via flag', # From skipped annotation
            'token_count': count_tokens(mock_gdrive_audio_transcript) # Use actual count
        }
    )
    # Check call for text file chunk
    mock_r2r_client_instance.upload_file.assert_any_call(
        document_id=ANY,
        file_path=ANY,
        metadata={
            'source_name': 'document.txt',
            'origin': 'gdrive',
            'gdrive_id': 'gdrive_doc1',
            'mimeType': 'text/plain',
            'temp_path': '/tmp/mock_doc1.txt',
            'chunk_index': 0, # Assuming simple chunking
            'tags': ['interno', 'tecnico'],
            'reason': 'Anotação pulada via flag',
            'token_count': count_tokens(mock_text_content)
        }
    )
    # Assert R2R was called twice (once per file, assuming one chunk each and keep=True)
    assert mock_r2r_client_instance.upload_file.call_count == 2


# TODO: Add more integration test cases:
# ... (rest of the todos remain) ...