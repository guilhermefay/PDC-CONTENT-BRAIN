import os
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

# Importar funções alvo
import ingestion.gdrive_ingest as gdrive_mod
import ingestion.video_transcription as vid_mod

# ---------------------------------------------------------------------------
# Helpers e fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def restore_docling_converter():
    """Garante que docling_converter original é restaurado após cada teste."""
    original = gdrive_mod.docling_converter
    yield
    gdrive_mod.docling_converter = original

# ---------------------------------------------------------------------------
# extract_text_from_file (gdrive_ingest)
# ---------------------------------------------------------------------------

def test_extract_text_plain_utf8():
    content = "Olá mundo".encode("utf-8")
    text = gdrive_mod.extract_text_from_file("text/plain", content, "hello.txt")
    assert text == "Olá mundo"


def test_extract_text_plain_latin1_fallback():
    # byte 0xe9 é "é" em latin1
    content = b"caf\xe9"
    text = gdrive_mod.extract_text_from_file("text/plain", content, "cafe.txt")
    assert text == "café"


@patch.object(gdrive_mod, "docling_converter")
def test_extract_text_docling_success(mock_converter):
    # Configurar mock convert
    mock_result = MagicMock()
    mock_result.document.export_to_markdown.return_value = "Extracted markdown text"
    mock_converter.convert.return_value = mock_result

    content = b"binarydata"
    text = gdrive_mod.extract_text_from_file(
        gdrive_mod.GDRIVE_EXPORT_MIME,
        content,
        "doc.docx",
    )
    assert text == "Extracted markdown text"
    mock_converter.convert.assert_called_once()


@patch.object(gdrive_mod, "docling_converter")
def test_extract_text_docling_error_returns_none(mock_converter):
    mock_converter.convert.side_effect = Exception("Conversion failed")
    content = b"binarydata"
    text = gdrive_mod.extract_text_from_file(
        gdrive_mod.GDRIVE_EXPORT_MIME,
        content,
        "doc.docx",
    )
    assert text is None

# ---------------------------------------------------------------------------
# transcribe_video_assemblyai (video_transcription)
# ---------------------------------------------------------------------------

class _FakeTranscript(SimpleNamespace):
    pass


@patch.object(vid_mod, "aai")
def test_transcribe_video_assemblyai_success(mock_aai):
    mock_transcriber = MagicMock()
    ts_enum = SimpleNamespace(completed="completed", error="error")
    mock_aai.TranscriptStatus = ts_enum

    fake_transcript = _FakeTranscript(
        status="completed",
        text="Answer",
        confidence=0.95,
        audio_duration=60,
    )
    mock_transcriber.transcribe.return_value = fake_transcript
    mock_aai.Transcriber.return_value = mock_transcriber

    with patch.object(vid_mod, "ASSEMBLYAI_API_KEY", "key"):
        result = vid_mod.transcribe_video_assemblyai("/path/video.mp4")

    assert result["text"] == "Answer"
    assert result["metadata"]["transcriber"] == "assemblyai"


@patch.object(vid_mod, "aai")
def test_transcribe_video_assemblyai_error_status(mock_aai):
    mock_transcriber = MagicMock()
    ts_enum = SimpleNamespace(completed="completed", error="error")
    mock_aai.TranscriptStatus = ts_enum

    fake_transcript = _FakeTranscript(status="error", error="failed")
    mock_transcriber.transcribe.return_value = fake_transcript
    mock_aai.Transcriber.return_value = mock_transcriber

    with patch.object(vid_mod, "ASSEMBLYAI_API_KEY", "key"):
        result = vid_mod.transcribe_video_assemblyai("/path/video.mp4")
    assert result is None


def test_transcribe_video_assemblyai_missing_key():
    with patch.object(vid_mod, "ASSEMBLYAI_API_KEY", None):
        result = vid_mod.transcribe_video_assemblyai("/any/path.mp4")
    assert result is None

# ---------------------------------------------------------------------------
# transcribe_video_whisperx (video_transcription) – sucesso caminho direto
# ---------------------------------------------------------------------------

@patch.object(vid_mod, "torch")
@patch.object(vid_mod, "whisperx")
def test_transcribe_video_whisperx_direct_success(mock_whisperx, mock_torch):
    # torch tweaks
    mock_torch.cuda.is_available.return_value = False

    # whisperx mocks
    mock_whisper_model = MagicMock()
    mock_whisper_model.transcribe.return_value = {
        "language": "en",
        "segments": [
            {"text": "Hello"},
            {"text": "world"},
        ],
    }
    mock_whisperx.load_audio.return_value = "audio"
    mock_whisperx.load_model.return_value = mock_whisper_model

    result = vid_mod.transcribe_video_whisperx("/path/video.mp4")
    assert result is not None
    assert result["text"].startswith("Hello")

# ---------------------------------------------------------------------------
# transcribe_video_whisperx – fallback fails due to missing ffmpeg
# ---------------------------------------------------------------------------

@patch.object(vid_mod, "shutil")
@patch.object(vid_mod, "whisperx")
def test_transcribe_video_whisperx_ffmpeg_missing(mock_whisperx, mock_shutil):
    mock_whisperx.load_audio.side_effect = Exception("fail load audio")
    mock_shutil.which.return_value = None  # ffmpeg not found

    result = vid_mod.transcribe_video_whisperx("/path/video.mp4")
    assert result is None 