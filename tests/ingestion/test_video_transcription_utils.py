import os
import tempfile
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest

import ingestion.video_transcription as vid_mod

# ---------------------------------------------------------------------------
# transcribe_video_whisperx – caminho de fallback via ffmpeg
# ---------------------------------------------------------------------------

@patch.object(vid_mod, "torch")
@patch.object(vid_mod, "whisperx")
@patch.object(vid_mod, "subprocess")
@patch.object(vid_mod, "shutil")
def test_transcribe_video_whisperx_ffmpeg_fallback_success(mock_shutil, mock_subprocess, mock_whisperx, mock_torch):
    # 1) Primeira tentativa de load_audio falha -> Exception
    mock_whisperx.load_audio.side_effect = [Exception("fail first"), "audio-from-temp"]

    # 2) ffmpeg exists
    mock_shutil.which.return_value = "/usr/bin/ffmpeg"

    # 3) subprocess.run retorna sucesso (returncode 0)
    completed = MagicMock()
    completed.returncode = 0
    mock_subprocess.run.return_value = completed

    # 4) load_model -> retorna model com transcribe
    fake_model = MagicMock()
    fake_model.transcribe.return_value = {
        "language": "en",
        "segments": [{"text": "hello"}, {"text": "world"}],
    }
    mock_whisperx.load_model.return_value = fake_model

    # 5) torch cuda not available
    mock_torch.cuda.is_available.return_value = False

    with patch.object(vid_mod, "DEVICE", "cpu"):
        result = vid_mod.transcribe_video_whisperx("/fake/video.mp4")

    assert result is not None
    assert result["metadata"]["transcriber"] == "whisperx"
    fake_model.transcribe.assert_called_once()
    # Garantir que load_audio foi chamado duas vezes (direto e depois do ffmpeg)
    assert mock_whisperx.load_audio.call_count == 2

# ---------------------------------------------------------------------------
# process_video
# ---------------------------------------------------------------------------

@patch.object(vid_mod, "transcribe_video_assemblyai")
@patch.object(vid_mod, "transcribe_video_whisperx")
def test_process_video_paths(mock_whisper, mock_assembly):
    # Caminho 1: AssemblyAI bem-sucedido
    mock_assembly.return_value = {"text": "ok", "metadata": {}}
    res = vid_mod.process_video("/video.mp4")
    assert res["metadata"]["origin"] == "video"
    mock_assembly.assert_called_once()
    mock_whisper.assert_not_called()

    # Reset mocks
    mock_assembly.reset_mock()
    mock_whisper.reset_mock()

    # Caminho 2: Assembly falha -> WhisperX sucesso
    mock_assembly.return_value = None
    mock_whisper.return_value = {"text": "whisper", "metadata": {}}
    res = vid_mod.process_video("/video2.mp4")
    assert res["text"] == "whisper"
    mock_whisper.assert_called_once()

    # Caminho 3: Ambos falham -> None
    mock_assembly.return_value = None
    mock_whisper.return_value = None
    res = vid_mod.process_video("/video3.mp4")
    assert res is None

# ---------------------------------------------------------------------------
# process_all_videos_in_directory
# ---------------------------------------------------------------------------

def _create_temp_video_dir(tmp_path):
    dir_path = tmp_path / "videos"
    dir_path.mkdir()
    # Criar dois arquivos de vídeo e um arquivo de texto que deve ser ignorado
    (dir_path / "a.mp4").write_bytes(b"dummy")
    (dir_path / "b.MOV").write_bytes(b"dummy")
    (dir_path / "note.txt").write_text("ignore")
    return str(dir_path)

@patch.object(vid_mod, "process_video")

def test_process_all_videos_in_directory_success(mock_process_video, tmp_path):
    mock_process_video.side_effect = [
        {"text": "t1", "metadata": {}},
        None,  # Segundo vídeo gera None
    ]
    video_dir = _create_temp_video_dir(tmp_path)

    res = vid_mod.process_all_videos_in_directory(video_dir)
    assert len(res) == 1  # Apenas um sucesso
    mock_process_video.assert_called()


def test_process_all_videos_invalid_directory():
    res = vid_mod.process_all_videos_in_directory("/path/not/exist")
    assert res == [] 