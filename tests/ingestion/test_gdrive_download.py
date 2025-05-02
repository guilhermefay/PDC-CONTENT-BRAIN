import io
from unittest.mock import patch, MagicMock

import ingestion.gdrive_ingest as gdrive_mod

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeDownloader:
    """Simula MediaIoBaseDownload escrevendo bytes no buffer."""

    def __init__(self, fh, request):
        self.fh = fh
        self._done = False

    def next_chunk(self):
        if not self._done:
            self.fh.write(b"dummy-data")
            self._done = True
            return (MagicMock(), True)  # status, done
        return (MagicMock(), True)


# ---------------------------------------------------------------------------
# download_file
# ---------------------------------------------------------------------------

@patch("ingestion.gdrive_ingest.MediaIoBaseDownload", _FakeDownloader)
def test_download_file_success():
    mock_service = MagicMock()
    mock_files = MagicMock()
    mock_service.files.return_value = mock_files
    mock_files.get_media.return_value = "request-object"

    result = gdrive_mod.download_file(mock_service, "file123")
    assert result == b"dummy-data"
    mock_files.get_media.assert_called_once_with(fileId="file123")


@patch("ingestion.gdrive_ingest.MediaIoBaseDownload", _FakeDownloader)
def test_download_file_http_error():
    # Criar HttpError dummy sem depender de googleapiclient
    class DummyHttpError(Exception):
        def __init__(self):
            self.resp = MagicMock(status=500)
            self.content = b"fail"

    with patch.object(gdrive_mod, "HttpError", DummyHttpError):
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_service.files.return_value = mock_files
        mock_files.get_media.side_effect = DummyHttpError()

        result = gdrive_mod.download_file(mock_service, "bad")
        assert result is None

# ---------------------------------------------------------------------------
# export_and_download_gdoc
# ---------------------------------------------------------------------------

@patch("ingestion.gdrive_ingest.MediaIoBaseDownload", _FakeDownloader)
def test_export_and_download_gdoc_success():
    mock_service = MagicMock()
    mock_files = MagicMock()
    mock_service.files.return_value = mock_files
    mock_files.export_media.return_value = "export-request"

    data = gdrive_mod.export_and_download_gdoc(mock_service, "file_gdoc", gdrive_mod.GDRIVE_EXPORT_MIME)
    assert data == b"dummy-data"
    mock_files.export_media.assert_called_once_with(fileId="file_gdoc", mimeType=gdrive_mod.GDRIVE_EXPORT_MIME)

@patch("ingestion.gdrive_ingest.MediaIoBaseDownload", _FakeDownloader)
def test_export_and_download_gdoc_error():
    class DummyHttpError(Exception):
        def __init__(self):
            self.resp = MagicMock(status=403)
            self.content = b"forbidden"
    with patch.object(gdrive_mod, "HttpError", DummyHttpError):
        mock_service = MagicMock()
        mock_files = MagicMock()
        mock_service.files.return_value = mock_files
        mock_files.export_media.side_effect = DummyHttpError()

        data = gdrive_mod.export_and_download_gdoc(mock_service, "file_bad", gdrive_mod.GDRIVE_EXPORT_MIME)
        assert data is None 