import pytest

from ingestion.gdrive_ingest import is_supported_image
from ingestion.image_processor import ImageProcessor

class DummyFile(dict):
    # Permite acessar keys via dot, mas dict é suficiente
    pass

@pytest.mark.parametrize("mime,name,expected", [
    ('image/jpeg', 'photo.jpg', True),
    ('image/png', 'diagram.PNG', True),
    ('image/heic', 'picture.heic', True),
    ('application/pdf', 'graphic.heic', True),  # nome com extensão válida
    ('video/mp4', 'video.jpg', False),
    ('', 'document.txt', False),
])
def test_is_supported_image_various(mime, name, expected):
    file = {'mimeType': mime, 'name': name}
    assert is_supported_image(file) == expected


def test_image_processor_generate_embeddings_returns_list():
    processor = ImageProcessor()
    embeddings = processor.generate_embeddings(b'')
    assert isinstance(embeddings, list)


def test_image_processor_extract_metadata_empty_bytes():
    processor = ImageProcessor()
    metadata = processor.extract_metadata(b'')
    assert isinstance(metadata, dict)


def test_image_processor_extract_text_empty_bytes():
    processor = ImageProcessor()
    text = processor.extract_text(b'')
    assert text is None or isinstance(text, str) 