# Integração de imagem: imports opcionais para não quebrar sem dependências
import io
import logging
from typing import Dict, Any, List, Optional

# PIL para manipulação de imagem
try:
    from PIL import Image
except ImportError:
    Image = None

# OCR via pytesseract
try:
    import pytesseract
except ImportError:
    pytesseract = None

# Extração de EXIF via exifread
try:
    import exifread
except ImportError:
    exifread = None

# Suporte para HEIC
try:
    import pillow_heif
except ImportError:
    pillow_heif = None

# CLIP para embeddings
try:
    import torch
    from transformers import CLIPProcessor, CLIPModel
except ImportError:
    torch = None
    CLIPProcessor = None
    CLIPModel = None

logger = logging.getLogger(__name__)

class ImageProcessor:
    """Processador de imagens para OCR, extração de metadata e geração de embeddings."""
    def __init__(self):
        # Carregar modelo CLIP para embeddings de imagem
        try:
            self.clip_model = CLIPModel.from_pretrained("openai/clip-vit-base-patch32")
            self.clip_processor = CLIPProcessor.from_pretrained("openai/clip-vit-base-patch32")
            logger.info("CLIP model and processor carregados com sucesso.")
        except Exception as e:
            logger.error(f"Falha ao carregar CLIP: {e}")
            self.clip_model = None
            self.clip_processor = None

    def convert_heic_to_jpeg(self, image_bytes: bytes) -> bytes:
        """Converte bytes de HEIC para JPEG usando pillow_heif."""
        if pillow_heif:
            heif_file = pillow_heif.read_heif(image_bytes)
            image = Image.frombytes(heif_file.mode, heif_file.size, heif_file.data, 'raw')
            with io.BytesIO() as output:
                image.save(output, format='JPEG')
                return output.getvalue()
        else:
            raise RuntimeError('pillow_heif não instalado para suporte a HEIC')

    def extract_text(self, image_bytes: bytes) -> Optional[str]:
        """Executa OCR na imagem e retorna o texto extraído, ou None se não houver texto."""
        try:
            # Detectar HEIC e converter
            # image = Image.open(io.BytesIO(image_bytes))
            from magic import from_buffer
            mime = from_buffer(image_bytes, mime=True)
            if mime == 'image/heic':
                image_bytes = self.convert_heic_to_jpeg(image_bytes)
            image = Image.open(io.BytesIO(image_bytes))
            text = pytesseract.image_to_string(image)
            return text.strip() or None
        except Exception as e:
            return None

    def extract_metadata(self, image_bytes: bytes) -> Dict[str, Any]:
        """Extrai metadata EXIF usando exifread."""
        try:
            tags = exifread.process_file(io.BytesIO(image_bytes))
            metadata = {tag: str(value) for tag, value in tags.items()}
            return {'exif': metadata}
        except Exception:
            return {}

    def generate_embeddings(self, image_bytes: bytes) -> List[float]:
        """Gera embeddings vetoriais para a imagem usando CLIP."""
        if not self.clip_model or not self.clip_processor:
            logger.warning("CLIP não disponível, retornando embeddings vazios.")
            return []
        try:
            image = Image.open(io.BytesIO(image_bytes)).convert('RGB')
            inputs = self.clip_processor(images=image, return_tensors="pt")
            outputs = self.clip_model.get_image_features(**{k: v.to(self.clip_model.device) for k, v in inputs.items()})
            embeddings = outputs.detach().cpu().numpy().flatten().tolist()
            return embeddings
        except Exception as e:
            logger.error(f"Erro ao gerar embeddings: {e}")
            return [] 