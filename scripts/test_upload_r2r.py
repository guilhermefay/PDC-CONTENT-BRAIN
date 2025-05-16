import os
import logging
import uuid  # Adicionado para gerar UUID válido
from infra.r2r_client import R2RClientWrapper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_upload_r2r")

def main():
    try:
        r2r_client = R2RClientWrapper()
        logger.info(f"R2RClientWrapper initialized for base URL: {r2r_client.base_url}")
    except Exception as e:
        logger.error(f"Erro ao inicializar R2RClientWrapper: {e}")
        return

    # Gerar um UUID válido para o campo document_id/id
    document_id = str(uuid.uuid4())
    file_path = "scripts/test_upload.txt"
    metadata = {"document_id": document_id, "source": "manual_test"}
    try:
        logger.info(f"Enviando arquivo de teste para R2R: {file_path} (document_id={document_id})")
        response = r2r_client.upload_file(
            file_path=file_path,
            document_id=document_id,
            metadata=metadata
        )
        logger.info(f"Resposta da API R2R: {response}")
    except Exception as e:
        logger.error(f"Erro ao fazer upload: {e}")
    logger.info("Upload manual para R2R finalizado. Verifique o painel SciPhi.")

if __name__ == "__main__":
    main() 