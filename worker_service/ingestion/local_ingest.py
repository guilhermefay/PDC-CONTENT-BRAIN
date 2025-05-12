import os
import logging

logger = logging.getLogger(__name__)

def ingest_local_directory(
    directory_path: str,
    allowed_extensions: list[str] | None = None,
    dry_run: bool = False,
    dry_run_limit: int | None = None
) -> list[dict]:
    """Ingere arquivos locais de um diretório, retornando conteúdo e metadata."""
    if allowed_extensions is None:
        allowed_extensions = ['.txt', '.md']

    if not os.path.isdir(directory_path):
        logger.error(f"Directory not found: {directory_path}")
        return []

    results = []
    count = 0

    for name in os.listdir(directory_path):
        # Verificar limite em dry_run antes de qualquer operação
        if dry_run and dry_run_limit is not None and count >= dry_run_limit:
            logger.info(f"Dry run limit ({dry_run_limit}) reached. Stopping ingestion.")
            break

        full_path = os.path.join(directory_path, name)
        # Chamar isfile para todos os itens
        if not os.path.isfile(full_path):
            continue

        ext = os.path.splitext(name)[1]
        # Filtrar extensões
        if allowed_extensions and ext.lower() not in [e.lower() for e in allowed_extensions]:
            continue

        # Processar arquivo
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()

            results.append({
                'content': content,
                'metadata': {
                    'source_filename': name,
                    'source_type': 'local_file'
                }
            })
        except Exception as e:
            logger.error(f"Error reading file {name}: {e}")
            # Não adicionar ao results
        finally:
            count += 1

        # Verificar limite após processar
        if dry_run and dry_run_limit is not None and count >= dry_run_limit:
            logger.info(f"Dry run limit ({dry_run_limit}) reached. Stopping ingestion.")
            break

    return results 