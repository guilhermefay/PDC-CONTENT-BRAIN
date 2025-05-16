import re
import logging
from collections import Counter
import argparse

# Configuração básica de logging para o analisador
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Regex para capturar as informações relevantes das linhas de log
# Exemplo de linha de log: 2025-05-07 09:33:02,041 - INFO - [enrich_metadata_all:283] - Documento a7688120-9bed-4b2c-9aa4-856815aecce9: Pasta="/TIME MARKETING DIGITAL/FUNIS (E-MAIL, PERPÉTUO, ISCA)/E-MAIL MARKETING/2025/[ESPECIAL] NEWSLETTERS/3 - MARÇO" -> SourceType='email'
LOG_LINE_PATTERN = re.compile(
    r"INFO - .*?Documento\s+(?P<document_id>[\w-]+):\s+Pasta=\"(?P<full_folder_path>.*?)\"\s+->\s+SourceType='(?P<source_type>[^']+?)'"
)

ERROR_PATH_PATTERN = re.compile(r"/\[(NOT_FOUND|HTTP_ERROR|ERROR)\]")

def parse_log_file(log_file_path):
    """Analisa o arquivo de log para extrair informações sobre o enriquecimento de metadados."""
    source_type_counts = Counter()
    unknown_source_type_paths = []
    path_errors = []
    processed_doc_ids = set() # Para contar documentos únicos processados
    total_lines_parsed = 0

    logger.info(f"Analisando arquivo de log: {log_file_path}")
    try:
        with open(log_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = LOG_LINE_PATTERN.search(line)
                if match:
                    total_lines_parsed +=1
                    data = match.groupdict()
                    doc_id = data['document_id']
                    full_path = data['full_folder_path']
                    source_type = data['source_type']

                    processed_doc_ids.add(doc_id)
                    source_type_counts[source_type] += 1

                    if source_type == 'desconhecido':
                        unknown_source_type_paths.append(full_path)
                    
                    if ERROR_PATH_PATTERN.search(full_path):
                        path_errors.append(f"{full_path} (Documento: {doc_id})")
                        
    except FileNotFoundError:
        logger.error(f"Arquivo de log não encontrado: {log_file_path}")
        return None
    except Exception as e:
        logger.error(f"Erro ao ler ou processar o arquivo de log: {e}")
        return None

    logger.info(f"Análise do log concluída. {total_lines_parsed} linhas de log de processamento encontradas.")
    return {
        "total_documents_processed": len(processed_doc_ids),
        "source_type_counts": source_type_counts,
        "unknown_source_type_paths": list(set(unknown_source_type_paths)), # Remover duplicatas
        "path_errors": list(set(path_errors)) # Remover duplicatas
    }

def main():
    """Função principal para executar a análise do log."""
    parser = argparse.ArgumentParser(description="Analisa o log do script de enriquecimento de metadados.")
    parser.add_argument(
        "log_file", 
        nargs='?', 
        default="scripts/enrich_metadata.log", 
        help="Caminho para o arquivo de log a ser analisado (default: scripts/enrich_metadata.log)"
    )
    args = parser.parse_args()

    results = parse_log_file(args.log_file)

    if results:
        logger.info("\n--- Resumo da Análise do Log de Enriquecimento ---")
        logger.info(f"Total de Documentos Únicos Processados (tentativas de enriquecimento): {results['total_documents_processed']}")
        
        logger.info("\nContagem por SourceType Inferido:")
        if results['source_type_counts']:
            for st, count in results['source_type_counts'].most_common():
                logger.info(f"  - {st}: {count}")
        else:
            logger.info("  Nenhum source_type foi inferido (ou log não capturou essa informação).")

        logger.info("\nCaminhos de Pasta que resultaram em SourceType 'desconhecido':")
        if results['unknown_source_type_paths']:
            # Salvar em arquivo
            with open("scripts/unknown_source_types.txt", "w", encoding="utf-8") as f_unknown:
                for path in sorted(results['unknown_source_type_paths']):
                    logger.info(f"  - {path}")
                    f_unknown.write(f"{path}\n")
            logger.info("Lista de caminhos desconhecidos salva em: scripts/unknown_source_types.txt")
        else:
            logger.info("  Nenhum caminho resultou em source_type 'desconhecido'.")

        logger.info("\nCaminhos de Pasta com Erros de API do GDrive (ex: /[NOT_FOUND]):")
        if results['path_errors']:
            # Salvar em arquivo
            with open("scripts/gdrive_path_errors.txt", "w", encoding="utf-8") as f_errors:
                for error_path in sorted(results['path_errors']):
                    logger.info(f"  - {error_path}")
                    f_errors.write(f"{error_path}\n")
            logger.info("Lista de erros de caminho salva em: scripts/gdrive_path_errors.txt")
        else:
            logger.info("  Nenhum erro de caminho de pasta do GDrive detectado.")
        logger.info("---------------------------------------------------")

if __name__ == "__main__":
    main() 