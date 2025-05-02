# Definir o interpretador Python (ajuste se necessário)
PYTHON = python
# Usar PYTHONPATH=. para garantir que os módulos locais sejam encontrados
PYTEST = PYTHONPATH=. pytest
FLAKE8 = PYTHONPATH=. flake8

# Variável para argumentos extras do ETL
ETL_ARGS ?= --source gdrive # Define um padrão se não for passado

# --- Comandos Principais ---

.PHONY: test coverage run-etl start-api lint clean install-dev help

test:
	@echo "==> Rodando testes..."
	$(PYTEST) -q tests/

coverage:
	@echo "==> Rodando testes com cobertura..."
	$(PYTEST) -q --cov=. --cov-report term-missing tests/
	@echo "Relatório HTML em: htmlcov/index.html"

run-etl:
	@echo "==> Executando pipeline ETL completo com args: $(ETL_ARGS)"
	PYTHONPATH=. $(PYTHON) etl/annotate_and_index.py $(ETL_ARGS)
	@echo "ETL concluído. Verifique os logs em logs/etl.log"

start-api:
	@echo "==> Iniciando API RAG..."
	PYTHONPATH=. $(PYTHON) api/rag_api.py

lint:
	@echo "==> Verificando linting com flake8..."
	$(FLAKE8) . --count --select=E9,F63,F7,F82 --show-source --statistics
	$(FLAKE8) . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

clean:
	@echo "==> Limpando arquivos gerados..."
	find . -type f -name '*.py[co]' -delete
	find . -type d -name '__pycache__' -delete
	rm -rf .pytest_cache
	rm -f .coverage
	rm -rf htmlcov
	rm -f logs/*.log
	rm -f /tmp/gdrive_ingest_summary.json
	# Adicione outros arquivos temporários aqui se necessário
	@echo "Limpeza concluída."

install-dev:
	@echo "==> Instalando dependências de desenvolvimento..."
	pip install -r requirements-dev.txt

help:
	@echo "Uso: make [comando]"
	@echo "Comandos disponíveis:"
	@echo "  install-dev  Instala dependências de desenvolvimento"
	@echo "  test         Roda todos os testes"
	@echo "  coverage     Roda testes com relatório de cobertura"
	@echo "  run-etl [ETL_ARGS=\"...\"] Executa o pipeline ETL. Argumentos padrão: --source gdrive"
	@echo "                           Ex: make run-etl ETL_ARGS=\" --source gdrive --dry-run --dry-run-limit=10\" "
	@echo "  start-api    Inicia a API RAG"
	@echo "  lint         Verifica o código com flake8"
	@echo "  clean        Remove arquivos gerados (__pycache__, .coverage, etc.)"
	@echo "  help         Mostra esta mensagem de ajuda" 