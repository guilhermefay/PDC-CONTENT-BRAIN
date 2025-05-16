# Use uma imagem base Python adequada
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Garante que /app esteja no PYTHONPATH
ENV PYTHONPATH /app

# Copia o arquivo de requisitos e instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o conteúdo de worker_service para /app
COPY ingestion ./ingestion
COPY etl ./etl
COPY infra ./infra

# Copia o stub do rag_api.py local
COPY rag_api.py .
COPY __init__.py .

# Copia o diretório agents local
COPY agents ./agents

# Define o comando padrão para rodar a aplicação
CMD ["python", "-m", "ingestion.gdrive_ingest"]

# ============================================================
# IMPORTANTE: CONFIGURAÇÃO DO RAILWAY
# ============================================================
# - Root Directory do serviço: worker_service/
# - Start Command: python -m ingestion.gdrive_ingest
#
# O Railway usará os arquivos dentro da pasta worker_service/
# com os caminhos relativos dentro dessa pasta.