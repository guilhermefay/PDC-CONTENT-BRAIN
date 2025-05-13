# Use uma imagem base Python adequada
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Garante que /app esteja no PYTHONPATH
ENV PYTHONPATH /app

# Copia o arquivo de requisitos e instala as dependências
COPY worker_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o conteúdo de worker_service para /app
COPY worker_service/ingestion ./ingestion
COPY worker_service/etl ./etl
COPY worker_service/infra ./infra

# Copia o arquivo rag_api.py da api_service para o diretório raiz
COPY api_service/rag_api.py .
COPY api_service/__init__.py .

# agents ainda é copiado da raiz do projeto para um diretório agents em /app
COPY agents ./agents

# Define o comando padrão para rodar a aplicação
CMD ["python", "-m", "ingestion.gdrive_ingest"]

# O Root Directory do serviço no Railway DEVE ser configurado para / (RAIZ DO PROJETO)
# O Start Command na UI do Railway ("python -m ingestion.gdrive_ingest") irá provavelmente sobrescrever este CMD.