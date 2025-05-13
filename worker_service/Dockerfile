# Use uma imagem base Python adequada
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de requisitos e instala as dependências
COPY worker_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o conteúdo de worker_service para /app
COPY worker_service/ingestion ./ingestion
COPY worker_service/etl ./etl
COPY worker_service/infra ./infra
# agents ainda é copiado da raiz do projeto para um diretório agents em /app
COPY agents ./agents

# Remove o comando CMD de start para usar um ENTRYPOINT de debug
# O ENTRYPOINT manterá o contêiner rodando para inspeção, permitindo ver os logs de build e o ambiente interno.
# Revertemos para o CMD de start normal após o debug.
# CMD ["python", "-m", "ingestion.gdrive_ingest"]
ENTRYPOINT ["tail", "-f", "/dev/null"]

# O comando de start real será configurado no Railway UI para "python -m ingestion.gdrive_ingest"
# O Root Directory do serviço no Railway DEVE ser configurado para / (RAIZ DO PROJETO)