# Use uma imagem base Python adequada
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de requisitos e instala as dependências
COPY worker_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia os diretórios necessários para o worker da raiz do repositório para /app no contêiner
# Adiciona comandos de debug para listar o conteúdo de /app após cada cópia
COPY worker_service/ingestion ./ingestion
RUN ls -la /app
COPY worker_service/etl ./etl
RUN ls -la /app
COPY worker_service/infra ./infra
RUN ls -la /app
COPY agents ./agents
RUN ls -la /app

# Remove o comando CMD de start para usar um ENTRYPOINT de debug
# O ENTRYPOINT manterá o contêiner rodando para inspeção, permitindo ver os logs de build e o ambiente interno.
# Revertemos para o CMD de start normal após o debug.
# CMD ["python", "-m", "ingestion.gdrive_ingest"]
ENTRYPOINT ["tail", "-f", "/dev/null"]

# O comando de start real será configurado no Railway UI para "python -m ingestion.gdrive_ingest"
# O Root Directory do serviço no Railway DEVE ser configurado para /