# Use a imagem oficial do Python como base
FROM python:3.11-slim

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar os arquivos de dependência
# Usando api/requirements.txt por enquanto, pois um requirements dedicado para o worker não foi encontrado.
# O caminho COPY é relativo à raiz do contexto de build.
COPY ./api/requirements.txt /app/requirements.txt

# Instalar as dependências.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar o código fonte do worker (incluindo ingestion e etl)
# Os caminhos COPY são relativos à raiz do contexto de build.
COPY ./ingestion /app/ingestion
COPY ./etl /app/etl
COPY ./infra /app/infra

# Definir o comando para iniciar a aplicação do worker.
# O WORKDIR /app garante que ingestion.gdrive_ingest seja importável.
CMD ["python", "-m", "ingestion.gdrive_ingest"] 