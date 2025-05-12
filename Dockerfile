# Use a imagem oficial do Python como base
FROM python:3.11-slim

# Comando temporário para debug: listar o conteúdo da raiz do contexto de build
RUN ls -la /

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Comando temporário para debug: listar o conteúdo do diretório de trabalho /app
RUN ls -la /app

# Copiar os arquivos de dependência
# Usando api/requirements.txt por enquanto, pois um requirements dedicado para o worker não foi encontrado.
# O caminho COPY é relativo à raiz do contexto de build (que é a raiz do repo)
COPY ./worker/requirements.txt /app/requirements.txt

# Instalar as dependências.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar o código fonte do worker (incluindo ingestion, etl, e infra)
# Os caminhos COPY são relativos à raiz do repositório
COPY ./worker/ingestion /app/ingestion
COPY ./worker/etl /app/etl
COPY ./worker/infra /app/infra

# Comando temporário para debug: listar o conteúdo de /app
RUN ls -la /app

# Definir o comando para iniciar a aplicação do worker.
# O WORKDIR /app garante que ingestion.gdrive_ingest seja importável.

# ENTRYPOINT para manter o contêiner ativo para debug
ENTRYPOINT ["tail", "-f", "/dev/null"] 