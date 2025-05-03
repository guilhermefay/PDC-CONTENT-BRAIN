# Força rebuild para novo CMD do ETL
# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Comando simples para quebrar o cache do Railway v3
RUN echo "Forçando quebra de cache v3 - $(date)"

# Argumento para receber o conteúdo do JSON de credenciais do Google
# Certifique-se que a variável GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT está disponível durante o build
ARG GOOGLE_CREDS_JSON_CONTENT

# Instalar ffmpeg robustamente e limpar cache apt
RUN apt-get update --no-cache && apt-get install -y --no-install-recommends ffmpeg && apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir reduces image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Criar o arquivo JSON de credenciais a partir do argumento de build
# Se o ARG não estiver disponível, este comando pode falhar ou criar um arquivo vazio
RUN echo "$GOOGLE_CREDS_JSON_CONTENT" > /app/gcp_creds.json

# Definir a variável de ambiente que o script Python espera
ENV GOOGLE_SERVICE_ACCOUNT_JSON=/app/gcp_creds.json

# Make port 8000 available (opcional, mas inofensivo)
EXPOSE 8000

# CMD e ENTRYPOINT ficam vazios ou comentados, pois o Procfile definirá o comando
# CMD []
# ENTRYPOINT [] 