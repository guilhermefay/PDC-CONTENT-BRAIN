# Força rebuild para novo CMD do ETL
# Use an official Python runtime as a parent image (v2 to break cache)
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Upgrade pip PRIMEIRO para tentar quebrar cache
RUN pip install --upgrade pip

# Instalar ffmpeg robustamente...
RUN apt-get update --no-cache && apt-get install -y --no-install-recommends ffmpeg && apt-get clean && rm -rf /var/lib/apt/lists/*

# Argumento para receber o conteúdo do JSON de credenciais do Google
# Certifique-se que a variável GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT está disponível durante o build
ARG GOOGLE_CREDS_JSON_CONTENT

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