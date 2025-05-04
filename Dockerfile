# Força rebuild para novo CMD do ETL
# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Argumento para quebrar o cache. Mude o valor se precisar forçar rebuild.
# Comentado pois não estava funcionando confiavelmente.
# ARG CACHE_BUSTER=3

# Set the working directory in the container
WORKDIR /app

# Upgrade pip
RUN pip install --upgrade pip

# Comando para usar o argumento e quebrar o cache
# Comentado pois não estava funcionando confiavelmente.
# RUN echo "Cache bust: $CACHE_BUSTER"

# Instalar ffmpeg robustamente...
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && apt-get clean && rm -rf /var/lib/apt/lists/*

# Argumento removido - arquivo será criado no runtime
# ARG GOOGLE_CREDS_JSON_CONTENT

# Etapa RUN removida - arquivo será criado no runtime
# RUN echo "$GOOGLE_CREDS_JSON_CONTENT" > /app/gcp_creds.json

# Definir variável de ambiente para que o script Python encontre o arquivo
# O arquivo /app/gcp_creds.json será criado pelo Custom Start Command
ENV GOOGLE_SERVICE_ACCOUNT_JSON=/app/gcp_creds.json

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Copy the entrypoint script into the container
COPY entrypoint.sh .
COPY normalize_json.py .

# Make port 8000 available (opcional, mas inofensivo)
EXPOSE 8000

# ENTRYPOINT ["python3"] # REMOVIDO NOVAMENTE

# Não definir CMD também, deixar o Start Command do Railway controlar 