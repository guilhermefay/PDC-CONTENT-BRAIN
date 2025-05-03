# Força rebuild para novo CMD do ETL
# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Comando simples para quebrar o cache do Railway
RUN echo "Forçando quebra de cache - $(date)"

# Instalar ffmpeg e outras dependências do sistema, depois limpar cache apt
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir reduces image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Make port 8000 available to the world outside this container
# Railway will map its internal port ($PORT) to this automatically if needed
EXPOSE 8000

# Define environment variable for the port (optional, Railway often injects $PORT)
# ENV PORT=8000

# CMD e ENTRYPOINT ficam vazios ou comentados, pois o Procfile definirá o comando
# CMD []
# ENTRYPOINT [] 