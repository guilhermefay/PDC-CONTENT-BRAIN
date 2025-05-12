# Use uma imagem base Python adequada
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia os arquivos de requisitos e instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia os diretórios necessários para a aplicação API
COPY api ./api
COPY infra ./infra

# Comando para iniciar o servidor Gunicorn
CMD ["gunicorn", "api.rag_api:app", "--workers", "4", "--bind", "0.0.0.0:7860", "--worker-class", "uvicorn.workers.UvicornWorker"] 