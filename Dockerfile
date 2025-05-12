# Use a imagem oficial do Python como base
FROM python:3.11-slim

# Definir o diretório de trabalho dentro do contêiner para os arquivos da API R2R
# O WORKDIR agora reflete o novo local dentro do contêiner
WORKDIR /app/api

# Copiar os arquivos de dependência para o diretório de trabalho.
# Os caminhos COPY agora são relativos à raiz do contexto de build (que será a raiz do seu repositório) e o novo diretório 'api'
COPY ./api/requirements.txt /app/api/requirements.txt

# Instalar as dependências.
# --no-cache-dir para evitar armazenar em cache pacotes baixados, economizando espaço.
# --upgrade pip garante que o pip esteja atualizado.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código fonte da API R2R.
# O caminho de origem também é relativo à raiz do contexto de build e o novo diretório 'api'
COPY ./api/. /app/api/

# Expor a porta que a aplicação R2R usa.
EXPOSE 8000

# Definir o comando para iniciar a aplicação R2R.
# Usamos gunicorn para servir a aplicação FastAPI, o --chdir agora aponta para o novo diretório dentro do contêiner
CMD ["gunicorn", "main:app", "--workers", "1", "--timeout", "120", "--bind", "0.0.0.0:8000", "--chdir", "/app/api"] 