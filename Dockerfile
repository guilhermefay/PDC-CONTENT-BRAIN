# Use a imagem oficial do Python como base
FROM python:3.11-slim

# Definir o diretório de trabalho dentro do contêiner para os arquivos da API R2R
WORKDIR /app/R2R/py

# Copiar os arquivos de dependência para o diretório de trabalho.
# Os caminhos COPY agora são relativos à raiz do contexto de build (que será a raiz do seu repositório).
COPY ./R2R/py/requirements.txt /app/R2R/py/requirements.txt

# Instalar as dependências.
# --no-cache-dir para evitar armazenar em cache pacotes baixados, economizando espaço.
# --upgrade pip garante que o pip esteja atualizado.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código fonte da API R2R.
# O caminho de origem também é relativo à raiz do contexto de build.
COPY ./R2R/py /app/R2R/py

# Expor a porta que a aplicação R2R usa.
EXPOSE 8000

# Definir o comando para iniciar a aplicação R2R.
# Usamos gunicorn para servir a aplicação FastAPI.
CMD ["gunicorn", "main:app", "--workers", "1", "--timeout", "120", "--bind", "0.0.0.0:8000", "--chdir", "/app/R2R/py"] 