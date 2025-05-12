# Dockerfile para a API do R2R, localizado na raiz do repositório.
# Ajustado para buildar a partir do contexto da raiz.

FROM python:3.12-slim AS builder

# Install system dependencies
# Estes pacotes são necessários para compilar algumas dependências Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ musl-dev curl libffi-dev gfortran libopenblas-dev \
    poppler-utils \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    # Instala Rustup para compilar dependências que precisam de Rust (como o LanceDB)
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

# Add Rust to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Prepare directory for builder stage - copying just what's needed for install
RUN mkdir -p /app/builder
WORKDIR /app/builder

# Copia o pyproject.toml do diretório original do R2R dentro do contexto da raiz
COPY R2R/py/pyproject.toml ./\n# Copia o diretório core e outros arquivos necessários para a instalação editável
# Ajuste conforme a estrutura exata necessária para a instalação "[core]"
COPY R2R/py/core ./core
COPY R2R/py/r2r ./r2r # Assumindo que 'r2r' também é parte da instalação editável ou dependências
# COPY R2R/py/pyproject.toml R2R/py/ # Removido, parece duplicado e no lugar errado

# Instala as dependências, incluindo a instalação editável do R2R "[core]"
# Se a instalação editável falhar, pode ser necessário copiar mais arquivos da pasta R2R/py
# para dentro do contexto /app/builder antes de rodar o pip install -e
# Vamos tentar assim primeiro:
RUN pip install -e ".[core]" && \
    pip install gunicorn uvicorn pydantic --no-cache-dir


# Create the final image
FROM python:3.12-slim

# Minimal runtime deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl poppler-utils \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy the built environment from builder to final image
# (If you want a fully self-contained environment, copy /usr/local)
COPY --from=builder /usr/local /usr/local

WORKDIR /app

# Copy the R2R source code needed at runtime
# Copia todo o conteúdo da pasta R2R/py para /app na imagem final
COPY R2R/py/ /app

# Expose environment variables and port
ARG R2R_PORT=8000 R2R_HOST=0.0.0.0
ENV R2R_PORT=$R2R_PORT R2R_HOST=$R2R_HOST
EXPOSE $R2R_PORT

# Launch the app
CMD ["sh", "-c", "uvicorn core.main.app_entry:app --host $R2R_HOST --port $R2R_PORT"]
