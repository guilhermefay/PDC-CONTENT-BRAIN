# ingestion/embeddings.py

import os
import logging
from dotenv import load_dotenv
from typing import List, Optional
from supabase import create_client, Client
import openai  # Biblioteca OpenAI para geração de embeddings

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    logger.error("Supabase URL ou Service Key não encontrados no .env. Verifique a configuração.")
    supabase: Optional[Client] = None
else:
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        logger.info("Cliente Supabase inicializado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao inicializar cliente Supabase: {e}")
        supabase = None

# OpenAI setup
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.error("OPENAI_API_KEY não encontrada. Fallback de embeddings OpenAI desabilitado.")
else:
    openai.api_key = OPENAI_API_KEY
    logger.info("OpenAI API Key configurada.")

def generate_embeddings(texts: List[str]) -> Optional[List[List[float]]]:
    """Gera embeddings vetoriais para uma lista de textos usando a API da OpenAI."""
    if not supabase:
        logger.error("Cliente Supabase não está inicializado. Não é possível gerar embeddings.")
        return None

    if not OPENAI_API_KEY:
        logger.error("Chave OpenAI não configurada. Não é possível gerar embeddings.")
        return None

    if not texts:
        logger.info("Lista de textos vazia, nenhum embedding para gerar.")
        return []

    logger.info(f"Gerando embeddings OpenAI para {len(texts)} textos...")
    try:
        # Inicializa o cliente OpenAI dentro da função (ou pode ser global se preferir)
        client = openai.OpenAI(api_key=OPENAI_API_KEY)

        # Chamada em lote para gerar embeddings (nova sintaxe)
        response = client.embeddings.create(
            model="text-embedding-3-small",  # Modelo recomendado e mais recente
            input=texts
        )
        # A estrutura da resposta mudou ligeiramente
        embeddings_list = [item.embedding for item in response.data]
        logger.info(f"Embeddings gerados com sucesso para {len(embeddings_list)} textos.")
        return embeddings_list
    except Exception as e:
        logger.error(f"Erro ao gerar embeddings via OpenAI: {e}", exc_info=True)
        return None

# Exemplo de uso
if __name__ == '__main__':
    test_texts = [
        "Este é o primeiro documento.",
        "Este é o segundo, um pouco mais longo.",
        "E o terceiro."
    ]

    generated_embeddings = generate_embeddings(test_texts)

    if generated_embeddings:
        print(f"\n--- Embeddings Gerados ({len(generated_embeddings)}) ---")
        for i, emb in enumerate(generated_embeddings):
            print(f"Texto {i+1}: Dimensão = {len(emb)}, Exemplo = {emb[:5]}...")
    else:
        print("\nFalha ao gerar embeddings.") 