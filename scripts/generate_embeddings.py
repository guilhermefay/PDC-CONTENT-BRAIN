from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from dotenv import load_dotenv
import os
import numpy as np
import json
import faiss

# Carregar variáveis de ambiente
load_dotenv()

# Configurações
EMBEDDINGS_MODEL = os.getenv("EMBEDDINGS_MODEL", "text-embedding-3-small")
EMBEDDINGS_DIMENSIONS = int(os.getenv("EMBEDDINGS_DIMENSIONS", "1536"))

def main():
    # ---- DEBUG: Print loaded environment variables ----
    loaded_api_key = os.getenv("OPENAI_API_KEY")
    loaded_project_id = os.getenv("OPENAI_PROJECT_ID")
    print(f"DEBUG: Using API Key: {loaded_api_key[:8]}...{loaded_api_key[-4:]}") # Print partial key for security
    print(f"DEBUG: Using Project ID: {loaded_project_id}")
    # --------------------------------------------------

    # Inicializar o wrapper OpenAIEmbeddings da LangChain
    embeddings = OpenAIEmbeddings(
        model=EMBEDDINGS_MODEL,
        dimensions=EMBEDDINGS_DIMENSIONS
    )

    # Criar documentos de teste
    documents = [
        {
            "content": "Programação é uma habilidade essencial no mundo moderno. Para estudar programação, comece com lógica de programação, depois escolha uma linguagem como Python para praticar.",
            "metadata": {"role": "aluno", "type": "article"}
        },
        {
            "content": "Marketing digital envolve várias estratégias como SEO, mídia social e email marketing. É importante definir seu público-alvo e criar conteúdo relevante.",
            "metadata": {"role": "aluno", "type": "article"}
        },
        {
            "content": "Documento restrito apenas para professores sobre avaliação de alunos e metodologias de ensino.",
            "metadata": {"role": "professor", "type": "restricted"}
        }
    ]

    try:
        # Extrair textos e metadados
        texts = [doc["content"] for doc in documents]
        metadatas = [doc["metadata"] for doc in documents]

        # Criar diretório vector_store se não existir
        os.makedirs("vector_store", exist_ok=True)

        # Gerar embeddings usando LangChain
        print("Gerando embeddings usando LangChain...")
        embeddings_list = embeddings.embed_documents(texts)

        # Converter lista de embeddings para numpy array
        embeddings_array = np.array(embeddings_list).astype('float32')

        # Criar índice FAISS
        print("Criando índice FAISS...")
        index = faiss.IndexFlatL2(EMBEDDINGS_DIMENSIONS)
        index.add(embeddings_array)

        # Salvar índice e metadados
        print("Salvando índice e metadados...")
        faiss.write_index(index, "vector_store/index.faiss")

        # Salvar metadados
        with open("vector_store/metadata.json", "w") as f:
            json.dump({
                "texts": texts,
                "metadata": metadatas
            }, f, ensure_ascii=False, indent=2)

        print("Embeddings gerados e salvos com sucesso usando LangChain!")

    except Exception as e:
        print(f"Erro durante a execução com LangChain: {str(e)}")
        # Imprimir traceback completo para mais detalhes
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main() 