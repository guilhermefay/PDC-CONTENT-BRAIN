import os
import json
from supabase import create_client, Client
from dotenv import load_dotenv

def main():
    """
    Script principal para verificar os metadados na tabela documents do Supabase.
    """
    load_dotenv()

    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        print("Erro: SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem ser definidos no arquivo .env")
        return

    supabase: Client = create_client(supabase_url, supabase_key)

    print("Verificando chunks não enriquecidos...")

    # Contagem de Chunks sem source_type válido
    # Tentativa 1: Checando ausência da chave OU valor NULL.
    # Nota: A API do supabase-py pode não suportar diretamente a checagem de "chave não existe" de forma simples e combinada com OR para valor NULL.
    # Vamos focar em contar onde 'source_type' IS NULL ou não está presente (o que pode ser mais difícil de diferenciar via API padrão).
    # Uma forma mais simples é checar onde `metadata->>'source_type' IS NULL`.
    # Se a chave 'source_type' não existir, metadata->>'source_type' também será NULL.
    
    response_no_source_type = supabase.table('documents').select('count', count='exact').is_('metadata->>source_type', 'null').execute()
    
    count_no_source_type = 0
    if response_no_source_type.count is not None:
        count_no_source_type = response_no_source_type.count
    print(f"Chunks com metadata->>'source_type' IS NULL: {count_no_source_type}")

    # Contagem de Chunks sem gdrive_full_folder_path
    # Similarmente, checando onde `metadata->>'gdrive_full_folder_path' IS NULL`.
    response_no_gdrive_path = supabase.table('documents').select('count', count='exact').is_('metadata->>gdrive_full_folder_path', 'null').execute()
    
    count_no_gdrive_path = 0
    if response_no_gdrive_path.count is not None:
        count_no_gdrive_path = response_no_gdrive_path.count
    print(f"Chunks com metadata->>'gdrive_full_folder_path' IS NULL: {count_no_gdrive_path}")


    print("\n--- Verificação de Amostras ---")
    sample_ids = [
        # IDs que o log da execução LIVE disse ter processado:
        "e8078b1d-09cd-41ba-82de-f816e0f930e3", 
        "dd0b438e-59b8-4900-b967-aafdfc1ca0f7", 
        "71a7cf3f-37ce-4b24-9d05-95d04ef80c6b",
        # IDs que talvez NÃO tenham sido processados/contados:
        "c35ed95c-0165-4402-8b97-81257457b9c6", 
        "b18d8943-e486-4dd4-a149-752655432a3b",
        "26255d75-a584-460e-b519-be39dd3703c7"
    ]

    for doc_id in sample_ids:
        print(f"\n--- Verificando Document ID: {doc_id} ---")
        try:
            response = supabase.table('documents').select('document_id, metadata').eq('document_id', doc_id).maybe_single().execute()
            if response.data:
                metadata = response.data.get('metadata', {})
                print(json.dumps(metadata, indent=2, ensure_ascii=False))
            else:
                print(f"Documento com ID {doc_id} não encontrado ou resposta sem dados.")
        except Exception as e:
            print(f"Erro ao buscar documento {doc_id}: {e}")

if __name__ == "__main__":
    main() 