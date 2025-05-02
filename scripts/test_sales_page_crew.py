# scripts/test_sales_page_crew.py
import sys
import os
import logging

# Adicionar o diretório raiz ao PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from crews.copywriting.sales_page_crew import SalesPageCopyCrew
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

if __name__ == "__main__":
    # Inputs de exemplo
    inputs = {
        'product_name': 'Curso Online "Pediátra Empreendedor 2.0"',
        'target_audience': 'Pediatras recém-formados ou buscando transição para consultório particular'
    }
    
    print(f"\n--- Iniciando Teste da SalesPageCopyCrew ---")
    print(f"Produto: {inputs['product_name']}")
    print(f"Público: {inputs['target_audience']}")
    
    try:
        sales_page_crew = SalesPageCopyCrew()
        print("\n--- Executando crew.run() ---")
        # Passar os inputs para o método run
        final_output = sales_page_crew.run(inputs=inputs)
        print("\n--- Execução da Crew Concluída ---")
        print("\nResultado Final (Texto da Página de Vendas em Markdown):")
        print("---------------------------------------------------------")
        print(final_output)
        print("---------------------------------------------------------")
        
    except Exception as e:
        logging.exception(f"Erro geral ao executar o teste da SalesPageCopyCrew: {e}")
        print(f"\nErro ao executar o teste: {e}")

    print("\n--- Teste da SalesPageCopyCrew Finalizado ---") 