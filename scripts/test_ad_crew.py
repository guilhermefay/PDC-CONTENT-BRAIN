# scripts/test_ad_crew.py
import sys
import os
import logging

# Adicionar o diretório raiz ao PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from crews.copywriting.ad_crew import AdCopyCrew
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

if __name__ == "__main__":
    # Inputs de exemplo
    inputs = {
        'target_audience': 'Pediatras de consultório sobrecarregados',
        'ad_objective': 'Gerar leads para um curso sobre gestão eficiente de tempo e processos no consultório.'
    }
    
    print(f"\n--- Iniciando Teste da AdCopyCrew ---")
    print(f"Objetivo: {inputs['ad_objective']}")
    print(f"Público: {inputs['target_audience']}")
    
    try:
        ad_crew = AdCopyCrew()
        print("\n--- Executando crew.run() ---")
        # Passar os inputs para o método run
        final_output = ad_crew.run(inputs=inputs)
        print("\n--- Execução da Crew Concluída ---")
        print("\nResultado Final (Variações de Anúncio):")
        print("-----------------------------------------")
        print(final_output)
        print("-----------------------------------------")
        
    except Exception as e:
        logging.exception(f"Erro geral ao executar o teste da AdCopyCrew: {e}")
        print(f"\nErro ao executar o teste: {e}")

    print("\n--- Teste da AdCopyCrew Finalizado ---") 