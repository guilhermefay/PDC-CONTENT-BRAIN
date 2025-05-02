# scripts/test_brainstorming_crew.py
import sys
import os
import logging

# Adicionar o diretório raiz ao PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from crews.copywriting.brainstorming_crew import BrainstormingCrew
from dotenv import load_dotenv

# Carregar variáveis de ambiente (OPENAI_API_KEY, SERPER_API_KEY, R2R_API_KEY)
load_dotenv()

# Configurar logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

if __name__ == "__main__":
    # <<< ADICIONADO: Definir logger local >>>
    logger = logging.getLogger(__name__)
    
    # Inputs de exemplo
    inputs = {
        'topic': 'Marketing de conteúdo para consultórios pediátricos',
        'target_audience': 'Pediatras buscando atrair mais pacientes particulares'
    }
    
    print(f"\n--- Iniciando Teste da BrainstormingCrew ---")
    print(f"Tópico: {inputs['topic']}")
    print(f"Público: {inputs['target_audience']}")
    
    # Verificar se a chave SERPER está configurada (necessária para o TrendResearcher)
    if not os.getenv("SERPER_API_KEY"):
        logger.warning("SERPER_API_KEY não configurada. O agente TrendResearcher pode não funcionar corretamente.")
        # Poderia parar aqui ou deixar continuar para testar os outros agentes
    
    try:
        brainstorming_crew = BrainstormingCrew()
        print("\n--- Executando crew.run() ---")
        # Passar os inputs para o método run
        final_output = brainstorming_crew.run(inputs=inputs)
        print("\n--- Execução da Crew Concluída ---")
        print("\nResultado Final (Ideias, Hooks, Headlines):")
        print("----------------------------------------------")
        print(final_output)
        print("----------------------------------------------")
        
    except Exception as e:
        logging.exception(f"Erro geral ao executar o teste da BrainstormingCrew: {e}")
        print(f"\nErro ao executar o teste: {e}")

    print("\n--- Teste da BrainstormingCrew Finalizado ---") 