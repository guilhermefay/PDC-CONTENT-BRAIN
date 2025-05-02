# scripts/test_email_crew.py
import sys
import os
import logging

# Adicionar o diretório raiz ao PYTHONPATH
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from crews.copywriting.email_crew import EmailCopyCrew
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

if __name__ == "__main__":
    # Inputs de exemplo
    inputs = {
        'target_audience': 'Pediatras de consultório que buscam otimizar a gestão',
        'email_objective': 'Apresentar o novo módulo de agendamento inteligente do PDC e convidar para um webinar de demonstração.'
    }
    
    print(f"\n--- Iniciando Teste da EmailCopyCrew ---")
    print(f"Objetivo: {inputs['email_objective']}")
    print(f"Público: {inputs['target_audience']}")
    
    try:
        email_crew = EmailCopyCrew()
        print("\n--- Executando crew.run() ---")
        # Passar os inputs para o método run
        final_output = email_crew.run(inputs=inputs)
        print("\n--- Execução da Crew Concluída ---")
        print("\nResultado Final (Provavelmente a lista de assuntos/preheaders):")
        print("-----------------------------------------------------------------")
        print(final_output)
        print("-----------------------------------------------------------------")
        # TODO: Idealmente, precisaríamos acessar os outputs das tasks intermediárias
        #       para ver o corpo do email também. Isso pode exigir modificação na crew
        #       ou uma abordagem diferente de retorno no método run.
        
    except Exception as e:
        logging.exception(f"Erro geral ao executar o teste da EmailCopyCrew: {e}")
        print(f"\nErro ao executar o teste: {e}")

    print("\n--- Teste da EmailCopyCrew Finalizado ---") 