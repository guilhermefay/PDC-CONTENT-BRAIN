# scripts/test_carrossel_crew.py
import sys
import os
import logging

# Adicionar o diretório raiz ao PYTHONPATH para encontrar os módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from crews.copywriting.carrossel_crew import CarrosselCopyCrew
from dotenv import load_dotenv

# Carregar variáveis de ambiente (importante para API keys e talvez config R2R)
load_dotenv()

# Configurar logging básico para ver o output dos agentes e da crew
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

if __name__ == "__main__":
    # Tópico de exemplo para o carrossel
    # TODO: Usar um tópico mais relevante para o PDC se possível
    # Ex: "5 Sinais de Alerta no Desenvolvimento do Bebê"
    # Ex: "Como preparar o consultório para a primeira consulta?"
    topic = "Importância da consulta pediátrica no primeiro mês de vida"
    
    print(f"\n--- Iniciando Teste da CarrosselCopyCrew para o tópico: '{topic}' ---")
    
    try:
        carrossel_crew = CarrosselCopyCrew()
        print("\n--- Executando crew.run() ---")
        final_copy = carrossel_crew.run(inputs={'topic': topic})
        print("\n--- Execução da Crew Concluída ---")
        print("\nResultado Final (Cópia do Carrossel):")
        print("----------------------------------------")
        print(final_copy)
        print("----------------------------------------")
        
    except Exception as e:
        logging.exception(f"Erro geral ao executar o teste da CarrosselCopyCrew: {e}")
        print(f"\nErro ao executar o teste: {e}")

    print("\n--- Teste da CarrosselCopyCrew Finalizado ---") 