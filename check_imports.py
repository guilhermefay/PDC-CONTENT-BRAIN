#!/usr/bin/env python
"""
Script de diagnóstico para verificar imports críticos no ambiente de execução.
Execute este script com `python check_imports.py` para verificar se os módulos
necessários estão configurados corretamente.
"""
import os
import sys
import importlib
import traceback
import json

# Lista de módulos/pacotes a verificar
MODULES_TO_CHECK = [
    "rag_api",
    "agents",
    "agents.base",
    "agents.annotator_agent",
    "ingestion.gdrive_ingest",
    "etl.annotate_and_index",
    "infra"
]

# Configuração de caminhos para verificação adicional
current_dir = os.getcwd()
sys.path.insert(0, current_dir)
print(f"Diretório atual: {current_dir}")
print(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'Não definido')}")
print(f"Caminho Python (sys.path): {json.dumps(sys.path, indent=2)}")

# Verificar a estrutura de diretórios
print("\n=== Estrutura de diretórios ===")
for dirpath in [".", "/app", "/app/agents", "/app/ingestion", "/app/etl"]:
    if os.path.exists(dirpath):
        print(f"\nListando conteúdo de '{dirpath}':")
        try:
            for item in os.listdir(dirpath):
                full_path = os.path.join(dirpath, item)
                if os.path.isdir(full_path):
                    print(f"  DIR: {item}/")
                else:
                    print(f"  ARQUIVO: {item}")
        except Exception as e:
            print(f"  ERRO ao listar diretório: {e}")
    else:
        print(f"Diretório '{dirpath}' não existe")

# Verificar os imports
print("\n=== Verificação de Imports ===")
results = []

for module_name in MODULES_TO_CHECK:
    try:
        module = importlib.import_module(module_name)
        results.append({
            "module": module_name,
            "status": "SUCESSO",
            "path": getattr(module, "__file__", "Desconhecido"),
            "error": None
        })
    except ImportError as e:
        results.append({
            "module": module_name,
            "status": "FALHA",
            "path": None,
            "error": str(e)
        })
    except Exception as e:
        results.append({
            "module": module_name,
            "status": "ERRO",
            "path": None,
            "error": f"{type(e).__name__}: {str(e)}"
        })

# Exibir resultados
print("\n=== Resultado da Verificação ===")
for result in results:
    status_marker = "✅" if result["status"] == "SUCESSO" else "❌"
    print(f"{status_marker} {result['module']}: {result['status']}")
    if result["path"]:
        print(f"   📁 Caminho: {result['path']}")
    if result["error"]:
        print(f"   ⚠️ Erro: {result['error']}")
    print()

# Contagem final
success_count = sum(1 for r in results if r["status"] == "SUCESSO")
print(f"\n=== Resumo: {success_count}/{len(results)} módulos importados com sucesso ===")

if success_count < len(results):
    sys.exit(1)
else:
    print("Todos os módulos importados com sucesso!")
    sys.exit(0)