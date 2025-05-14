#!/bin/bash
# Wrapper script para execução do worker no Railway

set -e # Parar em caso de erro 

echo "======================================"
echo "INICIANDO ETL WORKER ($(date))"
echo "Diretório atual: $(pwd)"
echo "======================================"

# Verificar se o PYTHONPATH está configurado
if [ -z "$PYTHONPATH" ]; then
  echo "⚠️ PYTHONPATH não definido. Configurando para /app..."
  export PYTHONPATH=/app
else
  echo "✅ PYTHONPATH já configurado: $PYTHONPATH"
fi

# Verificar variáveis de ambiente críticas 
echo "Verificando variáveis de ambiente críticas..."
REQUIRED_VARS=(
  "SUPABASE_URL"
  "SUPABASE_SERVICE_KEY"
  "GDRIVE_ROOT_FOLDER_ID"
  "GOOGLE_APPLICATION_CREDENTIALS"
)

MISSING_VARS=()
for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    MISSING_VARS+=("$VAR")
  fi
done

if [ ${#MISSING_VARS[@]} -ne 0 ]; then
  echo "⚠️ AVISO: As seguintes variáveis de ambiente não estão definidas:"
  for MVAR in "${MISSING_VARS[@]}"; do
    echo "  - $MVAR"
  done
else
  echo "✅ Todas as variáveis de ambiente críticas estão definidas."
fi

# Testar imports
echo "Executando verificação de imports..."
python /app/check_imports.py
IMPORT_CHECK=$?

if [ $IMPORT_CHECK -ne 0 ]; then
  echo "⚠️ AVISO: Verificação de imports falhou!"
  # Não vamos interromper a execução, apenas alertar
fi

# Verificar conteúdo dos diretórios fundamentais
echo "Listando diretórios fundamentais para diagnóstico..."
ls -la /app/
ls -la /app/agents/
ls -la /app/ingestion/
ls -la /app/etl/

# Iniciar o serviço principal
echo "======================================"
echo "Iniciando worker de ingestão..."
echo "======================================"
python -m ingestion.gdrive_ingest

# Bloco de Teste Injetado
echo "


!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
print "WORKER_ENTRYPOINT.SH: INICIANDO BLOCO DE TESTE DE CONEXÃO SUPABASE"
print "PYTHONUNBUFFERED está configurado como: [$PYTHONUNBUFFERED]"
echo "WORKER_ENTRYPOINT.SH: INICIANDO BLOCO DE TESTE DE CONEXÃO SUPABASE"
echo "PYTHONUNBUFFERED está configurado como: [$PYTHONUNBUFFERED]"
echo "Executando: python /app/worker_service/etl/test_supabase_connect.py"

python /app/worker_service/etl/test_supabase_connect.py
TEST_EXIT_CODE=$?

echo "TESTE DE CONEXÃO PYTHON FINALIZADO COM CÓDIGO DE SAÍDA: $TEST_EXIT_CODE"
echo "WORKER_ENTRYPOINT.SH: Conteúdo de /app/test_run_output.txt (se existir):"
if [ -f /app/test_run_output.txt ]; then
    cat /app/test_run_output.txt
else
    echo "Arquivo /app/test_run_output.txt não encontrado pelo worker_entrypoint.sh.
fi
echo "WORKER_ENTRYPOINT.SH: BLOCO DE TESTE FINALIZADO. SAINDO DO SCRIPT AGORA."
echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


"
exit $TEST_EXIT_CODE # Sair após o teste para não continuar com o worker normal