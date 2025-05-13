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