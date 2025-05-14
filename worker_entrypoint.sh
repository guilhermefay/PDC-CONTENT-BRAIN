#!/bin/bash
# Wrapper script para execução do worker no Railway

set -e # Parar em caso de erro 

echo "======================================"
echo "INICIANDO ETL WORKER (PRODUÇÃO - $(date))"
echo "Diretório atual: $(pwd)"
echo "PYTHONPATH: $PYTHONPATH"
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
  # Removido GDRIVE_ROOT_FOLDER_ID e GOOGLE_APPLICATION_CREDENTIALS da lista de obrigatórios por enquanto,
  # pois o foco é o pipeline de anotação/R2R que pode não depender diretamente deles aqui.
  # Adicionar de volta se forem realmente críticos para a inicialização do worker ANTES da lógica principal.
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
  # Considerar sair com erro se variáveis realmente críticas estiverem faltando para produção
  # exit 1 
else
  echo "✅ Todas as variáveis de ambiente críticas verificadas estão definidas."
fi

# Testar imports (opcional para produção, mas pode ser mantido para sanity check)
echo "Executando verificação de imports..."
python /app/check_imports.py
IMPORT_CHECK=$?

if [ $IMPORT_CHECK -ne 0 ]; then
  echo "CRÍTICO: Verificação de imports FALHOU! Verifique os logs acima. Saindo." 
  exit 1 # Sair se os imports críticos falharem
else
  echo "✅ Verificação de imports concluída com sucesso."
fi

# Verificar conteúdo dos diretórios fundamentais (opcional para produção)
# echo "Listando diretórios fundamentais para diagnóstico..."
# ls -la /app/
# ls -la /app/worker_service/etl/ # Mais específico

# Executa o script principal do pipeline ETL.
# O monkey-patch para HTTP/1.1 e a correção do RetryHandler estão em annotate_and_index.py
# e resilience.py, respectivamente.
# Usar python -u para saída não bufferizada, o que pode ajudar nos logs do Railway.
exec python -u /app/worker_service/etl/annotate_and_index.py

# O bloco de teste foi removido daqui.