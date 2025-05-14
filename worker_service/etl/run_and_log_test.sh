#!/bin/sh
echo "--- Wrapper script run_and_log_test.sh INICIADO ---"
# Garantir que o arquivo de log exista e esteja vazio antes do teste
rm -f /app/test_run_output.txt
touch /app/test_run_output.txt
chmod 666 /app/test_run_output.txt # Permissões abertas para escrita, caso o script python não rode como root

echo "Executando script Python: ./worker_service/etl/test_supabase_connect.py"
python ./worker_service/etl/test_supabase_connect.py

exit_code=$?
echo "--- Script Python finalizado com código de saída: $exit_code ---"

echo "--- Conteúdo de /app/test_run_output.txt (após execução do Python): ---"
if [ -f /app/test_run_output.txt ]; then
    cat /app/test_run_output.txt
else
    echo "Arquivo /app/test_run_output.txt não encontrado.
fi
echo "--- Fim do wrapper script run_and_log_test.sh ---"
exit $exit_code # Propagar o código de saída do script Python 