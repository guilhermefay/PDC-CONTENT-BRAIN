import os
from supabase import create_client, Client
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
load_dotenv() # Carrega variáveis do .env

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

# <<<!!! IMPORTANTE: PREENCHA COM AS CREDENCIAIS DO SEU USUÁRIO DE TESTE SUPABASE !!!>>>
TEST_USER_EMAIL = "teste@exemplo.com"
TEST_USER_PASSWORD = "Mariacecilia1"
# <<<!!! FIM DAS CREDENCIAIS !!!>>>


if not SUPABASE_URL or not SUPABASE_ANON_KEY:
    print("Erro: SUPABASE_URL ou SUPABASE_ANON_KEY não estão definidas no .env")
else:
    try:
        # Usar a chave ANÔNIMA para login
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        print("Tentando fazer login com:", TEST_USER_EMAIL)
        response = supabase.auth.sign_in_with_password({
            "email": TEST_USER_EMAIL,
            "password": TEST_USER_PASSWORD
        })

        # Verificar se a sessão e o token existem na resposta
        if hasattr(response, 'session') and response.session and hasattr(response.session, 'access_token') and response.session.access_token:
            print("\nLogin bem-sucedido!")
            print("\n--- SEU ACCESS TOKEN (JWT) ---")
            print(response.session.access_token)
            print("--- COPIE O TOKEN ACIMA ---")
        else:
            print("\nFalha no login. Verifique as credenciais ou a resposta:")
            # Imprimir a resposta completa pode ajudar a depurar
            print(response)

    except Exception as e:
        print(f"\nOcorreu um erro durante a tentativa de login: {e}")
        logging.exception("Erro detalhado:") # Logar o traceback completo

