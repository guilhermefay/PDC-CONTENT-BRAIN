# PDC-CONTENT-BRAIN

AI-powered content generation and analysis for PDC

<!-- Test comment to trigger deploy -->

Um sistema para ingerir, processar, anotar e indexar conteúdo do PDC (Pediatra de Sucesso) para consulta via RAG (Retrieval-Augmented Generation), utilizando R2R Cloud, Supabase e CrewAI.

## Arquitetura

O sistema é composto pelos seguintes componentes principais:

1.  **Ingestão:**
    *   `ingestion/gdrive_ingest.py`: Busca e baixa documentos e vídeos de pastas configuradas no Google Drive.
    *   `ingestion/video_transcription.py`: Transcreve os vídeos baixados usando AssemblyAI (com fallback para WhisperX).
2.  **ETL (Extract, Transform, Load):**
    *   `etl/annotate_and_index.py`: Orquestra o fluxo principal:
        *   Recebe dados do módulo `ingestion/gdrive_ingest.py` (que já inclui documentos e transcrições de vídeos).
        *   Faz o "chunking" (divisão em pedaços menores) do conteúdo textual.
        *   Usa `AnnotatorAgent` (`agents/annotator_agent.py` baseado em CrewAI) para avaliar e marcar cada chunk (se deve ser mantido para RAG).
        *   Armazena **todos** os chunks (anotados com `keep`/`discard` e tags) no **Supabase** para registro e análise futura.
        *   Envia **apenas** os chunks marcados como `keep=True` para o **R2R Cloud** para indexação vetorial e busca RAG.
3.  **R2R Cloud:**
    *   Plataforma externa responsável pela indexação vetorial e execução das buscas semânticas e RAG.
    *   Interação feita através do `infra/r2r_client.py`.
4.  **Supabase:**
    *   Banco de dados PostgreSQL usado para:
        *   Armazenar os chunks brutos e suas anotações (`keep`, `reason`, `tags`).
        *   Gerenciar autenticação de usuários (API e potencialmente futuras interfaces) via JWT.
        *   (Opcional) RLS (Row Level Security) pode ser configurada para controle de acesso granular (ver Tarefa #28 - adiada).
5.  **API RAG (`api/rag_api.py`):**
    *   API FastAPI que:
        *   Autentica usuários via JWT do Supabase.
        *   Recebe queries dos usuários.
        *   Interage com o **R2R Cloud** (através do `infra/r2r_client.py`) para realizar buscas semânticas (`search`) ou RAG agentic (`rag`).
        *   Retorna os resultados para o cliente.
6.  **Infraestrutura (`infra/`):**
    *   `r2r_client.py`: Wrapper robusto para a API R2R Cloud, incluindo retries.
    *   `resilience.py`: Utilitários como `RetryHandler` para chamadas de rede resilientes.
7.  **Agentes & Crews (`agents/`, `crews/`):
    *   `agents/annotator_agent.py`: Implementação do agente CrewAI para anotar chunks de conteúdo.
    *   `agents/base.py`, `crews/base.py`: Estrutura base para futuros agentes e crews.
8.  **DevOps & Utilitários:**
    *   `Makefile`: Comandos para tarefas comuns (veja Uso).
    *   `.env.sample`: Modelo para variáveis de ambiente.
    *   `requirements.txt`, `requirements-dev.txt`: Dependências Python.
    *   `.coveragerc`: Configuração para relatórios de cobertura de testes.

*(Diagrama de arquitetura pode ser adicionado aqui posteriormente)*

## Configuração (Setup)

1.  **Pré-requisitos:**
    *   Python 3.10+
    *   Node.js e npm/pnpm (para Supabase CLI e Task Master)
    *   Conta Supabase
    *   Conta OpenAI (para CrewAI)
    *   Conta R2R Cloud
    *   (Opcional) Conta AssemblyAI (para transcrição de vídeo)
    *   (Opcional) Credenciais de Serviço Google Cloud (para Google Drive)

2.  **Clonar o Repositório:**
    ```bash
    git clone <url-do-repositorio>
    cd PDC-CONTENT-BRAIN
    ```

3.  **Instalar Dependências:**
    ```bash
    # Dependências Python
    pip install -r requirements.txt
    # Dependências de desenvolvimento (inclui pytest, coverage, etc.)
    pip install -r requirements-dev.txt
    # Task Master (se for usar CLI ou servidor MCP localmente)
    npm install -g task-master-ai # ou pnpm add -g task-master-ai
    # Supabase CLI (para migrações)
    npm install supabase --save-dev # ou pnpm add -D supabase
    ```

4.  **Configurar Variáveis de Ambiente:**
    *   Copie o arquivo de exemplo (será criado por esta tarefa):
        ```bash
        cp .env.sample .env
        ```
    *   Edite o arquivo `.env` e preencha **todos** os valores com suas credenciais. Veja o arquivo `.env.sample` para a lista completa de variáveis necessárias e seus propósitos (inclui Supabase, OpenAI, R2R, AssemblyAI, Google Drive).

5.  **Configurar Banco de Dados Supabase:**
    *   Faça login e vincule o projeto usando o Supabase CLI:
        ```bash
        npx supabase login
        npx supabase link --project-ref <seu-project-ref>
        # Pode ser necessário fornecer a senha do banco de dados
        ```
    *   Aplique as migrações para criar as tabelas necessárias:
        ```bash
        npx supabase db push
        ```
    *   Isso criará as tabelas (ex: `documents`) conforme definido em `supabase_config/migrations/`.

6.  **Configurar Credenciais Google Drive (Opcional):**
    *   Siga as instruções do Google Cloud para criar uma Conta de Serviço.
    *   Baixe o arquivo JSON de credenciais.
    *   Defina a variável de ambiente `GOOGLE_SERVICE_ACCOUNT_JSON` no seu arquivo `.env` com o caminho para este arquivo JSON.
    *   Habilite a API do Google Drive para o projeto no Google Cloud Console.
    *   Compartilhe as pastas do Google Drive que você deseja ingerir com o email da conta de serviço.

## Uso

Recomenda-se usar o `Makefile` para as tarefas comuns.

*   **Executar Testes:**
    ```bash
    make test
    # Ou com relatório de cobertura:
    make coverage
    ```

*   **Executar o ETL Completo (Fonte Google Drive):**
    *   Certifique-se de que as variáveis de ambiente e credenciais (GDrv, AssemblyAI, Supabase, R2R, OpenAI) estão configuradas no `.env`.
    *   Execute:
        ```bash
        make run-etl
        ```
    *   Acompanhe os logs (`logs/etl.log`) para ver o progresso.
    *   Um resumo da ingestão será salvo (ver logs ou `.env.sample` para o caminho padrão).
    *   O diretório temporário de vídeos (se houver) **não** é limpo automaticamente; a limpeza é responsabilidade do processo que consome os vídeos (ex: upload para outro local).

*   **Iniciar a API RAG:**
    ```bash
    make start-api
    ```
    *   A API estará disponível em `http://127.0.0.1:8000`.
    *   A documentação interativa (Swagger UI) está em `http://127.0.0.1:8000/docs`.

*   **Consultar a API:**
    *   Obtenha um token JWT válido do Supabase (você pode usar `get_token.py` para gerar um token de teste se configurar as credenciais de usuário nele).
    *   Use `curl`, Postman, etc., para enviar requisições POST para `http://127.0.0.1:8000/query`.
    *   Inclua o header `Authorization: Bearer <seu_jwt_token>`.
    *   Corpo da requisição (exemplo busca simples):
        ```json
        {"query": "Como lidar com sono do bebê?", "top_k": 5}
        ```
    *   Corpo da requisição (exemplo RAG agentic):
        ```json
        {"query": "Faça um resumo sobre as principais dificuldades na amamentação.", "use_rag": true}
        ```

*   **Verificar Linting:**
    ```bash
    make lint
    ```

*   **Limpar Arquivos Gerados:**
    ```bash
    make clean
    ```

## Testes

*   Os testes unitários e de integração estão localizados no diretório `tests/`.
*   Use `make test` para rodar todos os testes.
*   Use `make coverage` para rodar testes e gerar um relatório de cobertura.
*   O arquivo `.coveragerc` define as configurações de cobertura, incluindo módulos a serem medidos e omitidos.
*   Testes de integração específicos podem exigir configuração adicional (ex: mocks para APIs externas ou acesso a um ambiente de staging).

## Task Master (Gerenciamento de Tarefas)

Este projeto utiliza o [Task Master AI](https://github.com/your-repo/task-master-ai) (link hipotético) para gerenciamento de tarefas de desenvolvimento.

*   **Ver Tarefas:** `task-master list`
*   **Ver Próxima Tarefa:** `task-master next`
*   **Marcar Tarefa como Concluída:** `task-master set-status --id=<ID> --status=done`
*   *(Consulte `README-task-master.md` ou a documentação do Task Master para mais comandos)*

## Próximos Passos / TODO

*   Implementar agentes/crews adicionais (Content Strategy, Launch Commander).
*   Refinar controle de acesso (RLS/Folder-based).
*   Melhorar monitoramento e dashboards.
*   Configurar pipeline CI/CD completo (GitHub Actions).
*   Otimizar performance do ETL e API. 
*   Workflow: See [`dev_workflow.mdc`](mdc:.cursor/rules/dev_workflow.mdc) 