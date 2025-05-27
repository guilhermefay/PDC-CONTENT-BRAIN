 # PDC-CONTENT-BRAIN

AI-powered content generation and analysis for PDC

<!-- Test comment to trigger deploy -->

Um sistema para ingerir, processar, anotar e indexar conteúdo do PDC (Pediatra de Sucesso) para consulta via RAG (Retrieval-Augmented Generation), utilizando R2R Cloud, Supabase e CrewAI.

## Arquitetura

O sistema é composto pelos seguintes componentes principais:

1.  **Ingestão (Módulo `ingestion`):**
    *   `ingestion/gdrive_ingest.py`:
        *   Responsável por interagir com o Google Drive de forma incremental.
        *   Verifica na tabela `processed_files` do Supabase para identificar arquivos novos ou modificados, evitando reprocessamento desnecessário.
        *   Busca e baixa os arquivos de origem (documentos e vídeos) de pastas configuradas no Google Drive.
        *   Filtra arquivos irrelevantes com base em nomes exatos, extensões configuradas ou se são arquivos ocultos.
        *   Para arquivos de vídeo, invoca o `ingestion/video_transcription.py` para obter o conteúdo textual.
        *   O texto transcrito é combinado com os metadados originais do Google Drive (como ID do arquivo, título, link original) antes de prosseguir para a próxima etapa.
        *   Realiza o "chunking" (divisão em pedaços menores e semanticamente coesos) do conteúdo textual (de documentos ou transcrições com metadados preservados).
        *   Salva cada chunk individualmente na tabela `documents` do Supabase, incluindo seus metadados e um status inicial de processamento (ex: `pending_annotation`).
        *   Após o salvamento bem-sucedido de todos os chunks de um arquivo de origem, registra o arquivo na tabela `processed_files` do Supabase, marcando-o como ingerido.
    *   `ingestion/video_transcription.py`:
        *   Transcreve os arquivos de vídeo baixados utilizando a API do AssemblyAI (configurada para transcrever em português e aplicar formatação de texto).
        *   Possui um mecanismo de fallback para WhisperX caso o AssemblyAI não esteja disponível ou falhe.
        *   Retorna o texto transcrito para o script `gdrive_ingest.py`.

2.  **ETL (Extract, Transform, Load - Módulo `etl`):**
    *   `etl/annotate_and_index.py`: Orquestra o fluxo de anotação e indexação dos chunks de conteúdo:
        *   Busca proativamente por chunks na tabela `documents` do Supabase que estão com o status `pending_annotation`.
        *   Processa cada chunk individualmente, aplicando lógica de transformação e enriquecimento.
        *   Utiliza o `AnnotatorAgent` (implementado em `agents/annotator_agent.py`), que é um agente CrewAI configurado com o modelo `gpt-4o-mini` da OpenAI, para:
            *   Avaliar a relevância e a qualidade do chunk.
            *   Decidir se o chunk deve ser mantido para indexação e uso em RAG (`keep=True`) ou descartado (`keep=False`).
            *   Gerar um breve motivo (`reason`) para a decisão tomada.
            *   Aplicar tags relevantes de uma lista predefinida (`ALLOWED_TAGS`) para categorizar o conteúdo.
        *   Atualiza o registro do chunk na tabela `documents` do Supabase com os resultados da anotação (`keep`, `reason`, `tags`) e altera seu status (ex: `pending_indexing` se `keep=True`, ou `discarded` se `keep=False`).
        *   Para todos os chunks marcados como `keep=True` e com status `pending_indexing`, envia-os para o **R2R Cloud** para indexação vetorial e armazenamento otimizado para busca.
        *   Após a tentativa de indexação, atualiza o status do chunk no Supabase para `indexed` (em caso de sucesso) ou `indexing_failed` (em caso de falha).
        *   O script incorpora mecanismos de resiliência, como retentativas automáticas (usando a biblioteca `tenacity`), para chamadas críticas às APIs externas (Supabase, OpenAI via AnnotatorAgent, R2R Cloud), aumentando a tolerância a falhas transitórias de rede ou serviço.

3.  **R2R Cloud:**
    *   Plataforma externa responsável pela indexação vetorial e execução das buscas semânticas e RAG.
    *   Interação feita através do `infra/r2r_client.py`.
4.  **Supabase:**
    *   Banco de dados PostgreSQL que atua como a espinha dorsal para o armazenamento de metadados, conteúdo intermediário e controle de estado do pipeline:
        *   Tabela `documents`: Armazena todos os chunks de conteúdo extraídos dos arquivos originais. Cada registro de chunk inclui:
            *   O texto do chunk.
            *   Metadados herdados do arquivo de origem (ex: `gdrive_id`, título original, tipo de arquivo).
            *   Os resultados do processo de anotação realizado pelo `AnnotatorAgent` (incluindo a decisão `keep`/`discard`, o motivo `reason` e as `tags` aplicadas).
            *   Um campo de status granular que rastreia a etapa de processamento de cada chunk (ex: `pending_annotation`, `pending_indexing`, `indexed`, `discarded`, `annotation_failed`, `indexing_failed`).
        *   Tabela `processed_files`: Mantém um registro dos arquivos do Google Drive que já foram completamente ingeridos (ou seja, baixados, seus conteúdos extraídos, "chunkados" e os chunks iniciais salvos na tabela `documents`). Isso previne o reprocessamento de arquivos já processados em execuções subsequentes do script de ingestão.
        *   Utilizado para gerenciar autenticação de usuários para a API RAG (e potencialmente futuras interfaces de usuário) através de JWT (JSON Web Tokens).
        *   (Opcional) Permite a configuração de RLS (Row Level Security) para um controle de acesso mais granular aos dados, embora esta funcionalidade possa estar planejada para uma fase futura.

5.  **API RAG (`api/rag_api.py`):**
    *   API FastAPI que:
        *   Autentica usuários via JWT do Supabase.
        *   Recebe queries dos usuários.
        *   Interage com o **R2R Cloud** (através do `infra/r2r_client.py`) para realizar buscas semânticas (`search`) ou RAG agentic (`rag`).
        *   Retorna os resultados para o cliente.
6.  **Infraestrutura (`infra/`):**
    *   `r2r_client.py`: Wrapper robusto para a API R2R Cloud, incluindo retries.
    *   `resilience.py`: Utilitários como `RetryHandler` (baseado em `tenacity`) para implementar chamadas de rede resilientes e configuráveis, usado nos módulos de ETL e ingestão.
7.  **Agentes & Crews (`agents/`, `crews/`):
    *   `agents/annotator_agent.py`: Implementação do agente CrewAI, utilizando o modelo `gpt-4o-mini` da OpenAI. Este agente é responsável por processar um único chunk de conteúdo por vez, realizando a avaliação de relevância, decidindo sobre sua manutenção para o sistema RAG e aplicando tags de categorização.
    *   `agents/base.py`, `crews/base.py`: Contêm a estrutura e classes base para facilitar a criação e gerenciamento de futuros agentes e crews no sistema.
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
    *   Copie o arquivo de exemplo:
        ```bash
        cp .env.sample .env
        ```
    *   Edite o arquivo `.env` e preencha **todos** os valores com suas credenciais e configurações. Consulte o arquivo `.env.sample` para a lista completa de variáveis necessárias e seus respectivos propósitos (inclui Supabase, OpenAI, R2R Cloud, AssemblyAI, Google Drive, etc.). **Atenção especial às credenciais do Google Cloud para acesso ao Drive.**

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

6.  **Configurar Credenciais Google Drive:**
    *   No Google Cloud Console, crie uma Conta de Serviço para o projeto que terá acesso ao Google Drive.
    *   Faça o download do arquivo JSON contendo as credenciais dessa conta de serviço.
    *   Habilite a API do Google Drive para o seu projeto no Google Cloud Console.
    *   Compartilhe as pastas específicas do Google Drive que você deseja que o sistema ingira com o endereço de e-mail da conta de serviço criada (este e-mail pode ser encontrado dentro do arquivo JSON de credenciais).
    *   Para configurar as credenciais no seu ambiente:
        *   **Opção 1 (Recomendado para Deploy, como Railway):**
            1.  Converta todo o conteúdo do arquivo JSON de credenciais para uma string Base64.
            2.  Defina a variável de ambiente `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64` no seu arquivo `.env` (ou diretamente nas configurações de variáveis de ambiente da sua plataforma de deploy) com esta string Base64.
        *   **Opção 2 (Para Desenvolvimento Local):**
            1.  Defina a variável de ambiente `GOOGLE_SERVICE_ACCOUNT_JSON_PATH` no seu arquivo `.env` com o caminho absoluto para o local onde você salvou o arquivo JSON de credenciais.
    *   O sistema priorizará `GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT_BASE64` se ambas estiverem definidas.

## Uso

Recomenda-se usar o `Makefile` para as tarefas comuns, mas a execução direta dos scripts também é possível.

*   **Executar Testes:**
    ```bash
    make test
    # Ou com relatório de cobertura:
    make coverage
    ```

*   **Executar o Pipeline de Ingestão e ETL (Fonte Google Drive):**
    *   O pipeline completo para processar conteúdo do Google Drive e prepará-lo para RAG envolve duas etapas principais, que geralmente são executadas em sequência:
        1.  **Etapa 1: Ingestão de Conteúdo do Google Drive**
            *   **Comando:** `python -m ingestion.gdrive_ingest`
            *   **Descrição:** Este script é responsável por:
                *   Conectar-se ao Google Drive usando as credenciais configuradas.
                *   Verificar a tabela `processed_files` no Supabase para identificar arquivos que ainda não foram processados ou que foram modificados desde a última ingestão.
                *   Baixar os arquivos novos/modificados (documentos, vídeos, etc.).
                *   Para vídeos, solicitar a transcrição através do `ingestion/video_transcription.py`.
                *   Dividir o conteúdo textual (de documentos ou transcrições) em chunks menores.
                *   Salvar cada chunk, juntamente com metadados relevantes (ID do GDrive, etc.), na tabela `documents` do Supabase com o status inicial `pending_annotation`.
                *   Após o processamento completo e bem-sucedido de um arquivo, registrá-lo na tabela `processed_files`.
        2.  **Etapa 2: Anotação e Indexação de Chunks**
            *   **Comando:** `python -m etl.annotate_and_index`
            *   **Descrição:** Este script é responsável por:
                *   Consultar a tabela `documents` no Supabase para encontrar chunks com o status `pending_annotation`.
                *   Para cada um desses chunks, utilizar o `AnnotatorAgent` (CrewAI) para determinar se o chunk deve ser mantido (`keep=True/False`), gerar um motivo para a decisão e aplicar tags de categorização.
                *   Atualizar o chunk no Supabase com os resultados da anotação e o novo status (ex: `pending_indexing` ou `discarded`).
                *   Para chunks marcados como `keep=True`, enviá-los ao R2R Cloud para serem indexados para busca vetorial.
                *   Atualizar o status final do chunk no Supabase (ex: `indexed` ou `indexing_failed`).
    *   **Execução Local via `Makefile`:**
        *   Certifique-se de que todas as variáveis de ambiente necessárias (Google Drive, AssemblyAI, Supabase, R2R Cloud, OpenAI) estão corretamente configuradas no seu arquivo `.env`.
        *   O comando `make run-etl` no `Makefile` atual executa **apenas a Etapa 2 (Anotação e Indexação)**:
        ```bash
        make run-etl
        ```
        *   Para executar o pipeline completo localmente, você precisará executar a Etapa 1 primeiro, seguida pela Etapa 2:
            ```bash
            python -m ingestion.gdrive_ingest
            # Após a conclusão da ingestão:
            make run-etl # Ou python -m etl.annotate_and_index
            ```
        *   *(Nota: O `Makefile` pode ser atualizado no futuro para incluir um comando que orquestre ambas as etapas, como `make run-full-pipeline` ou similar).*
    *   **Acompanhamento:**
        *   O progresso e possíveis erros podem ser acompanhados pelos logs emitidos no console durante a execução dos scripts (ou em arquivos de log, se a saída for redirecionada).
        *   O estado detalhado de cada arquivo ingerido (`processed_files`) e de cada chunk (`documents`) pode ser monitorado diretamente nas respectivas tabelas no Supabase.
    *   O diretório temporário de vídeos (se houver) **não** é limpo automaticamente pelo script de ingestão; a gestão desse diretório (ex: limpeza periódica ou upload para outro local) é uma responsabilidade externa ao fluxo principal.

*   **Iniciar a API RAG:**
    ```