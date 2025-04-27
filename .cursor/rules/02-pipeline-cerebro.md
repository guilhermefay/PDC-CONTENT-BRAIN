# Pipeline “Cérebro” – Visão 10 mil pés

```mermaid
graph LR
    A[Google Drive<br/>/PDC Content] -->|Docling| B(Extrator de texto)
    B --> C(chunk_content.py)
    C --> D(AnnotatorAgent (CrewAI))
    D --> E(Supabase pgvector)
    E --> F(Retrieval / Embeddings)
```

**Fluxo resumido**

1. **Docling** baixa só arquivos novos (.docx, .gdoc, .pdf, .txt).
2. `chunk_content()` quebra em ~1 000 chars.
3. `AnnotatorAgent` avalia → `{keep,tags,reason}`.
4. Chunks `keep` vão pro Supabase (`documents.origin = pasta`).
5. Consulta de conteúdo usa LangChain retriever + embeddings.

### Variáveis de ambiente

```
GOOGLE_SERVICE_ACCOUNT_JSON=
DRIVE_ROOT_FOLDER_ID=
OPENAI_API_KEY=
SUPABASE_URL=
SUPABASE_SERVICE_KEY=
```

Docs técnicos detalhados em `docs/`.