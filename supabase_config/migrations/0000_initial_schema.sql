-- Criação de tabela inicial simplificada para pdc-content-brain (foco em raw storage)

-- Tabela para documentos brutos ou processados antes da curadoria R2R
CREATE TABLE IF NOT EXISTS documents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content TEXT, -- Conteúdo textual principal do documento/chunk
    metadata JSONB, -- Campos flexíveis: source_name, origin_url, page_number, chunk_index, etc.
    annotation_tags TEXT[], -- Tags atribuídas pela anotação (CrewAI)
    annotation_keep BOOLEAN DEFAULT NULL, -- Flag do AnnotatorAgent indicando se deve ir pro R2R (NULL = não anotado)
    annotation_reason TEXT, -- Justificativa para keep/discard
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Adicionar Índice para o status de anotação
CREATE INDEX IF NOT EXISTS idx_documents_annotation_keep ON documents (annotation_keep);

-- Função para atualizar 'updated_at' automaticamente
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Aplicar trigger à tabela documents
CREATE TRIGGER set_timestamp
BEFORE UPDATE ON documents
FOR EACH ROW
EXECUTE PROCEDURE trigger_set_timestamp();

-- Habilitar Row Level Security (RLS)
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;

-- Políticas RLS básicas:
-- 1. Permitir que service_role (usado pelo backend/ETL) faça tudo.
--    (Supabase cria service_role por padrão)
CREATE POLICY "Allow ALL for service_role" ON documents
    FOR ALL
    USING (auth.role() = 'service_role');

-- 2. Permitir que usuários autenticados leiam documentos.
--    (Isso pode precisar ser refinado depois com base em times/permissões específicas)
CREATE POLICY "Allow SELECT for authenticated users" ON documents
    FOR SELECT
    USING (auth.role() = 'authenticated');

-- 3. (Opcional/Exemplo) Permitir que usuários insiram documentos (se aplicável via API direta)
-- CREATE POLICY "Allow INSERT for authenticated users" ON documents
--     FOR INSERT
--     WITH CHECK (auth.role() = 'authenticated');

-- NOTA: Políticas de UPDATE e DELETE geralmente são mais restritas
--       e podem depender de ownership ou roles específicos (team/student).
--       Por enquanto, apenas service_role pode modificar/deletar. 