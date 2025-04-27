-- Criação de tabelas iniciais para pdc-content-brain

-- Tabela para documentos brutos ou processados antes da curadoria
CREATE TABLE IF NOT EXISTS documents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type TEXT, -- Ex: 'audio_transcript', 'youtube_caption', 'article_text'
    source_url TEXT,
    raw_content TEXT,
    annotated_content TEXT,
    embedding vector(1536), -- Ajustar dimensão se usar modelo diferente (OpenAI=1536)
    metadata JSONB, -- Tags, tone, tema, etc. adicionados pelo AnnotatorAgent
    approved BOOLEAN DEFAULT FALSE, -- Flag do CuratorAgent
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Tabela para posts gerados para diferentes plataformas
CREATE TABLE IF NOT EXISTS posts (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    platform TEXT NOT NULL, -- Ex: 'instagram_feed', 'instagram_story', 'email'
    document_id uuid REFERENCES documents(id), -- Documento base (opcional)
    title TEXT,
    content TEXT NOT NULL,
    media_url TEXT, -- URL para imagem/vídeo associado
    status TEXT DEFAULT 'draft', -- Ex: 'draft', 'scheduled', 'published', 'archived'
    scheduled_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Tabela para métricas de posts (granularidade: post + data)
CREATE TABLE IF NOT EXISTS post_metrics (
    post_id uuid REFERENCES posts(id) ON DELETE CASCADE,
    metric_date DATE NOT NULL,
    likes INT DEFAULT 0,
    comments INT DEFAULT 0,
    shares INT DEFAULT 0, -- Ou saves, dependendo da plataforma
    reach INT DEFAULT 0,
    impressions INT DEFAULT 0,
    ad_spend NUMERIC(10, 2) DEFAULT 0.00,
    source TEXT, -- Ex: 'instagram_api', 'manual_input'
    fetched_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (post_id, metric_date)
);

-- Tabela para conteúdo específico de Stories (se diferente de posts)
CREATE TABLE IF NOT EXISTS stories (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    post_id uuid REFERENCES posts(id), -- Link para o post se for uma adaptação
    content TEXT,
    media_url TEXT NOT NULL,
    status TEXT DEFAULT 'draft',
    scheduled_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Calendário de lançamentos/eventos importantes
CREATE TABLE IF NOT EXISTS launch_calendar (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_name TEXT NOT NULL,
    event_type TEXT, -- Ex: 'product_launch', 'webinar', 'promo'
    start_date DATE NOT NULL,
    end_date DATE,
    description TEXT,
    target_audience TEXT, -- Ex: 'tofu', 'mofu', 'bofu'
    related_posts uuid[], -- Array de IDs de posts associados
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Referências criativas (posts inspiradores, etc.)
CREATE TABLE IF NOT EXISTS creative_refs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source_url TEXT NOT NULL UNIQUE,
    platform TEXT,
    content_type TEXT, -- Ex: 'image', 'video', 'carousel', 'text'
    description TEXT, -- Por que é uma boa referência?
    tags TEXT[],
    thumbnail_url TEXT,
    added_at TIMESTAMPTZ DEFAULT now()
);

-- Adicionar Índices básicos
CREATE INDEX IF NOT EXISTS idx_documents_embedding ON documents USING ivfflat (embedding vector_l2_ops) WITH (lists = 100); -- Exemplo de índice IVFFlat
CREATE INDEX IF NOT EXISTS idx_documents_approved ON documents (approved);
CREATE INDEX IF NOT EXISTS idx_posts_status ON posts (status);
CREATE INDEX IF NOT EXISTS idx_posts_scheduled_at ON posts (scheduled_at);
CREATE INDEX IF NOT EXISTS idx_post_metrics_date ON post_metrics (metric_date);


-- Função para atualizar 'updated_at' automaticamente (exemplo)
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Aplicar trigger às tabelas que possuem 'updated_at'
DO $$
DECLARE
  tbl_name TEXT;
BEGIN
  FOR tbl_name IN SELECT table_name FROM information_schema.columns WHERE column_name = 'updated_at' AND table_schema = 'public' LOOP
    EXECUTE format('CREATE TRIGGER set_timestamp BEFORE UPDATE ON %I FOR EACH ROW EXECUTE PROCEDURE trigger_set_timestamp();', tbl_name);
  END LOOP;
END;
$$; 