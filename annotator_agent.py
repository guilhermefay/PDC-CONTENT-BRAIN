import os
import json
from openai import OpenAI
from dotenv import load_dotenv

# Carregar variáveis de ambiente (incluindo OPENAI_API_KEY)
load_dotenv()

# Configurar cliente OpenAI
client = OpenAI()

# TODO: Considerar tiktoken para contagem de tokens e validação de limite

ANNOTATOR_SYSTEM_PROMPT = """
Você é um agente assistente especialista em análise de conteúdo para marketing digital.
Sua tarefa é analisar trechos de conteúdo (transcrições de áudio, legendas de vídeo, textos de artigos) 
e determinar se o trecho deve ser mantido para uso posterior, atribuir tags relevantes e 
explicar o motivo da sua decisão.

Responda APENAS com um objeto JSON válido contendo as seguintes chaves:
- "keep": (boolean) true se o trecho for útil e relevante, false caso contrário (ruído, redundância, irrele
vante).
- "tags": (array de strings) Lista de tags descritivas (ex: ["dor_cliente", "solucao_produto", "prova_social
", "chamada_acao", "tom_inspirador", "nivel_consciencia_problema"]). Use no máximo 5 tags.
- "reason": (string) Breve explicação concisa (máximo 2 frases) do porquê da decisão "keep" e das tags 
escolhidas.
"""

DEFAULT_MODEL = "gpt-4o" # Ou outro modelo apropriado

class AnnotatorAgent:
    def __init__(self, model=DEFAULT_MODEL):
        self.model = model

    def _call_openai(self, content_snippet):
        """Chama a API da OpenAI para analisar um trecho de conteúdo."""
        try:
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": ANNOTATOR_SYSTEM_PROMPT},
                    {"role": "user", "content": f"Analise o seguinte trecho de conteúdo:\n\n```{content_snippet}```"}
                ],
                response_format={"type": "json_object"}, # Garantir que a resposta seja JSON
                temperature=0.2, # Baixa temperatura para respostas mais determinísticas
                max_tokens=250 # Limitar o tamanho da resposta JSON
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"Erro ao chamar a API da OpenAI: {e}")
            # Retornar um erro padronizado ou tentar novamente?
            return {"keep": False, "tags": ["error"], "reason": f"Falha na análise: {e}"}

    def annotate_snippet(self, content_snippet):
        """Analisa e anota um único trecho de conteúdo."""
        if not content_snippet or not isinstance(content_snippet, str) or len(content_snippet.strip()) < 10:
            # Ignorar trechos muito curtos ou inválidos
            return {"keep": False, "tags": ["invalid_snippet"], "reason": "Trecho muito curto ou inválido."}

        # TODO: Adicionar validação de contagem de tokens antes de enviar

        annotation = self._call_openai(content_snippet)

        # TODO: Adicionar validação do JSON retornado (presença de chaves, tipos)

        return annotation

    def process_batch(self, snippets):
        """Processa uma lista de trechos de conteúdo em lote."""
        # TODO: Implementar processamento em lote real (talvez com chamadas assíncronas ou API Batch da OpenAI se aplicável)
        # Por agora, processa sequencialmente
        results = []
        for i, snippet in enumerate(snippets):
            print(f"Processando trecho {i+1}/{len(snippets)}...")
            annotation = self.annotate_snippet(snippet)
            results.append({
                "original_snippet": snippet,
                "annotation": annotation
            })
        return results

    def save_to_supabase(self, annotated_batch):
        """Salva os resultados anotados no banco de dados Supabase."""
        # TODO: Implementar integração com Supabase
        #       - Conectar ao Supabase usando as credenciais do .env
        #       - Inserir/Atualizar registros na tabela 'documents'
        #         - Mapear a anotação para os campos `annotated_content` (ou `metadata`?), `approved`
        #         - Gerar e salvar `embedding` se `keep` for true
        print(f"\n--- Simulação de salvamento no Supabase ({len(annotated_batch)} registros) ---")
        for item in annotated_batch:
            if item["annotation"]["keep"]:
                print(f"  - Mantido: {item["annotation"]["tags"]} ({item["annotation"]["reason"]}) - Snippet: {item['original_snippet'][:50]}...")
            else:
                print(f"  - Descartado: ({item["annotation"]["reason"]}) - Snippet: {item['original_snippet'][:50]}...")
        print("--- Fim da Simulação ---")
        pass # Placeholder

# Exemplo de uso (para teste inicial)
if __name__ == "__main__":
    agent = AnnotatorAgent()

    # Exemplo de trechos
    test_snippets = [
        "Neste vídeo, vamos explorar como o marketing de conteúdo pode alavancar suas vendas.",
        "Clique no link da bio para saber mais e agendar sua consultoria gratuita agora mesmo!",
        "ehhh então assim tipo sabe como é ne aí a gente foi lá", # Ruído
        "Resultados comprovados: nossos clientes aumentaram o engajamento em 50% em apenas 3 meses.", # Prova social
        "Você se sente perdido sem saber o que postar? A falta de um calendário editorial te paralisa?", # Dor
        " " # Inválido
    ]

    print("--- Iniciando Processamento em Lote (Simulado) ---")
    annotated_results = agent.process_batch(test_snippets)
    print("--- Fim do Processamento ---")

    agent.save_to_supabase(annotated_results) 