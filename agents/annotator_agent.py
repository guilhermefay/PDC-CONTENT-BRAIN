import os, json, logging, anyio
from enum import Enum
from typing import List, Dict, Any, Set, Optional
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from crewai import Agent, Task, Crew, Process
from .base import BaseAgent
from crewai.crews.crew_output import CrewOutput

# Configurar logging para ESTE módulo especificamente
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')
logger = logging.getLogger(__name__)
logger.info("--- ANNOTATOR_AGENT.PY --- CACHE_BUST_V6_ANNOTATOR --- LOADING ---")

# --- INÍCIO: Copiar função _sanitize_metadata --- 
def _sanitize_metadata(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Converte valores não‑hasháveis (ex.: slice) em string recursivamente."""
    if not meta or not isinstance(meta, dict):
        return {} if meta is None else meta
        
    clean: Dict[str, Any] = {}
    for k, v in meta.items():
        if isinstance(v, slice):
            clean[k] = str(v)
        elif isinstance(v, dict):
            clean[k] = _sanitize_metadata(v) # Chamada recursiva para dicionários aninhados
        elif isinstance(v, list):
             # Processa listas recursivamente (caso contenham dicts ou slices)
             clean[k] = [_sanitize_metadata(item) if isinstance(item, dict) 
                         else str(item) if isinstance(item, slice) 
                         else item for item in v]
        else:
            # Mantém outros tipos hasheáveis como estão
            try:
                hash(v) # Verifica se é hasheável
                clean[k] = v
            except TypeError:
                clean[k] = str(v) # Converte para string se não for hasheável
    return clean
# --- FIM: Copiar função _sanitize_metadata --- 

# load_dotenv(); # Pode ser redundante se já carregado pelo ETL

class Tag(str, Enum):
    """Enumeração das tags permitidas para classificação de conteúdo."""
    PUERICULTURA="puericultura"; SONO_DO_BEBE="sono do bebe"; AMAMENTACAO="amamentacao"
    INTRODUCAO_ALIMENTAR="introducao alimentar"; MARKETING_DIGITAL="marketing digital"
    VENDAS="vendas"; COPYWRITING="copywriting"; LANCAMENTO="lancamento"
    TRAFEGO_PAGO="trafego pago"; GESTAO_DE_TEMPO="gestao de tempo"; PRODUTIVIDADE="produtividade"
    MENTALIDADE="mentalidade"; NEGOCIOS_DIGITAIS="negocios digitais"; FERRAMENTAS="ferramentas"
    INSPIRACIONAL="inspiracional"; GERAL="geral"; INTERNO="interno"; TECNICO="tecnico"

ALLOWED_TAGS: Set[str] = {t.value for t in Tag}
MAX_CHARS, BATCH_SIZE, TIMEOUT = 4000, 16, 45 # BATCH_SIZE relevante para o loop externo

class ChunkIn(BaseModel):
    """Modelo Pydantic para os dados de entrada esperados pela Task do CrewAI."""
    temp_id:int = Field(description="ID temporário para correspondência entrada/saída.")
    content:str = Field(description="O conteúdo textual do chunk a ser analisado.")
    meta:Dict[str,Any] = Field(description="Metadados associados ao chunk (origem, etc.).")

class ChunkOut(BaseModel):
    """Modelo Pydantic para os dados de saída esperados da Task do CrewAI."""
    temp_id:int = Field(description="ID temporário que DEVE corresponder ao da entrada.")
    keep:bool = Field(description="True se o chunk deve ser mantido para RAG, False caso contrário.")
    tags:List[str] = Field(description="Lista de tags relevantes (de ALLOWED_TAGS) aplicadas ao chunk.")
    reason:str = Field(description="Breve justificativa para a decisão 'keep' e as tags aplicadas.")

class AnnotatorAgent(BaseAgent):
    """
    Agente CrewAI responsável por analisar chunks de texto e decidir se devem
    ser mantidos para RAG, além de atribuir tags relevantes.

    Utiliza um LLM (configurado via ChatOpenAI) para executar a tarefa de classificação
    definida no prompt da Task. O resultado esperado é um objeto Pydantic `ChunkOut`.

    Herda de `BaseAgent` (embora `BaseAgent` possa ser simples no momento).
    """
    def __init__(self, model:str="gpt-4o-mini", config: Optional[dict] = None):
        """
        Inicializa o AnnotatorAgent.

        Configura o LLM, o Agente CrewAI subjacente, a Task com o prompt detalhado
        e o Crew que executará a tarefa.

        Args:
            model (str): O nome do modelo LLM a ser usado (padrão: "gpt-4o-mini").
            config (Optional[dict]): Dicionário de configuração adicional (atualmente não usado).
        """
        super().__init__(config) # Chama o __init__ da classe base
        # Configurar LLM com timeout, usando o modelo passado (agora default gpt-4o-mini)
        llm = ChatOpenAI(model=model, temperature=0.1, timeout=TIMEOUT) 
        
        # Definir o Agente CrewAI
        analyst = Agent(role="Analista PDC", llm=llm, verbose=False,
                        goal="Classificar um chunk de texto para o Cérebro PDC",
                        backstory="Você é um especialista em marketing digital e nos conteúdos do PDC."
                        )
        
        # Definir a Task com descrição detalhada e saída Pydantic esperada
        self.task = Task(
            agent=analyst,
            # *** output_pydantic RESTAURADO para ChunkOut ***
            output_pydantic=ChunkOut,
            description=(
                "[ROLE]\n"
                "Você é um Analista de Conteúdo especialista nos materiais e na comunicação do PDC (Pediatra de Sucesso).\n\n"
                "[CONTEXTO]\n"
                "Você receberá um chunk de texto extraído de diversos materiais do PDC (aulas, emails, posts de redes sociais, copys de lançamento, documentos internos, etc.). "
                "O objetivo é classificar esse chunk para construir o Cérebro PDC, uma base de conhecimento útil. "
                "O input será um dicionário Python: `{temp_id: int, content: str, meta: dict}`. A chave `meta` pode conter informações sobre a origem do arquivo.\n\n"
                "[TAREFA]\n"
                "Analise o `content` do chunk fornecido e retorne um objeto Pydantic `ChunkOut` contendo sua classificação.\n\n"
                "[CRITÉRIOS DE CLASSIFICAÇÃO PARA 'keep']\n"
                "- Defina `keep=True` se UMA das seguintes condições for atendida:\n"
                "  (A) O chunk contém informações técnicas, instruções, conceitos ou dados úteis que podem ajudar a responder perguntas futuras de alunos ou da equipe (ex: trechos de aulas, protocolos, definições, dados de pesquisa).\n"
                "  (B) O chunk é um bom exemplo representativo da comunicação, marketing, vendas ou estratégia passada do PDC (ex: um email de lançamento bem escrito, uma copy de vendas, um post de rede social de uma campanha específica, um trecho de planejamento estratégico). Mesmo que não responda a uma pergunta direta, ele serve como referência histórica ou de estilo.\n"
                "- Defina `keep=False` se o chunk for principalmente ruído (transcrição de hesitações como 'uhm', 'ah'), um Call-to-Action (CTA) muito genérico e isolado ('clique aqui!'), conteúdo extremamente obsoleto e sem valor histórico, ou claramente irrelevante para os objetivos do PDC.\n\n"
                "[REGRAS DE TAGS]\n"
                "- As `tags` (uma lista de strings) DEVEM pertencer apenas à seguinte lista: " + f"{list(ALLOWED_TAGS)}" + "\n"
                "- Use tags técnicas (como 'puericultura', 'sono_do_bebe', 'amamentacao', 'introducao_alimentar') para classificar conteúdo de ensino ou técnico (Critério A para `keep=True`).\n"
                "- Use tags de negócio/marketing (como 'marketing_digital', 'vendas', 'copywriting', 'lancamento', 'interno', 'inspiracional', 'gestao_de_tempo', 'produtividade', 'mentalidade', 'negocios_digitais', 'ferramentas') para classificar conteúdo de referência histórica, comunicação ou estratégia (Critério B para `keep=True`).\n"
                "- Aplique múltiplas tags se aplicável, mas seja conciso.\n\n"
                "[REASON]\n"
                "- No campo `reason` (string), explique brevemente (1-2 frases) por que você definiu `keep` como True ou False e quais tags principais você aplicou, conectando com os critérios acima.\n\n"
                "[FORMATO DE SAÍDA]\n"
                "Sua resposta DEVE ser APENAS e EXATAMENTE UM objeto Pydantic `ChunkOut`. A estrutura é: `{temp_id: int, keep: bool, tags: List[str], reason: str}`.\n"
                "CRÍTICO: O `temp_id` no objeto de saída DEVE ser IDÊNTICO ao `temp_id` do objeto de entrada.\n\n"
                "[EXEMPLO]\n"
                "- Input: `{'temp_id': 0, 'content': 'Lembre-se: as vagas para a Mentoria X fecham amanhã! Garanta seu bônus exclusivo clicando no link.', 'meta': {'source_filename': 'email_campanha_mentoriaX_final.eml'}}`\n"
                "- Output Esperado (Exemplo): `ChunkOut(temp_id=0, keep=True, tags=['vendas', 'copywriting', 'lancamento'], reason='Exemplo de email de urgência de fechamento de carrinho para Mentoria X. Útil como referência de copy.')`"
            ),
            expected_output="Um único objeto Pydantic ChunkOut. Exemplo: ChunkOut(temp_id=0, keep=True, tags=['vendas', 'lancamento'], reason='Email de campanha, referência histórica.')"
        )
        self.crew = Crew(agents=[analyst], tasks=[self.task], process=Process.sequential, verbose=0)

    def run(self, original_chunk_dict: Dict[str, Any]) -> Optional[ChunkOut]:
        """
        Processa UM ÚNICO chunk (dicionário), chama o CrewAI 
        e retorna o resultado da anotação (ChunkOut) ou None se falhar.

        Args:
            original_chunk_dict (Dict[str, Any]): O dicionário do chunk a ser processado,
                                                 contendo pelo menos 'content' e opcionalmente 'metadata'.

        Returns:
            Optional[ChunkOut]: O objeto ChunkOut com a anotação se bem-sucedido, ou None.
        """
        doc_id_log = original_chunk_dict.get("document_id", "ID Desconhecido")
        logger.debug(f"[Agent Run] Iniciando processamento para: {doc_id_log}")

        # === Manter validação do chunk individual ===
        if not original_chunk_dict or not original_chunk_dict.get("content"):
            logger.warning(f"[Agent Run - {doc_id_log}] Chunk inválido ou vazio encontrado. Retornando None.")
            return None # << RETORNAR None para chunk inválido

        # === Manter preparação do input para a task ===
        # Pegar metadados originais
        chunk_meta_orig = original_chunk_dict.get("metadata", {})
        # Aplicar sanitização
        chunk_meta_sanitized = _sanitize_metadata(chunk_meta_orig)
        
        logger.debug(f"[Agent Run - {doc_id_log}] Tipos nos metadados SANITIZADOS: {{k: type(v).__name__ for k, v in chunk_meta_sanitized.items()}}")
        origin = chunk_meta_sanitized.get("origin", None)
        source_filename = chunk_meta_sanitized.get("source_filename", None)
        chunk_index = chunk_meta_sanitized.get("chunk_index", None)
        duration_sec = chunk_meta_sanitized.get("duration_sec", None)
        if isinstance(chunk_index, slice):
            logger.critical(f"[Agent Run - {doc_id_log}] ALERTA CRÍTICO! 'chunk_index' AINDA é um objeto slice APÓS sanitização: {chunk_index}.")

        chunk_meta_final_for_task = {
            "origin": origin,
            "source_filename": source_filename,
            "chunk_index": chunk_index,
            "duration_sec": duration_sec
        }
        # Usar o ID do Supabase (inteiro) como temp_id esperado
        # Certifique-se que 'id' existe e é um inteiro; adicione fallback se necessário ou logue erro.
        supabase_chunk_id = original_chunk_dict.get("id")
        if not isinstance(supabase_chunk_id, int):
            logger.error(f"[Agent Run - {doc_id_log}] ID do Supabase ('id') ausente ou não é inteiro no original_chunk_dict: {supabase_chunk_id}. Usando 0 como fallback para temp_id.")
            expected_temp_id = 0
        else:
            expected_temp_id = supabase_chunk_id

        logger.info(f"[Agent Run - {doc_id_log}] VALOR INICIAL DE expected_temp_id: {expected_temp_id}, TIPO: {type(expected_temp_id).__name__}") # LOG ADICIONADO

        chunk_content = original_chunk_dict["content"]
        task_input_dict = {
            "temp_id": expected_temp_id, # Usar o ID do Supabase
            "content": chunk_content[:MAX_CHARS],
            "meta": chunk_meta_final_for_task
        }
        loggable_input = task_input_dict.copy()
        loggable_input["content"] = loggable_input["content"][:100] + "..."
        logger.debug(f"[Agent Run - {doc_id_log}] Input para crew.kickoff (sanitizado): {loggable_input}")

        # --- LOG DETALHADO ANTES DO KICKOFF (manter) ---
        try:
            meta_types_before_kickoff = {k: type(v).__name__ for k, v in task_input_dict.get("meta", {}).items()}
            logger.info(f"[Agent Run - {doc_id_log}] VERIFICANDO TIPOS EM META ANTES DO KICKOFF: {meta_types_before_kickoff}")
            if "chunk_index" in task_input_dict.get("meta", {}):
                 logger.info(f"[Agent Run - {doc_id_log}] VALOR DE META['chunk_index'] ANTES DO KICKOFF: {task_input_dict['meta']['chunk_index']}")
        except Exception as log_exc:
             logger.error(f"[Agent Run - {doc_id_log}] Erro ao tentar logar tipos antes do kickoff: {log_exc}")
        # --- FIM DO LOG DETALHADO ---

        crew_output_result = None
        annotation_result: Optional[ChunkOut] = None

        # === ALTERAÇÃO 3: Try/Except agora envolve a lógica do chunk individual ===
        try:
            logger.debug(f"[Agent Run - {doc_id_log}] Chamando crew.kickoff...")
            crew_output_result = self.crew.kickoff(inputs=task_input_dict)
            logger.debug(f"[Agent Run - {doc_id_log}] Resultado bruto do crew.kickoff: {crew_output_result}")
            logger.debug(f"[Agent Run - {doc_id_log}] Tipo do resultado bruto: {type(crew_output_result).__name__}")

            logger.debug(f"[Agent Run - {doc_id_log}] Tentando extrair ChunkOut do resultado...")
            if crew_output_result and isinstance(crew_output_result, CrewOutput) and hasattr(crew_output_result, 'pydantic') and isinstance(crew_output_result.pydantic, ChunkOut):
                annotation_result = crew_output_result.pydantic
                logger.debug(f"[Agent Run - {doc_id_log}] ChunkOut extraído com sucesso: {annotation_result}")
                logger.info(f"[Agent Run - {doc_id_log}] VALOR DE annotation_result.temp_id IMEDIATAMENTE APÓS EXTRAÇÃO: {annotation_result.temp_id if annotation_result else 'N/A'}, TIPO: {type(annotation_result.temp_id).__name__ if annotation_result else 'N/A'}") # LOG ADICIONADO
                
                # CORREÇÃO: Forçar o temp_id correto, independentemente do que o LLM/CrewAI retornou.
                logger.debug(f"[Agent Run - {doc_id_log}] Forçando temp_id para {expected_temp_id}. Valor original do LLM/CrewAI: {annotation_result.temp_id if annotation_result else 'N/A'}")
                if annotation_result:
                    annotation_result.temp_id = expected_temp_id
                    logger.info(f"[Agent Run - {doc_id_log}] VALOR DE annotation_result.temp_id APÓS FORÇAR: {annotation_result.temp_id}, TIPO: {type(annotation_result.temp_id).__name__}") # LOG ADICIONADO
                else: 
                    logger.error(f"[Agent Run - {doc_id_log}] ALERTA: annotation_result é None ANTES de forçar temp_id. Isso não deveria acontecer aqui se o if externo foi satisfeito.")
                
                # Validar tags
                if annotation_result and hasattr(annotation_result, 'tags') and isinstance(annotation_result.tags, list):
                    invalid_tags = {tag for tag in annotation_result.tags if tag not in ALLOWED_TAGS}
                    if invalid_tags:
                        logger.warning(f"[Agent Run - {doc_id_log}] Tags inválidas encontradas: {invalid_tags}. Removendo-as.")
                        annotation_result.tags = [tag for tag in annotation_result.tags if tag in ALLOWED_TAGS]
                elif annotation_result: # Se annotation_result existe mas não tem tags ou não é lista
                    logger.warning(f"[Agent Run - {doc_id_log}] Campo 'tags' ausente ou não é uma lista no annotation_result. Definindo como lista vazia.")
                    annotation_result.tags = []

                # === ALTERAÇÃO 4: Retornar o ChunkOut se tudo OK ===
                logger.info(f"[Agent Run - {doc_id_log}] Anotação bem-sucedida (temp_id corrigido se necessário).")
                return annotation_result

            else:
                logger.error(f"[Agent Run - {doc_id_log}] Não foi possível extrair ChunkOut do resultado do CrewAI. Resultado: {crew_output_result}")
                if crew_output_result and hasattr(crew_output_result, 'raw') and crew_output_result.raw:
                    logger.error(f"[Agent Run - {doc_id_log}] Conteúdo bruto (raw) do CrewOutput: {crew_output_result.raw}")
                elif crew_output_result:
                    logger.error(f"[Agent Run - {doc_id_log}] CrewOutput não tem 'raw' ou está vazio, __dict__: {crew_output_result.__dict__ if hasattr(crew_output_result, '__dict__') else 'N/A'}")

                if crew_output_result and hasattr(crew_output_result, '__dict__'):
                    logger.debug(f"[Agent Run - {doc_id_log}] Atributos do CrewOutput: {crew_output_result.__dict__}")
                return None # << RETORNAR None se extração falhar

        # === Manter tratamento de exceções ===
        except Exception as e:
            logger.exception(f"[Agent Run - {doc_id_log}] Exceção inesperada durante crew.kickoff ou processamento: {e}")
            return None # << RETORNAR None em caso de exceção

# Exemplo de uso (para teste inicial)
if __name__ == "__main__":
    agent = AnnotatorAgent()

    # Exemplo de trechos - agora processados um a um pela função run
    test_chunks_dicts = [
        {"content": "Neste vídeo, vamos explorar como o marketing de conteúdo pode alavancar suas vendas.", "metadata": {"source_filename": "test1.txt", "chunk_index": 0}},
        {"content": "Clique no link da bio para saber mais e agendar sua consultoria gratuita agora mesmo!", "metadata": {"source_filename": "test2.txt", "chunk_index": 0}},
        {"content": "ehhh então assim tipo sabe como é ne aí a gente foi lá", "metadata": {"source_filename": "test3.txt", "chunk_index": 0}}, # Ruído
        {"content": "Resultados comprovados: nossos clientes aumentaram o engajamento em 50% em apenas 3 meses.", "metadata": {"source_filename": "test4.txt", "chunk_index": 0}}, # Prova social
        {"content": "Você se sente perdido sem saber o que postar? A falta de um calendário editorial te paralisa?", "metadata": {"source_filename": "test5.txt", "chunk_index": 0}}, # Dor
        {"content": " ", "metadata": {"source_filename": "test6.txt", "chunk_index": 0}} # Inválido
    ]

    print("--- Iniciando Processamento (Simulado) ---")
    annotated_results = agent.run(test_chunks_dicts)
    print("--- Fim do Processamento ---")

    print(f"\nResultados Anotados (aprovados):")
    for res in annotated_results:
        print(json.dumps(res, indent=2, ensure_ascii=False))

   