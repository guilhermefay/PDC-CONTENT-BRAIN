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

    def run(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processa uma lista de chunks (dicionários), chama o CrewAI para cada um
        e retorna uma lista de dicionários dos chunks que foram APROVADOS.

        Itera sobre os chunks de entrada, prepara o input para a task CrewAI,
        executa a task, valida a saída (ChunkOut) e atualiza o dicionário
        original do chunk com os resultados da anotação se `keep=True`.

        Args:
            chunks (List[Dict[str, Any]]): Uma lista de dicionários, onde cada
                dicionário representa um chunk e deve conter pelo menos a chave 'content'
                e opcionalmente 'metadata'.

        Returns:
            List[Dict[str, Any]]: Uma lista contendo apenas os dicionários dos chunks
                                   originais que foram marcados com `keep=True` pelo agente,
                                   agora atualizados com as chaves 'keep', 'tags' e 'reason'.
        """
        approved = []
        # O BATCH_SIZE aqui controla apenas o log/agrupamento externo, não o envio ao LLM.
        # O kickoff do CrewAI processa um item por vez conforme a estrutura atual.
        for i in range(0, len(chunks), BATCH_SIZE):
            subset = chunks[i:i + BATCH_SIZE]
            logger.info(f"Processando lote de anotação: {i+1}-{min(i+len(subset), len(chunks))}/{len(chunks)}")

            for j, original_chunk_dict in enumerate(subset):
                # === LOG ADICIONAL: Início do processamento do chunk ===
                current_chunk_index = i + j
                chunk_meta_orig = original_chunk_dict.get("metadata", {}) # Pegar metadados originais
                # --- APLICAR SANITIZAÇÃO AQUI --- 
                chunk_meta_sanitized = _sanitize_metadata(chunk_meta_orig)
                # ----------------------------------
                source_filename_log = chunk_meta_sanitized.get('source_filename', 'N/A') # Usar metadados sanitizados para log
                chunk_index_log = chunk_meta_sanitized.get('chunk_index', 'N/A')
                logger.debug(f"[Agent Run - {current_chunk_index}] Iniciando processamento para: {source_filename_log} - chunk {chunk_index_log}")

                if not original_chunk_dict or not original_chunk_dict.get("content"):
                    logger.warning(f"[Agent Run - {current_chunk_index}] Chunk inválido ou vazio encontrado. Pulando.")
                    continue

                # Preparar os dados de entrada para a task (dicionário Python)
                chunk_content = original_chunk_dict["content"]
                # === LOG ADICIONAL: Verificar tipos nos metadados SANITIZADOS ===
                meta_to_log = chunk_meta_sanitized # <--- Usar metadados sanitizados aqui
                logger.debug(f"[Agent Run - {current_chunk_index}] Tipos nos metadados SANITIZADOS: {{k: type(v).__name__ for k, v in meta_to_log.items()}}")
                # Usar get para evitar KeyError e logar se chave não existe
                origin = meta_to_log.get("origin", None)
                source_filename = meta_to_log.get("source_filename", None)
                chunk_index = meta_to_log.get("chunk_index", None) # Agora deve ser string se era slice
                duration_sec = meta_to_log.get("duration_sec", None)
                # Log específico se chunk_index ainda for slice (não deveria acontecer)
                if isinstance(chunk_index, slice):
                    logger.critical(f"[Agent Run - {current_chunk_index}] ALERTA CRÍTICO! 'chunk_index' AINDA é um objeto slice APÓS sanitização: {chunk_index}.")

                # CRIA task_input_dict usando metadados JÁ SANITIZADOS
                chunk_meta_final_for_task = {
                    "origin": origin,
                    "source_filename": source_filename,
                    "chunk_index": chunk_index,
                    "duration_sec": duration_sec
                }
                # Usar um ID temporário consistente (0) para o LLM comparar na saída
                expected_temp_id = 0
                task_input_dict = {
                    "temp_id": expected_temp_id,
                    "content": chunk_content[:MAX_CHARS],
                    "meta": chunk_meta_final_for_task # <--- Passa metadados JÁ SANITIZADOS para a task
                }
                # === LOG ADICIONAL: Input para o CrewAI ===
                # Cuidado ao logar 'content' inteiro se for muito grande
                loggable_input = task_input_dict.copy()
                loggable_input["content"] = loggable_input["content"][:100] + "..." # Logar apenas início do conteúdo
                logger.debug(f"[Agent Run - {current_chunk_index}] Input para crew.kickoff (sanitizado): {loggable_input}")

                # --- LOG DETALHADO ANTES DO KICKOFF ---
                try:
                    meta_types_before_kickoff = {k: type(v).__name__ for k, v in task_input_dict.get("meta", {}).items()}
                    logger.info(f"[Agent Run - {current_chunk_index}] VERIFICANDO TIPOS EM META ANTES DO KICKOFF: {meta_types_before_kickoff}")
                    # Logar o valor específico de chunk_index se existir
                    if "chunk_index" in task_input_dict.get("meta", {}):
                         logger.info(f"[Agent Run - {current_chunk_index}] VALOR DE META['chunk_index'] ANTES DO KICKOFF: {task_input_dict['meta']['chunk_index']}")
                except Exception as log_exc:
                     logger.error(f"[Agent Run - {current_chunk_index}] Erro ao tentar logar tipos antes do kickoff: {log_exc}")
                # --- FIM DO LOG DETALHADO ---

                crew_output_result = None # Inicializar fora do try
                annotation_result: Optional[ChunkOut] = None # Inicializar fora do try

                try:
                    # Chamar o CrewAI.
                    # Espera-se que o kickoff retorne um objeto CrewOutput
                    logger.debug(f"[Agent Run - {current_chunk_index}] Chamando crew.kickoff...")
                    crew_output_result = self.crew.kickoff(inputs=task_input_dict)
                    # === LOG ADICIONAL: Resultado bruto do CrewAI ===
                    logger.debug(f"[Agent Run - {current_chunk_index}] Resultado bruto do crew.kickoff: {crew_output_result}")
                    logger.debug(f"[Agent Run - {current_chunk_index}] Tipo do resultado bruto: {type(crew_output_result).__name__}")

                    # Extrair o resultado Pydantic (ChunkOut) do CrewOutput
                    logger.debug(f"[Agent Run - {current_chunk_index}] Tentando extrair ChunkOut do resultado...")
                    if crew_output_result and isinstance(crew_output_result, CrewOutput) and hasattr(crew_output_result, 'pydantic') and isinstance(crew_output_result.pydantic, ChunkOut):
                        annotation_result = crew_output_result.pydantic
                        logger.debug(f"[Agent Run - {current_chunk_index}] ChunkOut extraído com sucesso: {annotation_result}")
                    else:
                        # Log detalhado se a estrutura for inesperada
                        logger.error(f"[Agent Run - {current_chunk_index}] Não foi possível extrair ChunkOut do resultado do CrewAI. Resultado recebido: {crew_output_result}")
                        if crew_output_result and hasattr(crew_output_result, '__dict__'):
                            logger.debug(f"[Agent Run - {current_chunk_index}] Atributos do CrewOutput: {crew_output_result.__dict__}")
                        # Continuar para o próximo chunk se não puder extrair
                        logger.warning(f"[Agent Run - {current_chunk_index}] Pulando para o próximo chunk devido à falha na extração do ChunkOut.")
                        continue # << IMPORTANTE: Pular para o próximo chunk se a extração falhar

                    # *** O restante da lógica agora usa annotation_result (que é ChunkOut ou None) ***
                    # A verificação if annotation_result agora acontece implicitamente,
                    # pois o 'continue' acima impede a execução se for None.

                    # Verificar se o temp_id retornado corresponde ao esperado (0)
                    if annotation_result.temp_id != expected_temp_id:
                         logger.error(f"[Agent Run - {current_chunk_index}] Erro de correspondência de temp_id! Esperado: {expected_temp_id}, Recebido: {annotation_result.temp_id}. Pulando chunk {source_filename_log} index {chunk_index_log}.")
                         continue # Pular para o próximo chunk

                    # Verificar se o chunk deve ser mantido
                    if annotation_result.keep:
                        # ATUALIZAR O DICIONÁRIO ORIGINAL com os dados da anotação
                        original_chunk_dict.update(
                            keep=True,
                            tags=[t for t in annotation_result.tags if t in ALLOWED_TAGS], # Filtrar tags permitidas
                            reason=annotation_result.reason
                        )
                        approved.append(original_chunk_dict) # Adicionar o dicionário original atualizado
                        logger.debug(f"[Agent Run - {current_chunk_index}] Chunk APROVADO: {source_filename_log} index {chunk_index_log}")
                    else:
                         logger.debug(f"[Agent Run - {current_chunk_index}] Chunk REJEITADO: {source_filename_log} index {chunk_index_log}. Razão: {annotation_result.reason}")

                # === CAPTURA ADICIONAL: Especificamente TypeError ===
                except TypeError as te:
                     logger.error(f"[Agent Run - {current_chunk_index}] TypeError capturado durante processamento do chunk {source_filename_log} index {chunk_index_log}: {te}", exc_info=True)
                     # Verificar se a mensagem de erro é a esperada
                     if "unhashable type: 'slice'" in str(te):
                         logger.error(f"[Agent Run - {current_chunk_index}] ERRO CONFIRMADO: 'unhashable type: slice'. Provavelmente causado por metadados inválidos (chunk_index?).")
                     # Continuar para o próximo chunk após logar o TypeError
                     continue
                except Exception as e:
                    # Logar erro geral e continuar para o próximo chunk
                    logger.error(f"[Agent Run - {current_chunk_index}] Exceção geral capturada durante processamento do chunk {source_filename_log} index {chunk_index_log}: {e}", exc_info=True)
                    continue # Garantir que continue para o próximo chunk em caso de erro

        logger.info(f"Processamento de anotação concluído. Total de chunks aprovados: {len(approved)}")
        return approved # Retorna a lista de dicionários dos chunks originais que foram aprovados

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

   