# agents/structure_analyzer_agent.py
import os
import json
import re  # Importar o módulo re
import logging # Adicionar import para logging
from typing import List, Dict, TypedDict, Optional

# Tentar importar json_repair
try:
    from json_repair import repair_json
    JSON_REPAIR_AVAILABLE = True
except ImportError:
    JSON_REPAIR_AVAILABLE = False
    # Definir uma função dummy se json_repair não estiver instalado
    def repair_json(s, return_objects=False): # type: ignore
        # Adicionando type: ignore para o linter não reclamar da redefinição
        # apenas no contexto do fallback da importação.
        logging.warning("json_repair não está instalado. Tentativas de reparo de JSON falharão.")
        raise ImportError("json_repair não está instalado.")

# Configurar o logger para este módulo
logger = logging.getLogger(__name__)

# Tentar importar do CrewAI e Langchain, lidar com possíveis erros de importação
try:
    from crewai import Agent, Task, Crew, Process
    from langchain_openai import ChatOpenAI
    CREWAI_AVAILABLE = True
except ImportError:
    CREWAI_AVAILABLE = False
    # Definir classes dummy se o CrewAI não estiver instalado
    Agent = Task = Crew = Process = object
    ChatOpenAI = object
    print("AVISO: Bibliotecas CrewAI ou Langchain não encontradas. StructureAnalyzerAgent não funcional.")

from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# --- Estrutura de Dados ---
class IdentifiedSection(TypedDict):
    section_type: str
    content: str
    start_char_offset: Optional[int]
    end_char_offset: Optional[int]

# --- Vocabulários ---
EMAIL_SECTION_VOCABULARY = [
    "identificacao_remetente_destinatario_data",
    "saudacao_inicial_email",
    "corpo_principal_email",
    "despedida_assinatura_email",
    "anexos_referencias_email",
    "observacoes_ps_email",
    "confidencialidade_aviso_legal_email",
    "bloco_assinatura_detalhado_contato",
    "respostas_anteriores_thread_email",
    "email_automatico_notificacao",
    "solicitacao_agendamento_email",
    "confirmacao_agendamento_email",
    "lembrete_consulta_email",
    "pesquisa_satisfacao_email",
    "agradecimento_comparecimento_email",
    "convite_evento_webinar_email",
    "newsletter_informativo_email",
    "comunicado_importante_email",
    "erro_entrega_email",
    "email_marketing_promocional",
    "saudacao_personalizada_cliente",
    "introducao_objetivo_email",
    "desenvolvimento_argumentacao_email",
    "chamada_para_acao_email",
    "informacoes_contato_detalhadas_email",
    "resumo_proximos_passos_email",
    "historico_interacoes_cliente_email",
    "feedback_recente_cliente_email",
    "atualizacao_status_projeto_email",
    "assinatura_padrao_empresa_email",
    "componente_visual_logo_banner_email",
    "link_redes_sociais_website_email",
    "pergunta_problema_especifico_email",
    "resposta_solucao_detalhada_email",
    "contexto_discussao_anterior_email",
    "email_cta_principal"
]

# Adicione outros vocabulários aqui conforme necessário
ROTEIRO_VIDEO_YOUTUBE_VOCABULARY = [
    "titulo_video_roteiro",
    "introducao_gancho_roteiro",
    "segmento_1_roteiro",
    "segmento_2_roteiro",
    "segmento_3_roteiro",
    "segmento_transicao_roteiro",
    "chamada_para_acao_roteiro",
    "conclusao_enceramento_roteiro",
    "vinheta_abertura_roteiro",
    "vinheta_encerramento_roteiro",
    "bloco_patrocinio_roteiro",
    "dicas_rapidas_roteiro",
    "recapitulacao_pontos_chave_roteiro",
    "agradecimentos_creditos_roteiro",
    "teaser_proximo_video_roteiro"
]

# Adicionar o novo vocabulário para transcrições de vídeo
YOUTUBE_TRANSCRIPTION_SECTION_VOCABULARY = [
    "introducao_apresentador",
    "fala_principal",
    "segmento_perguntas_respostas",
    "demonstracao_tela",
    "conclusao_resumo",
    "chamada_para_acao",
    "vinheta_encerramento"
]

# Novos vocabulários adicionados

CURSO_PDC_ESPECIALIDADES_VOCABULARY = [
    "titulo_modulo_aula_especialidades",
    "apresentacao_instrutor_especialidades",
    "introducao_tema_especialidade",
    "definicao_conceito_chave_especialidades",
    "epidemiologia_relevancia_especialidades",
    "fisiopatologia_mecanismo_especialidades",
    "quadro_clinico_sintomatologia_especialidades",
    "diagnostico_diferencial_exames_especialidades",
    "tratamento_manejo_farmacologico_especialidades",
    "tratamento_manejo_nao_farmacologico_especialidades",
    "prevencao_orientacoes_especialidades",
    "prognostico_evolucao_especialidades",
    "estudo_de_caso_clinico_especialidades",
    "discussao_artigo_cientifico_especialidades",
    "conclusao_resumo_aula_especialidades"
]

CONTEUDO_SOCIAL_VOCABULARY_SIMPLIFIED = [
    "identificacao_post_campanha_social",
    "legenda_post_social",
    "hashtags_relacionadas_social",
    "chamada_para_acao_social"
]

VIDEO_YOUTUBE_METADATA_VOCABULARY = [ # Para descrições/metadados de vídeos
    "titulo_video_metadata", # Adicionado sufixo para evitar colisão de nome se houvesse outra constante "titulo_video"
    "descricao_sumario_video_metadata",
    "palavras_chave_tags_video_metadata",
    "informacao_canal_autor_metadata",
    "links_relacionados_na_descricao_metadata"
]

CURSO_URGENCIAS_PDC_VOCABULARY = [
    "tema_aula_urgencias",
    "apresentacao_caso_urgencia",
    "avaliacao_inicial_triagem_urgencias",
    "protocolo_atendimento_sbv_savp_urgencias", # Suporte Básico/Avançado de Vida Pediátrico
    "diagnostico_rapido_urgencias",
    "intervencao_imediata_urgencias",
    "medicacoes_de_emergencia_urgencias",
    "equipamentos_materiais_urgencias",
    "comunicacao_equipe_urgencias",
    "transporte_paciente_critico_urgencias",
    "manejo_pos_parada_urgencias", # PCR = Parada Cardiorrespiratória
    "simulacao_pratica_urgencias",
    "debriefing_discussao_caso_urgencias",
    "aspectos_eticos_legais_urgencias",
    "conclusao_pontos_aprendizado_urgencias"
]

PROVA_SOCIAL_VOCABULARY_SIMPLIFIED = [
    "tipo_prova_social", # Ex: "Depoimento Aluno", "Instrução Edição Vídeo"
    "conteudo_principal_prova_social",
    "identificacao_autor_prova_social"
]

MATERIAL_AULA_CURSO_VOCABULARY = [
    "titulo_autor_material_aula",
    "introducao_objetivos_material_aula",
    "conceito_definicao_tecnica_material",
    "classificacao_tipos_variacoes_material",
    "discussao_pratica_consultorio_material",
    "perguntas_respostas_frequentes_material",
    "revisao_conteudo_previo_link_material",
    "recomendacao_conduta_orientacao_material",
    "material_complementar_referencia_material",
    "resumo_pontos_chave_material_aula"
]

MANUAL_ATUALIZACAO_PED_VOCABULARY = [
    "tema_topico_atualizacao_manual",
    "recomendacao_diretriz_oficial_manual",
    "ponto_chave_lista_item_manual",
    "alerta_risco_contraindicacao_manual",
    "indicacao_uso_produto_substancia_manual",
    "procedimento_exame_recomendado_manual",
    "faixa_etaria_especifica_orientacao_manual",
    "quantidade_dosagem_referencia_manual",
    "referencia_imagem_diagrama_manual"
]

ANUNCIO_COPY_VOCABULARY_SIMPLIFIED = [
    "identificacao_anuncio_campanha_copy",
    "publico_alvo_direcionamento_copy",
    "corpo_principal_anuncio_copy",
    "chamada_para_acao_cta_copy",
    "legenda_para_anuncio_copy"
]

CURSO_PEDCLASS_VOCABULARY = [
    "apresentacao_palestrante_experiencia_pedclass",
    "introducao_tema_relevancia_clinica_pedclass",
    "discussao_fisiopatologia_conceitos_pedclass",
    "abordagem_diagnostica_sinais_alerta_pedclass",
    "manejo_terapeutico_nao_farmacologico_pedclass",
    "manejo_terapeutico_farmacologico_pedclass",
    "discussao_desafios_pratica_clinica_pedclass",
    "comunicacao_acolhimento_familia_pedclass",
    "experiencia_pessoal_caso_ilustrativo_pedclass",
    "referencia_material_externo_curso_pedclass",
    "sessao_perguntas_respostas_interacao_pedclass",
    "mensagem_final_reflexao_aula_pedclass"
]

PROTOCOLO_SECRETARIA_VOCABULARY = [
    "objetivo_treinamento_secretaria_protocolo",
    "papel_secretaria_vendedora_protocolo",
    "jornada_paciente_pre_consulta_protocolo",
    "jornada_paciente_pos_consulta_protocolo",
    "desafios_comuns_secretariado_protocolo",
    "habilidade_essencial_secretaria_protocolo",
    "processo_onboarding_treinamento_secretaria_protocolo",
    "ferramenta_gestao_consultorio_secretaria_protocolo",
    "indicador_desempenho_nps_secretaria_protocolo",
    "comunicacao_script_atendimento_secretaria_protocolo",
    "gestao_conflitos_paciente_secretaria_protocolo",
    "estrategia_fidelizacao_paciente_secretaria_protocolo"
]

# Novo vocabulário para material_lancamento
MATERIAL_LANCAMENTO_SECTION_VOCABULARY = [
    "material_lancamento_abertura_evento_boas_vindas",
    "material_lancamento_introducao_palestrante_tema",
    "material_lancamento_apresentacao_conteudo_principal",
    "material_lancamento_discussao_interativa_chat_perguntas",
    "material_lancamento_demonstracao_produto_servico",
    "material_lancamento_estudo_caso_depoimento",
    "material_lancamento_oferta_produto_detalhes_preco",
    "material_lancamento_bonus_garantias",
    "material_lancamento_chamada_para_acao_instrucoes_compra",
    "material_lancamento_esclarecimento_duvidas_finais_faq",
    "material_lancamento_encerramento_evento_agradecimentos",
    "material_lancamento_copy_anuncio_curto",
    "material_lancamento_lista_links_recursos",
    "material_lancamento_corpo_geral_fallback" # Fallback para material_lancamento
]

# Vocabulário para ACP (Acelerador de Consultório Pediátrico)
ACP_SECTION_VOCABULARY = [
    "acp_boas_vindas_introducao_curso",
    "acp_apresentacao_modulo_aula",
    "acp_conceito_estrategia_marketing",
    "acp_ferramenta_plataforma_marketing",
    "acp_conceito_gestao_consultorio",
    "acp_personal_branding_posicionamento",
    "acp_tecnicas_otimizacao_consulta",
    "acp_instrucoes_tarefas_planner",
    "acp_recapitulacao_proximos_passos",
    "acp_encerramento_aula_modulo",
    "acp_corpo_geral_aula_fallback" # Fallback para acp
]

# Vocabulário para EVENTO_SIPCON (Simpósio)
EVENTO_SIPCON_SECTION_VOCABULARY = [
    "sipcon_abertura_evento_boas_vindas",
    "sipcon_apresentacao_palestrante_tema",
    "sipcon_introducao_conceito_chave",
    "sipcon_desenvolvimento_topico_principal",
    "sipcon_exemplos_praticos_estudos_caso",
    "sipcon_discussao_interacao_publico_perguntas",
    "sipcon_ferramentas_recursos_recomendados",
    "sipcon_conclusao_palestra_resumo",
    "sipcon_chamada_para_acao_proximos_passos",
    "sipcon_encerramento_evento_agradecimentos",
    "sipcon_conteudo_geral_palestra_fallback" # Fallback para evento_sipcon
]

# Vocabulário para PDC_NOTES
PDC_NOTES_SECTION_VOCABULARY = [
    "pdc_notes_tema_calendario_saude",
    "pdc_notes_guia_orientacao_pais",
    "pdc_notes_protocolo_clinico_resumo",
    "pdc_notes_planejamento_conteudo_roteiro",
    "pdc_notes_checklist_procedimento",
    "pdc_notes_geral_informativo_fallback" # Fallback para pdc_notes
]

SECTION_VOCABULARIES: Dict[str, List[str]] = {
    "email": EMAIL_SECTION_VOCABULARY,
    "roteiro_video_youtube": ROTEIRO_VIDEO_YOUTUBE_VOCABULARY,
    "youtube_video_transcription": YOUTUBE_TRANSCRIPTION_SECTION_VOCABULARY,
    "curso_pdc_especialidades": CURSO_PDC_ESPECIALIDADES_VOCABULARY,
    "conteudo_social": CONTEUDO_SOCIAL_VOCABULARY_SIMPLIFIED,
    "video_youtube": VIDEO_YOUTUBE_METADATA_VOCABULARY,
    "curso_urgencias_pdc": CURSO_URGENCIAS_PDC_VOCABULARY,
    "prova_social": PROVA_SOCIAL_VOCABULARY_SIMPLIFIED,
    "material_aula_curso": MATERIAL_AULA_CURSO_VOCABULARY,
    "manual_atualizacao_ped": MANUAL_ATUALIZACAO_PED_VOCABULARY,
    "anuncio_copy": ANUNCIO_COPY_VOCABULARY_SIMPLIFIED,
    "curso_pedclass": CURSO_PEDCLASS_VOCABULARY,
    "protocolo_secretaria": PROTOCOLO_SECRETARIA_VOCABULARY,
    "material_lancamento": MATERIAL_LANCAMENTO_SECTION_VOCABULARY,
    "acp": ACP_SECTION_VOCABULARY, # Novo
    "evento_sipcon": EVENTO_SIPCON_SECTION_VOCABULARY, # Novo
    "pdc_notes": PDC_NOTES_SECTION_VOCABULARY # Novo
}

DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX = "_corpo_geral_fallback"

# --- Classe do Agente ---
class StructureAnalyzerAgent:
    def __init__(self, llm_provider: Optional[ChatOpenAI] = None):
        if not CREWAI_AVAILABLE:
             logger.error("Bibliotecas CrewAI ou Langchain não estão instaladas. Agente não pode ser inicializado.")
             raise ImportError("Bibliotecas CrewAI ou Langchain não estão instaladas. Agente não pode ser inicializado.")

        if llm_provider:
            self.llm = llm_provider
        else:
            # Configurar o LLM Padrão
            self.llm = ChatOpenAI(
                model=os.getenv("OPENAI_MODEL_NAME", "gpt-4o-mini"),
                temperature=0.2,
                openai_api_key=os.getenv("OPENAI_API_KEY")
            )
        
        if not os.getenv("OPENAI_API_KEY"):
            logger.warning("OPENAI_API_KEY não definida. O agente pode não funcionar corretamente.")

        # Configurar o Agente CrewAI
        self.agent = Agent(
            role="Analista Especialista em Estrutura de Documentos",
            goal=(
                "Segmentar o conteúdo do documento em seções funcionais coesas "
                "com base em um vocabulário predefinido e retornar os resultados em formato JSON."
            ),
            backstory=(
                "Você é um analista de IA altamente treinado com vasta experiência em entender a estrutura semântica "
                "de diversos tipos de documentos, como emails, artigos e roteiros. Sua precisão em identificar seções "
                "e formatar a saída em JSON é impecável."
            ),
            llm=self.llm,
            verbose=True, # Pode ser ajustado para False em produção
            allow_delegation=False
        )

    def _get_prompt_template(self) -> str:
        """Retorna a string de modelo do prompt."""
        # O prompt agora é uma string de modelo com placeholders para CrewAI
        # Inclui as instruções refinadas para segmentação mais coesa
        prompt_template = """[ROLE]
Você é um Analista Especialista em Estrutura de Documentos. Você é um analista de IA altamente treinado com vasta experiência em entender a estrutura semântica de diversos tipos de documentos, como emails, artigos e roteiros. Sua precisão em identificar seções e formatar a saída em JSON é impecável.

[GOAL]
Segmentar o conteúdo do documento em seções funcionais **coesas** com base em um vocabulário predefinido e retornar os resultados em formato JSON.

[CONTEXT]
Tipo do Documento (source_type): {source_type}
Vocabulário de Seções Esperadas para '{source_type}' (use estes rótulos exatos para 'section_type'):
{vocabulary_json}

Texto Completo do Documento:
---
{document_content}
---

[INSTRUCTIONS]
1. Leia TODO o texto do documento cuidadosamente.
2. Identifique blocos de texto (que podem abranger um ou múltiplos parágrafos) que correspondem semanticamente a cada uma das seções esperadas no vocabulário fornecido.
3. **Agrupe parágrafos consecutivos que pertençam claramente à mesma seção funcional** (ex: múltiplos parágrafos descrevendo benefícios devem ser agrupados sob um único '{source_type}_beneficios').
4. **OBJETIVO CRÍTICO PARA RAG: Seu objetivo principal é agrupar o texto em blocos funcionais semanticamente coesos e SUBSTANCIAIS (ideais para busca e recuperação - RAG).** Chunks muito pequenos não fornecem contexto suficiente. **Prefira ter MENOS seções (idealmente entre 5 a 8 para todo o corpo do email), porém mais COMPLETAS e LONGAS**, em vez de muitas seções curtas e fragmentadas.
5. **EVITE SEÇÕES CURTAS (MENOS DE ~300 caracteres):** Não crie seções muito pequenas (ex: uma única frase curta ou parágrafo), a menos que sejam inequivocamente seções estruturais como 'saudacao', 'assinatura', ou 'ps', que são inerentemente curtas. **Se uma seção funcional identificada for curta, agrupe-a com a seção vizinha mais apropriada semanticamente (preferencialmente a seguinte)** para formar um chunk maior e mais útil.
6. Tente atribuir a maior parte do texto a uma das seções do vocabulário fornecido. Se um bloco de texto não se encaixar claramente em nenhuma seção específica, classifique-o como '{source_type}{DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX}'.
7. Mantenha a ordem original das seções conforme aparecem no texto. Não omita nenhuma parte significativa do texto original.
8. Retorne o resultado EXCLUSIVAMENTE no formato JSON, como uma lista de objetos. Cada objeto DEVE representar uma seção identificada e conter EXATAMENTE duas chaves:
   - 'section_type': uma string contendo o rótulo exato do vocabulário fornecido (ou '{source_type}{DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX}').
   - 'content': uma string contendo o texto EXATO e completo da seção identificada.
9. NÃO inclua nenhuma explicação, introdução, resumo ou qualquer texto adicional antes ou depois da lista JSON. Sua resposta deve começar com '[' e terminar com ']'.

[OUTPUT EXAMPLE (for a hypothetical email)]
[
  {{ "section_type": "email_saudacao", "content": "Olá Guilherme," }},
  {{ "section_type": "email_gancho_problema", "content": "Você já se sentiu perdido tentando organizar seus dados?\\nNós entendemos a sua dor. Muitos profissionais enfrentam desafios similares..." }},
  {{ "section_type": "email_apresentacao_oferta", "content": "É por isso que criamos o Organizador Supremo! Uma ferramenta revolucionária..." }},
  {{ "section_type": "email_beneficios", "content": "- Organiza tudo\\n- Economiza tempo\\n- Traz paz de espírito\\nImagine como seria ter tudo sob controle." }},
  {{ "section_type": "email_cta_principal", "content": "Clique aqui para saber mais e garantir o seu! Não perca tempo." }},
  {{ "section_type": "email_assinatura", "content": "Atenciosamente,\\nSua Equipe de Sucesso\\nwww.exemplo.com" }},
  {{ "section_type": "email_ps", "content": "P.S. A oferta termina em 24 horas!" }}
]
"""
        return prompt_template

    def _parse_llm_response_to_json(self, llm_response_text: str) -> List[IdentifiedSection]:
        """
        Tenta parsear a resposta do LLM para JSON, com limpeza e fallback para json_repair.
        """
        if not llm_response_text:
            logger.warning("Resposta do LLM está vazia.")
            return []

        clean_text = llm_response_text.strip()
        if clean_text.startswith("```json"):
            clean_text = clean_text[7:]
        if clean_text.startswith("```"):
            clean_text = clean_text[3:]
        if clean_text.endswith("```"):
            clean_text = clean_text[:-3]
        clean_text = clean_text.strip()

        try:
            logger.debug(f"Tentando json.loads() em texto limpo (repr): {repr(clean_text)}")
            parsed_json = json.loads(clean_text)
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON com json.loads(): {e}")
            logger.error(f"String que falhou no parse (repr): {repr(clean_text)}")
            logger.error(f"String que falhou no parse (len): {len(clean_text)}")
            
            if JSON_REPAIR_AVAILABLE:
                logger.info("Tentando reparar JSON com json_repair...")
                try:
                    repaired_json_obj = repair_json(clean_text, return_objects=True)
                    logger.info(f"json_repair retornou objeto do tipo: {type(repaired_json_obj)}")
                    logger.debug(f"Conteúdo reparado (repr): {repr(repaired_json_obj)}")
                    parsed_json = repaired_json_obj # Tentar usar o objeto reparado
                except Exception as repair_e:
                    logger.error(f"Falha EXCEPCIONAL ao tentar reparar JSON com json_repair: {repair_e}", exc_info=True)
                    return [] # Retorna lista vazia se o reparo também falhar
            else:
                logger.warning("json_repair não está disponível. Não é possível tentar o reparo.")
                return []
        except Exception as e: # Outros erros inesperados durante json.loads
             logger.error(f"Erro inesperado durante json.loads() inicial: {e}", exc_info=True)
             logger.error(f"Texto que causou o erro (repr): {repr(clean_text)}")
             return []


        # Validar a estrutura do JSON parseado (seja original ou reparado)
        if not isinstance(parsed_json, list):
            logger.error(f"JSON parseado (ou reparado) não é uma lista. Tipo: {type(parsed_json)}. Conteúdo (repr): {repr(parsed_json)}")
            return []

        validated_sections: List[IdentifiedSection] = []
        for i, item in enumerate(parsed_json):
            if not isinstance(item, dict):
                logger.warning(f"Item {i} na lista JSON não é um dicionário: {repr(item)}")
                continue 
            if 'section_type' not in item or 'content' not in item:
                logger.warning(f"Item {i} na lista JSON não tem 'section_type' ou 'content': {item}")
                continue
            
            validated_sections.append({
                'section_type': str(item['section_type']),
                'content': str(item['content']),
                'start_char_offset': item.get('start_char_offset'),
                'end_char_offset': item.get('end_char_offset')
            })
        
        if not validated_sections and parsed_json: # Se houve itens mas nenhum passou na validação
             logger.warning(f"Nenhum item no JSON parseado/reparado passou na validação de estrutura. JSON original (repr): {repr(parsed_json)}")
        elif not validated_sections and not parsed_json: # Se o JSON estava vazio desde o início ou após reparo
             logger.info("JSON parseado/reparado resultou em uma lista vazia de seções validadas.")


        return validated_sections

    def analyze_structure(self, document_content: str, source_type: str) -> List[IdentifiedSection]:
        """
        Analisa a estrutura de um documento usando o LLM e retorna uma lista de seções identificadas.
        """
        vocabulary = SECTION_VOCABULARIES.get(source_type)
        fallback_section_type = f'{source_type}{DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX}'

        if not vocabulary:
            logger.warning(f"Vocabulário não definido para source_type '{source_type}'. Retornando fallback.")
            return [{
                'section_type': fallback_section_type,
                'content': document_content,
                'start_char_offset': 0,
                'end_char_offset': len(document_content)
            }]

        vocabulary_json_str = json.dumps(vocabulary, indent=2)
        prompt_template = self._get_prompt_template()

        task = Task(
            description=prompt_template,
            expected_output=(
                f"Uma lista JSON de seções identificadas (com base no vocabulário para {source_type}). "
                "Cada seção deve ter 'section_type' e 'content'. "
                "A resposta DEVE ser APENAS o JSON, começando com '[' e terminando com ']'."
            ),
            agent=self.agent
        )

        crew = Crew(
            agents=[self.agent],
            tasks=[task],
            process=Process.sequential,
            verbose=0 # Ajuste conforme necessário (0=silencioso, 1=básico, 2=detalhado para CrewAI)
        )

        inputs = {
            'document_content': document_content,
            'source_type': source_type,
            'vocabulary_json': vocabulary_json_str,
            'DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX': DEFAULT_FALLBACK_SECTION_TYPE_SUFFIX
        }

        logger.info(f"Analisando estrutura para source_type: '{source_type}'...")
        result_string = None
        try:
            result_object = crew.kickoff(inputs=inputs)
            result_string = str(result_object) 
            logger.debug(f"LLM Result String (raw):\n{result_string}")
        except Exception as e:
            logger.error(f"Erro durante a execução do Crew kickoff: {e}", exc_info=True)
            return []

        # Usar o método _parse_llm_response_to_json refatorado
        parsed_sections = self._parse_llm_response_to_json(result_string)
        
        if not parsed_sections:
             logger.warning(f"Nenhuma seção foi parseada com sucesso para source_type '{source_type}'. Verificar logs anteriores para detalhes sobre o parse do JSON.")

        return parsed_sections

# --- Bloco de Teste com Múltiplos Emails ---
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(module)s] - %(message)s')
    if not CREWAI_AVAILABLE:
        logger.info("Pulando teste pois CrewAI/Langchain não estão disponíveis.")
    else:
        # --- Definir Textos dos Emails Reais ---

        email_oferta_27_08 = """Assunto: O Poder da Comunidade!

Pré-cabeçalho: Dá uma olhada 👀 

Que INGRESSO foi esse, meu Brasil?!

%FIRSTNAME%, se você participou do INGRESSO 2024 no último sábado, você viu o poder da Comunidade.

[INSERIR FOTO DO INGRESSO 2024 EM GRUPO]

Você teve acesso a um conteúdo que nunca falaram em nenhum outro evento da forma como apresentamos.

E se você não participou, dá só uma olhada no que falaram: 

https://prnt.sc/ENKtKPvTHPsu 

https://prnt.sc/mM-Qe35vlEDb 

"Você é moldada pelo seu ambiente."

É exatamente esse o PODER da COMUNIDADE.

Foram dezenas e dezenas de pediatras reunidas em um só lugar para aprender sobre puericultura, gestão, marketing e se unir como comunidade.

Tudo isso foi realmente MUITO incrível!

E nós pensamos desde o início:

"Como podemos tornar isso ainda maior e mais impactante?"

Pensando nisso, tomamos uma decisão…

O INGRESSO 2025 está CHEGANDO!

Acontecerá no dia 23 de agosto de 2025.

E liberamos hoje uma oportunidade exclusiva para você garantir a sua vaga na próxima edição com o preço de PRÉ-VENDA, o melhor investimento possível! 

Para ter acesso a esse lote especial em primeira mão, basta tocar agora no botão abaixo e garantir sua vaga:ingresso_2025

[QUERO PARTICIPAR DO INGRESSO 2025]

Já adiantamos…

Tudo o que virá no próximo ano fará você realmente feliz por ter feito sua inscrição antes nesse lote e aproveitado essa oportunidade.

Então já toca no botão acima e garante a sua vaga.

Beijos,
Gabi
"""

        email_newsletter_estreia = """Assunto: Estreia e nova fase

—-


Oiie, tudo bem?

Gabi e Julie aqui!

Seguinte, esse email é mais do que especial!

É uma ESTREIA de um novo conteúdo exclusivo - realmente especial - que vamos dar início aqui no PDC.

E sim… você já é convidada(o) VIP por já estar aqui na nossa lista de emails.

"Ah, mas por quê isso?"

A verdade é que decidimos criar um conteúdo realmente INÉDITO para quem deseja se aprofundar mais no consultório e quer fazer DAR CERTO.

Eu estou falando de você ter um CONSULTÓRIO PARTICULAR de pediatria…

Atender as famílias da melhor forma possível…

E fazer uma consulta DIFERENCIADA!!

Sabe aquelas que enche o coração de orgulho do trabalho e faz a mãe enviar mensagem de agradecimento depois? 🥺

É isso que vamos te ajudar a fazer aqui!

Hoje você vai dar início à captação de pacientes realmente EFICAZ, que funciona.

Isso vai te pôr anos luz à frente de muitos pediatras.

Primeira coisa…

Torne-se atraente no Instagram

Fato: as mães estão cada vez mais no Instagram, e se elas estão lá, você também precisa estar.

Independente de você ser residente ou já estar formada, você precisa ter um perfil interessante.

Como fazer isso?

Faça a sua BIO ser relevante para seu público!

Você busca atingir mães que querem ter cuidados constantes sobre o desenvolvimento do filho, então…

Pontos interessantes para colocar na sua BIO: autoridade, o que você faz, seu CFM, e um CTA chamando para marcar a sua consulta.

Exemplo: 

Pediatra e Neonatologista (autoridade)
Ajudo crianças e adolescentes a terem o desenvolvimento mais seguro e saudável possível
CFM
Agende a sua consulta 👇🏻

Produza conteúdo nos melhores formatos

Muita gente fala de VÁRIAS formas de produzir conteúdo, né?

Bom, conteúdo nada mais é do que informação transmitida de forma interessante e relevante para as pessoas.

Logo, se você deseja atingir o público de mães, faça REELS e CARROSSÉIS que conversem com elas.

"Mãe, seu bebê não dorme a noite de jeito nenhum?"

"Quando e qual o melhor protetor solar a ser usado na criança?"

E várias outras dúvidas que tiverem, todas podem virar conteúdo.

"Eu preciso aparecer?"

Olha, vai te ajudar sim, viu. 

Mas é totalmente possível você ir desenvolvendo seu perfil com mais textos.

Só que olha com carinho para os reels e aparece nos stories também…

Porque isso aumenta a conexão com o público, transmite confiança e vai te aproximando das mães.

Combinado?

Existem vááárias outras estratégias de marketing para você impulsionar cada vez mais a sua captação de pacientes no consultório…

E vamos falar sobre isso nos próximos emails.

VAI SER "SÓ" SOBRE ISSO?

Não!

Aqui vamos abordar sobre conhecimento (puericultura), marketing, gestão E TAMBÉM VIDA PESSOAL! 🤪

Sim, somos filhas de Deus também, temos perrengues como qualquer pessoa, mas também temos várias bençãos que hoje são proporcionadas através do consultório particular.

Então, o que podemos dizer, é: fique atenta(o) no próximo email, que vai vir na semana que vem.

Porque vamos te mostrar mais dos bastidores do consultório.

Me diz: gostou desse tipo de conteúdo? Se sim, responde esse email, vamos adorar saber!! 🥰

Beijos,
Gabi e Julie
"""

        email_aquecimento_01_11 = """Assunto: Segure o Limite do seu cartão até o dia 09/11

Pré-cabeçalho: 👀 

Oiie, %FIRSTNAME%!

Aqui é a Julie.

Eu estou passando por aqui porque tenho algo MUITO importante pra te contar… 

Se eu fosse você, seguraria o limite do cartão até o dia 09/11. 

Isso mesmo!

No Primeiro Simpósio de Pediatria de Consultório, estamos preparando a MAIOR OFERTA de Black Friday que já fizemos no PDC! 

E acredite, você não vai querer perder essa chance.

O que vem aí? 

Será uma oferta completa, pensada para você que quer finalmente viver só do consultório, sem precisar se desdobrar em plantões e convênios. 

É a oportunidade de ter acesso a tudo o que realmente importa para consolidar seu consultório, atrair mais pacientes e se tornar uma referência na sua cidade.

Essa oferta vai incluir tudo o que você precisa para transformar sua realidade no consultório…

Incluindo um SUPER BÔNUS exclusivo para quem estiver conosco, ao vivo, no dia 09 de novembro, no SIPCON!

E olha…

São centenas e centenas de pediatras já confirmados nesse Simpósio, e eu garanto que você vai querer ser uma das primeiras pessoas a se inscrever. 👀

Então, %FIRSTNAME%, marque o dia 09/11 no calendário e segure o limite do seu cartão! 

Você está prestes a receber uma oportunidade única.

Nos vemos no Simpósio!

Julie do PDC
"""

        email_convite_31_10 = """Assunto: Você viu esse Resumo do CONGRESSO?

Pré-cabeçalho: Foi enviado no grupo… 

%FIRSTNAME%,

Na segunda-feira nós liberamos um CONTEÚDO INÉDITO sobre o Congresso Brasileiro de Pediatria.

E o pessoal achou INCRÍVEL!

[IMAGEM]
Print email .png 

São centenas e centenas de pessoas participando do Primeiro Simpósio de Pediatria de Consultório.

E todas elas vão receber acesso ao link para participar de todas as palestras de puericultura, gestão e marketing que acontecerão no dia 09 de novembro!

Além disso, também vão poder se inscrever na Black Friday do PDC com a MAIOR OFERTA DO ANO!

Vai mesmo só você ficar de fora e perder acesso a tudo isso?

Para se inscrever, é simples…

Toque agora no botão abaixo e garanta a sua vaga:

https://chat.whatsapp.com/GDmhHUgp9EPFkAdJr9qbtC 

Nos vemos lá dentro!

Beijos,
Julie 
"""
        # --- Lista de Emails para Teste ---
        test_emails = [
            {"name": "Email 1 - Oferta - 27/08", "content": email_oferta_27_08},
            {"name": "Email 2 - Newsletter - Estreia", "content": email_newsletter_estreia},
            {"name": "Email 3 - Aquecimento - 01/11", "content": email_aquecimento_01_11},
            {"name": "Email 4 - Convite Leads Antigos - 31/10", "content": email_convite_31_10},
        ]

        analyzer_agent = StructureAnalyzerAgent()

        # --- Loop para Analisar Cada Email ---
        for email_data in test_emails:
            logger.info(f"\n{'='*20} Analisando Email: {email_data['name']} {'='*20}")
            email_sections = analyzer_agent.analyze_structure(email_data['content'], "email")

            if email_sections:
                logger.info("\n--- Seções Identificadas ---")
                for i, section in enumerate(email_sections):
                    logger.info(f"Seção {i+1}:")
                    logger.info(f"  Tipo: {section['section_type']}")
                    content_preview = section['content'].replace('\\n', ' ').strip()
                    logger.info(f"  Conteúdo: {content_preview[:100]}...")
                    logger.info("---")
            else:
                logger.warning("Nenhuma seção foi identificada ou ocorreu um erro.")
            logger.info(f"{'='*20} Fim da Análise: {email_data['name']} {'='*20}")

        # --- Teste de Fallback (Mantido) ---
        logger.info("\n--- Testando Fallback para Tipo Desconhecido ---")
        unknown_content = "Este é um documento de um tipo não mapeado, com várias frases."
        unknown_sections = analyzer_agent.analyze_structure(unknown_content, "tipo_desconhecido")
        if unknown_sections:
             logger.info("\n--- Seções Identificadas (Fallback) ---")
             for section in unknown_sections:
                logger.info(f"Tipo: {section['section_type']}")
                content_preview = section['content'].replace('\\n', ' ').strip()
                logger.info(f"Conteúdo: {content_preview[:100]}...")
                logger.info("---")