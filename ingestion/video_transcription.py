# ingestion/video_transcription.py
"""
Módulo responsável pela transcrição de arquivos de vídeo.

Utiliza a API da AssemblyAI como método principal e o WhisperX (rodando localmente)
como fallback caso a AssemblyAI falhe ou não esteja configurada.

Funções Principais:
- `transcribe_video_assemblyai`: Tenta transcrever usando AssemblyAI.
- `transcribe_video_whisperx`: Tenta transcrever usando WhisperX (inclui fallback
  para extrair áudio com ffmpeg se necessário).
- `process_video`: Orquestra a tentativa de transcrição, usando AssemblyAI primeiro
  e depois WhisperX.
- `process_all_videos_in_directory`: Itera sobre um diretório, identifica arquivos
  de vídeo e chama `process_video` para cada um.

Requer configuração via variáveis de ambiente (`ASSEMBLYAI_API_KEY`)
 e instalação de dependências (`assemblyai`, `whisperx`, `torch`, `ffmpeg`).
"""

import os
import logging
import time
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List
import assemblyai as aai
import whisperx
import torch
import argparse
import subprocess
import shutil
import tempfile

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente (especialmente ASSEMBLYAI_API_KEY)
load_dotenv()

ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")
if not ASSEMBLYAI_API_KEY:
     logger.warning("ASSEMBLYAI_API_KEY não encontrada no .env. Transcrição AssemblyAI desabilitada.")
else:
    try:
        aai.settings.api_key = ASSEMBLYAI_API_KEY
        logger.info("AssemblyAI API Key configurada.")
    except Exception as e:
        logger.error(f"Erro ao configurar AssemblyAI API Key: {e}")
        ASSEMBLYAI_API_KEY = None # Desabilitar se a chave for inválida

# TODO: Confirmar/parametrizar o caminho dos vídeos
# VIDEO_SOURCE_PATH = "/PDC Content/videos"
# Sugestão: Usar um caminho relativo como "data/videos" ou passar como argumento

# Configuração WhisperX (pode ser ajustada)
WHISPER_MODEL = "base" # ou "medium", "large-v2", etc.
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "float16" if torch.cuda.is_available() else "int8"
BATCH_SIZE_WHISPER = 16 # Ajustar conforme memória da GPU/CPU

def transcribe_video_assemblyai(video_path: str) -> Optional[Dict[str, Any]]:
    """Tenta transcrever um vídeo usando a API da AssemblyAI.

    Requer que a variável de ambiente `ASSEMBLYAI_API_KEY` esteja configurada.

    Args:
        video_path (str): O caminho para o arquivo de vídeo local.

    Returns:
        Optional[Dict[str, Any]]: Um dicionário com o texto transcrito e metadados
                                  se a transcrição for bem-sucedida, None caso contrário.
                                  O dicionário retornado tem o formato:
                                  `{"text": "...", "metadata": {"transcriber": "assemblyai", ...}}`
    """
    logger.info(f"Tentando transcrição via AssemblyAI para: {video_path}")
    start_time = time.time()
    if not ASSEMBLYAI_API_KEY:
        # logger.error("ASSEMBLYAI_API_KEY não configurada.") # Já logamos aviso na inicialização
        return None

    try:
        transcriber = aai.Transcriber()
        # --- Configuração explícita (voltando ao modelo padrão) ---
        config = aai.TranscriptionConfig(
            language_code="pt",  # Especificar Português
            punctuate=True,      # Habilitar pontuação automática
            format_text=True     # Habilitar formatação de texto (números, etc.)
            # speech_model="slam-1" # Comentado/Removido
            # Adicionar outros parâmetros se desejado, e.g., speaker_labels=True
        )
        logger.info(f"Usando TranscriptionConfig: language_code='pt', punctuate=True, format_text=True") # Log sem slam-1
        transcript = transcriber.transcribe(video_path, config=config) # Passar a config
        # --- Fim Modificação ---

        if transcript.status == aai.TranscriptStatus.error:
            logger.error(f"Falha na transcrição AssemblyAI: {transcript.error}")
            return None
        elif transcript.status == aai.TranscriptStatus.completed:
            # Extrair texto e talvez outros metadados relevantes (timestamps, speakers, etc.)
            # A estrutura exata de 'metadata' pode variar
            end_time = time.time()
            logger.info(f"Transcrição AssemblyAI bem-sucedida para: {video_path} (Duração: {end_time - start_time:.2f}s)")
            result_data = {
                "text": transcript.text,
                "metadata": {
                    "transcriber": "assemblyai",
                    "confidence": transcript.confidence,
                    "audio_duration": transcript.audio_duration,
                    # Adicione mais metadados se necessário, ex: transcript.utterances
                }
            }
            return result_data
        else:
            # Status pode ser queued, processing, etc. Precisaria de polling ou webhook para produção.
            # Para este script simples, vamos tratar outros status como falha por enquanto.
            logger.warning(f"Status inesperado da transcrição AssemblyAI: {transcript.status} para {video_path}")
            return None

    except Exception as e:
        logger.error(f"Erro durante a chamada da API AssemblyAI para {video_path}: {e}", exc_info=True)
        return None

def transcribe_video_whisperx(video_path: str) -> Optional[Dict[str, Any]]:
    """
    Tenta transcrever um vídeo localmente usando a biblioteca WhisperX.

    Utiliza o modelo Whisper configurado (WHISPER_MODEL) e o dispositivo
    (DEVICE, cpu ou cuda). Tenta carregar o áudio diretamente do vídeo;
    se falhar, tenta extrair o áudio para um arquivo WAV temporário usando
    `ffmpeg` (se disponível no PATH) e então carregar o WAV.

    Args:
        video_path (str): O caminho para o arquivo de vídeo local.

    Returns:
        Optional[Dict[str, Any]]: Um dicionário com o texto transcrito e metadados
                                  se a transcrição for bem-sucedida, None caso contrário.
                                  O dicionário retornado tem o formato:
                                  `{"text": "...", "metadata": {"transcriber": "whisperx", ...}}`
    """
    logger.info(f"Tentando transcrição via WhisperX (Modelo: {WHISPER_MODEL}, Device: {DEVICE}) para: {video_path}")
    start_time = time.time()
    audio = None # Inicializa a variável de áudio

    # --- Tentativa 1: Carregar áudio diretamente ---
    try:
        logger.debug(f"Tentando carregar áudio diretamente de: {video_path}")
        audio = whisperx.load_audio(video_path)
        logger.info(f"Áudio carregado diretamente de: {video_path}")
    except Exception as audio_err:
        logger.warning(f"Falha ao carregar áudio diretamente de {video_path}: {audio_err}. Verificando fallback com ffmpeg...")

        # --- Tentativa 2: Fallback com ffmpeg ---
        ffmpeg_path = shutil.which('ffmpeg')
        if not ffmpeg_path:
            logger.error("ffmpeg não encontrado no PATH. Não é possível extrair áudio para fallback. Verifique a instalação do ffmpeg.")
            # Retorna None aqui pois a falha primária já ocorreu e o fallback não é possível.
            return None
        else:
            logger.info(f"ffmpeg encontrado em: {ffmpeg_path}. Tentando extrair áudio...")
            temp_audio_path = None
            try:
                # Criar arquivo temporário para o áudio extraído
                with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_f:
                    temp_audio_path = temp_f.name
                logger.info(f"Extraindo áudio de {video_path} para {temp_audio_path} usando ffmpeg...")

                # Comando ffmpeg (converte para WAV 16kHz mono)
                command = [
                    ffmpeg_path, "-y", # -y para sobrescrever arquivo temporário se já existir
                    "-i", video_path,
                    "-vn",  # Sem vídeo
                    "-acodec", "pcm_s16le", # Formato WAV
                    "-ar", "16000", # Sample rate
                    "-ac", "1",      # Mono
                    temp_audio_path
                ]
                logger.debug(f"Executando comando ffmpeg: {' '.join(command)}")

                ffmpeg_start_time = time.time()
                # Usar stderr=subprocess.PIPE para capturar erros do ffmpeg
                # Definir um timeout razoável para o ffmpeg
                process = subprocess.run(command, capture_output=True, text=True, check=False, timeout=300, stderr=subprocess.PIPE) # Timeout de 5 minutos
                ffmpeg_end_time = time.time()

                if process.returncode != 0:
                    logger.error(f"Falha ao extrair áudio com ffmpeg (código: {process.returncode}) para {video_path}.")
                    # Logar stderr que pode conter a mensagem de erro do ffmpeg
                    logger.error(f"Erro ffmpeg: {process.stderr.strip()}")
                    # Não retorna aqui ainda, vamos limpar o temp file no finally
                    audio = None # Garante que audio seja None se ffmpeg falhar
                else:
                    logger.info(f"Extração de áudio com ffmpeg bem-sucedida (Duração: {ffmpeg_end_time - ffmpeg_start_time:.2f}s).")
                    # Tentar carregar o áudio extraído
                    try:
                        logger.debug(f"Tentando carregar áudio do arquivo temporário: {temp_audio_path}")
                        audio = whisperx.load_audio(temp_audio_path)
                        logger.info(f"Áudio carregado do arquivo temporário: {temp_audio_path}")
                    except Exception as load_temp_err:
                        logger.error(f"Falha ao carregar áudio do arquivo temporário {temp_audio_path}: {load_temp_err}")
                        # Define audio como None explicitamente se falhar aqui
                        audio = None
            finally:
                # Garantir que o arquivo temporário seja removido
                if temp_audio_path and os.path.exists(temp_audio_path):
                    try:
                        os.remove(temp_audio_path)
                        logger.debug(f"Arquivo temporário removido: {temp_audio_path}")
                    except OSError as remove_err:
                        logger.error(f"Erro ao remover arquivo temporário {temp_audio_path}: {remove_err}")

    # --- Prosseguir com a transcrição se o áudio foi carregado ---
    if audio is None:
        logger.error(f"Falha ao obter áudio de {video_path} após todas as tentativas. Abortando WhisperX.")
        return None

    # Se chegou aqui, 'audio' deve estar carregado
    model = None # Inicializa model fora do try de transcrição
    try:
        # 1. Carregar o modelo WhisperX (Nota: ainda seria ideal carregar fora da função)
        logger.debug(f"Carregando modelo WhisperX: {WHISPER_MODEL} para {DEVICE}")
        model = whisperx.load_model(WHISPER_MODEL, DEVICE, compute_type=COMPUTE_TYPE)
        logger.debug("Modelo WhisperX carregado.")

        # 3. Transcrever
        logger.debug(f"Iniciando transcrição WhisperX para {video_path}")
        result = model.transcribe(audio, batch_size=BATCH_SIZE_WHISPER)
        logger.debug("Transcrição WhisperX concluída.")

        # 4. (Opcional) Alinhamento - Mantido comentado por simplicidade
        # try:
        #     model_a, metadata = whisperx.load_align_model(language_code=result["language"], device=DEVICE)
        #     aligned_result = whisperx.align(result["segments"], model_a, metadata, audio, DEVICE, return_char_alignments=False)
        #     # Usar aligned_result["segments"] para texto com timestamps melhores
        #     transcribed_text = " ".join([seg['text'] for seg in aligned_result["segments"]]).strip()
        # except Exception as align_err:
        #     logger.warning(f"Falha no alinhamento WhisperX para {video_path}: {align_err}. Usando transcrição básica.")
        transcribed_text = " ".join([seg['text'] for seg in result["segments"]]).strip()


        transcription_end_time = time.time()
        logger.info(f"Transcrição WhisperX bem-sucedida para: {video_path} (Duração total: {transcription_end_time - start_time:.2f}s)")
        return {
            "text": transcribed_text,
            "metadata": {
                "transcriber": "whisperx",
                "model": WHISPER_MODEL,
                "language": result.get("language"),
            }
        }

    except ImportError:
        logger.error("Biblioteca whisperx ou suas dependências (como torch) não estão instaladas corretamente.")
        return None
    except Exception as e:
        logger.error(f"Erro durante a etapa de carregamento/transcrição WhisperX para {video_path}: {e}", exc_info=True)
        return None
    finally:
        # Limpar memória da GPU se aplicável, mesmo em caso de erro
        if DEVICE == "cuda" and model is not None:
            logger.debug("Limpando memória da GPU (WhisperX)...")
            try:
                # Tentar deletar explicitamente o modelo e limpar o cache
                del model
                if 'audio' in locals() and audio is not None: del audio
                # Se o alinhamento fosse usado, deletar model_a também
                torch.cuda.empty_cache()
                logger.debug("Cache da GPU limpo.")
            except Exception as cleanup_err:
                 logger.warning(f"Erro durante a limpeza da memória da GPU: {cleanup_err}")

def process_video(video_path: str) -> Optional[Dict[str, Any]]:
    """Processa um único vídeo, orquestrando as tentativas de transcrição.

    Tenta primeiro usar `transcribe_video_assemblyai`. Se falhar (ou se a API
    não estiver configurada), tenta usar `transcribe_video_whisperx` como fallback.

    Adiciona metadados de origem (`origin`, `source_name`) ao resultado final.

    Args:
        video_path (str): O caminho para o arquivo de vídeo a ser processado.

    Returns:
        Optional[Dict[str, Any]]: O dicionário com o resultado da transcrição
                                  (incluindo metadados de origem) se alguma das
                                  tentativas for bem-sucedida, None caso contrário.
    """
    logger.info(f"Processando vídeo: {video_path}")
    transcription_result = transcribe_video_assemblyai(video_path)
    
    if transcription_result is None:
        logger.info(f"Transcrição AssemblyAI falhou ou não configurada. Iniciando fallback para WhisperX: {video_path}") # Log INFO para o fallback
        transcription_result = transcribe_video_whisperx(video_path)
        
    if transcription_result:
        # Adicionar metadados de origem
        transcription_result["metadata"] = transcription_result.get("metadata", {})
        transcription_result["metadata"]["origin"] = "video"
        transcription_result["metadata"]["source_name"] = os.path.basename(video_path)
        logger.info(f"Transcrição obtida para: {video_path}")
        return transcription_result
    else:
        logger.error(f"Falha ao transcrever vídeo: {video_path} com ambos os métodos.")
        return None

def process_all_videos_in_directory(directory: str) -> List[Dict[str, Any]]:
    """Busca e processa todos os arquivos de vídeo suportados em um diretório.

    Itera sobre os arquivos no diretório fornecido, identifica arquivos com
    extensões de vídeo comuns (.mp4, .mov, .avi, .mkv) e chama `process_video`
    para cada um.

    Args:
        directory (str): O caminho para o diretório contendo os arquivos de vídeo.

    Returns:
        List[Dict[str, Any]]: Uma lista contendo os dicionários de resultado para
                               cada vídeo que foi transcrito com sucesso.
                               Retorna lista vazia se o diretório for inválido ou
                               nenhum vídeo for processado.
    """
    transcriptions = []
    logger.info(f"Buscando vídeos no diretório: {directory}")
    try:
        if not directory or not os.path.isdir(directory):
             # Log de erro mais claro se o diretório não for fornecido ou inválido
             logger.error(f"Diretório de vídeos inválido ou não encontrado: '{directory}'")
             return []

        for filename in os.listdir(directory):
            if filename.lower().endswith( (".mp4", ".mov", ".avi", ".mkv", ".mp3", ".wav", ".m4a", ".flac", ".ogg")):
                file_path = os.path.join(directory, filename)
                result = process_video(file_path)
                if result:
                    transcriptions.append(result)
            else:
                logger.debug(f"Ignorando arquivo: {filename}")
                
    except Exception as e:
        logger.error(f"Erro ao processar diretório de vídeos {directory}: {e}", exc_info=True)
        
    logger.info(f"Processamento de vídeos concluído. {len(transcriptions)} transcrições obtidas.")
    return transcriptions

# Exemplo de uso (para teste)
if __name__ == '__main__':
    """Ponto de entrada para executar a transcrição via linha de comando."""
    parser = argparse.ArgumentParser(description="Transcreve vídeos em um diretório usando AssemblyAI com fallback para WhisperX.")
    parser.add_argument("-d", "--directory", default="data/videos_test", 
                        help="Diretório contendo os arquivos de vídeo para processar. Padrão: 'data/videos_test'")
    args = parser.parse_args()

    # Crie um diretório args.directory com um vídeo de teste
    # e defina ASSEMBLYAI_API_KEY no .env
    print(f"Processando vídeos do diretório: {args.directory}")
    results = process_all_videos_in_directory(args.directory)
    if results:
        print("\n--- Transcrições Obtidas ---")
        for i, res in enumerate(results):
            print(f"\n{i+1}. Origem: {res['metadata']['source_name']}")
            print(f"   Texto: {res.get('text', 'N/A')[:100]}...") # Mostra os primeiros 100 caracteres
    else:
        print("Nenhuma transcrição foi obtida.") 