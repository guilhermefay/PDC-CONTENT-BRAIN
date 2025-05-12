#!/usr/bin/env python3
import os
import json
import argparse
import logging
from datetime import datetime, timezone
import subprocess
import time

import assemblyai as aai
from dotenv import load_dotenv

# Configuração básica de logging
log_format = '%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente do .env
load_dotenv()

ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")

if not ASSEMBLYAI_API_KEY:
    logger.error("ASSEMBLYAI_API_KEY não encontrada no ambiente. Verifique seu arquivo .env.")
    exit(1)

aai.settings.api_key = ASSEMBLYAI_API_KEY

# Constantes
DEFAULT_OUTPUT_DIR = "data/transcriptions_youtube"
AUDIO_TEMP_DIR = "data/transcriptions_youtube/audio_temp"
TRANSCRIPTIONS_DIR_NAME = "transcriptions"
METADATA_DIR_NAME = "metadata"

def ensure_dir(directory_path: str):
    """Garante que um diretório exista, criando-o se necessário."""
    os.makedirs(directory_path, exist_ok=True)

def get_video_ids_from_channel(channel_url: str, limit: int = None) -> list[str]:
    """
    Usa yt-dlp para listar IDs de vídeos de um canal do YouTube.
    Retorna uma lista de IDs de vídeo.
    """
    logger.info(f"Buscando IDs de vídeo do canal: {channel_url}")
    cmd = [
        'yt-dlp',
        '--get-id',
        '--flat-playlist', # Essencial para obter IDs de uma lista/canal
        '--skip-download', # Não queremos baixar nada aqui
        '--no-warnings', # Suprime avisos do yt-dlp para um output mais limpo
        channel_url
    ]

    if limit and limit > 0:
        # yt-dlp usa --playlist-items para limitar, e aceita ranges como 1-5
        # Para um limite simples, pegamos os primeiros 'limit' itens.
        cmd.extend(['--playlist-items', f"1-{limit}"])
        logger.info(f"Limitando a {limit} vídeo(s).")
    else:
        logger.info("Buscando todos os vídeos do canal (sem limite explícito via script).")

    try:
        # Timeout adicionado para evitar que o script fique preso indefinidamente
        process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=120) # Timeout de 2 minutos
        video_ids = [line.strip() for line in process.stdout.splitlines() if line.strip()]
        
        if not video_ids:
            logger.warning(f"Nenhum ID de vídeo retornado por yt-dlp para {channel_url}. Saída stdout:\n{process.stdout}")
            return []
            
        logger.info(f"Encontrados {len(video_ids)} IDs de vídeo.")
        if limit and len(video_ids) > limit:
             logger.warning(f"yt-dlp retornou {len(video_ids)} IDs, mas o limite era {limit}. Truncando a lista.")
             # Isso não deveria acontecer se --playlist-items funcionar como esperado, mas é uma salvaguarda.
             return video_ids[:limit]
        return video_ids
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout ao buscar IDs de vídeo para {channel_url} após 120 segundos.")
        return []
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar yt-dlp para buscar IDs de vídeo (código de saída: {e.returncode}): {channel_url}")
        logger.error(f"Comando: {' '.join(e.cmd)}")
        logger.error(f"Stderr: {e.stderr.strip() if e.stderr else 'N/A'}")
        logger.error(f"Stdout: {e.stdout.strip() if e.stdout else 'N/A'}")
        return []
    except FileNotFoundError:
        logger.error("Erro: O comando 'yt-dlp' não foi encontrado. Verifique se está instalado e no PATH do sistema.")
        return []
    except Exception as e:
        logger.error(f"Um erro inesperado ocorreu ao buscar IDs de vídeo para {channel_url}: {e}")
        return []

def download_audio(video_id: str, output_dir: str) -> str | None:
    """
    Baixa o áudio de um vídeo do YouTube usando yt-dlp.
    Salva em formato .wav no diretório especificado.
    Retorna o caminho para o arquivo de áudio baixado ou None em caso de falha.
    """
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    # Usar .tmp como extensão durante o download, yt-dlp renomeia ao concluir.
    # O nome final do arquivo será {video_id}.wav.
    # O template de saída -o garante que o arquivo seja salvo no diretório correto com o nome correto.
    output_template = os.path.join(output_dir, f"{video_id}.%(ext)s")
    audio_filepath_final = os.path.join(output_dir, f"{video_id}.wav") # Caminho esperado após download

    # Garante que o diretório de áudio temporário exista
    ensure_dir(output_dir) 

    logger.info(f"Baixando áudio para video_id: {video_id} para {output_template} (esperado como .wav)")
    cmd = [
        'yt-dlp',
        '--no-check-certificate', # Adicionado para evitar problemas com SSL em alguns ambientes
        '-x',  # Extrair áudio
        '--audio-format', 'wav',
        # Tentar forçar a qualidade do áudio para algo razoável se o padrão for muito baixo
        # 'bestaudio' pode resultar em formatos não-wav se wav não for o melhor, então é mais seguro
        # especificar o formato e deixar yt-dlp converter se necessário.
        # '--audio-quality', '0', # 0 é a melhor qualidade para o formato escolhido
        '-o', output_template,
        video_url
    ]
    try:
        # Timeout mais longo para download de áudio
        process = subprocess.run(cmd, check=True, capture_output=True, timeout=300) # Timeout de 5 minutos
        # Verifica se o arquivo .wav esperado foi criado
        if os.path.exists(audio_filepath_final):
            logger.info(f"Áudio baixado com sucesso: {audio_filepath_final}")
            logger.debug(f"Saída do yt-dlp (download): {process.stdout.strip()}")
            return audio_filepath_final
        else:
            logger.error(f"yt-dlp concluiu mas o arquivo .wav esperado não foi encontrado: {audio_filepath_final}")
            logger.error(f"Saída stdout do yt-dlp: {process.stdout.strip() if process.stdout else 'N/A'}")
            logger.error(f"Saída stderr do yt-dlp: {process.stderr.strip() if process.stderr else 'N/A'}")
            return None
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout ao baixar áudio para {video_id} após 300 segundos.")
        # Tenta limpar arquivos parciais se o download falhou por timeout
        if os.path.exists(audio_filepath_final + ".part"):
            try:
                os.remove(audio_filepath_final + ".part")
            except OSError:
                pass # Ignora erro na remoção de arquivo parcial
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar yt-dlp para baixar áudio (código de saída: {e.returncode}): {video_id}")
        logger.error(f"Comando: {' '.join(e.cmd)}")
        logger.error(f"Stderr: {e.stderr.strip() if e.stderr else 'N/A'}")
        logger.error(f"Stdout: {e.stdout.strip() if e.stdout else 'N/A'}")
        return None
    except FileNotFoundError:
        logger.error("Erro: O comando 'yt-dlp' não foi encontrado ao baixar áudio.")
        return None
    except Exception as e:
        logger.error(f"Um erro inesperado ocorreu ao baixar áudio para {video_id}: {e}")
        return None

def transcribe_audio_with_assemblyai(audio_filepath: str, video_id: str) -> tuple[str | None, dict | None, str | None]:
    """
    Transcreve um arquivo de áudio usando AssemblyAI.
    Retorna o texto da transcrição, os dados da transcrição (incluindo palavras e timestamps)
    e o ID da transcrição do AssemblyAI.
    """
    logger.info(f"Iniciando transcrição com AssemblyAI para: {audio_filepath} (video_id: {video_id})")
    try:
        config = aai.TranscriptionConfig(
            speaker_labels=True, 
            language_code="pt",
            # word_timestamps=True # Já incluído no objeto transcript.words
        )
        transcriber = aai.Transcriber(config=config)
        
        # Aumentar o timeout de polling padrão do SDK do AssemblyAI se necessário para arquivos longos.
        # O SDK tem seu próprio polling interno. O timeout aqui é para a chamada .transcribe()
        # que espera a conclusão. Para vídeos muito longos, pode ser necessário ajustar 
        # aai.settings.polling_interval ou aai.settings.polling_timeout (consultar docs do SDK)
        # Por agora, vamos confiar nos defaults do SDK, que geralmente são robustos.
        
        transcript = transcriber.transcribe(audio_filepath)

        if transcript.status == aai.TranscriptStatus.error:
            logger.error(f"Falha na transcrição AssemblyAI para {audio_filepath} (video_id: {video_id}): {transcript.error}")
            return None, None, None

        if not transcript.words:
            logger.warning(f"Transcrição AssemblyAI para {audio_filepath} (video_id: {video_id}) não retornou palavras (transcript.words está vazio ou None).")
            # Pode acontecer para áudio muito curto ou silencioso.
            # Ainda podemos tentar retornar o texto principal se existir.
            if transcript.text:
                 logger.info(f"Transcrição retornou texto principal mas sem words: '{transcript.text[:100]}...'")
                 # Monta uma estrutura básica para transcript_data se apenas o texto estiver disponível
                 transcript_data_for_json = {
                    "text": transcript.text,
                    "words": [],
                    "utterances": [] 
                 }
                 return transcript.text, transcript_data_for_json, transcript.id
            else:
                logger.error(f"Transcrição AssemblyAI não retornou nem texto nem palavras para {audio_filepath}.")
                return None, None, None

        transcript_data_for_json = {
            "text": transcript.text, # Texto completo da transcrição
            "words": [
                {"text": word.text, "start": word.start, "end": word.end, "confidence": word.confidence}
                for word in transcript.words
            ],
            "utterances": [] # Será preenchido se speaker_labels funcionar
        }
        
        if transcript.utterances:
            logger.info(f"Diarização (speaker_labels) detectada: {len(transcript.utterances)} falas.")
            transcript_data_for_json["utterances"] = [
                {"speaker": utt.speaker, "text": utt.text, "start": utt.start, "end": utt.end}
                for utt in transcript.utterances
            ]
        else:
            logger.info("Nenhuma diarização (speaker_labels) retornada pela AssemblyAI.")

        logger.info(f"Transcrição AssemblyAI concluída para {audio_filepath} (video_id: {video_id}). ID AssemblyAI: {transcript.id}")
        return transcript.text, transcript_data_for_json, transcript.id
        
    except Exception as e:
        # Captura exceções mais amplas que podem ocorrer durante a chamada à API ou processamento.
        logger.error(f"Erro excepcional durante a transcrição com AssemblyAI para {audio_filepath} (video_id: {video_id}): {e}", exc_info=True)
        return None, None, None

def save_transcription_and_metadata(
    video_id: str, 
    transcript_text: str, 
    transcript_data: dict,
    assemblyai_transcript_id: str,
    video_title: str, # Adicionado para metadados
    channel_url: str, # Adicionado para metadados
    output_dir: str
    ):
    """Salva a transcrição em .txt e os metadados estruturados em .json."""
    transcriptions_path = os.path.join(output_dir, TRANSCRIPTIONS_DIR_NAME, video_id)
    metadata_path = os.path.join(output_dir, METADATA_DIR_NAME, video_id)
    ensure_dir(transcriptions_path)
    ensure_dir(metadata_path)

    txt_filepath = os.path.join(transcriptions_path, f"{video_id}.txt")
    json_filepath = os.path.join(metadata_path, f"{video_id}.json") # Salva dados estruturados aqui
    # Metadata original também será salvo aqui, junto com os dados da transcrição
    # ou podemos ter um video_metadata.json e um transcript_detail.json

    with open(txt_filepath, 'w', encoding='utf-8') as f:
        f.write(transcript_text)
    logger.info(f"Transcrição (texto) salva em: {txt_filepath}")

    metadata_payload = {
        "video_id": video_id,
        "video_url": f"https://www.youtube.com/watch?v={video_id}",
        "video_title": video_title, # Usar o título real obtido pelo yt-dlp
        "channel_url": channel_url,
        "source_type": "youtube_video_transcription", # Novo source_type
        "transcribed_at": datetime.now(timezone.utc).isoformat(),
        "assemblyai_transcript_id": assemblyai_transcript_id,
        "transcription_details": transcript_data # Contém .text, .words, .utterances
    }

    with open(json_filepath, 'w', encoding='utf-8') as f:
        json.dump(metadata_payload, f, ensure_ascii=False, indent=4)
    logger.info(f"Metadados e transcrição estruturada salvos em: {json_filepath}")

def get_video_metadata_yt_dlp(video_id: str) -> dict:
    """Obtém metadados do vídeo (título) usando yt-dlp."""
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    # Usaremos --print "%(title)s" que é mais robusto e direto para obter o título.
    # --get-title pode às vezes incluir outras informações ou ter comportamento variado.
    cmd = [
        'yt-dlp',
        '--print', '%(title)s', # Imprime o título do vídeo diretamente
        '--skip-download',
        '--no-warnings',
        video_url
    ]
    logger.info(f"Buscando metadados (título) para video_id: {video_id}")
    try:
        process = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=60) # Timeout de 1 minuto
        title = process.stdout.strip()
        if not title:
            logger.warning(f"yt-dlp não retornou título para {video_id}. Saída stdout: {process.stdout}")
            return {"title": "Título Desconhecido"}
        logger.info(f"Título obtido para {video_id}: {title}")
        return {"title": title}
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout ao buscar título para {video_id} após 60 segundos.")
        return {"title": "Título Desconhecido (Timeout)"}
    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar yt-dlp para buscar título (código de saída: {e.returncode}): {video_id}")
        logger.error(f"Comando: {' '.join(e.cmd)}")
        logger.error(f"Stderr: {e.stderr.strip() if e.stderr else 'N/A'}")
        logger.error(f"Stdout: {e.stdout.strip() if e.stdout else 'N/A'}")
        return {"title": "Título Desconhecido (Erro yt-dlp)"}
    except FileNotFoundError:
        logger.error("Erro: O comando 'yt-dlp' não foi encontrado ao buscar título. Verifique se está instalado e no PATH.")
        # Esta exceção não deveria ocorrer se a função anterior (get_video_ids_from_channel) funcionou.
        return {"title": "Título Desconhecido (yt-dlp não encontrado)"}
    except Exception as e:
        logger.error(f"Um erro inesperado ocorreu ao buscar título para {video_id}: {e}")
        return {"title": "Título Desconhecido (Erro inesperado)"}

def main():
    parser = argparse.ArgumentParser(description="Transcreve vídeos de um canal do YouTube usando AssemblyAI.")
    parser.add_argument("--channel-url", required=True, help="URL do canal do YouTube.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help=f"Diretório base para salvar transcrições e metadados (padrão: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--limit", type=int, help="Limitar o número de vídeos a processar (para teste).")
    parser.add_argument("--force-retranscribe", action="store_true", help="Forçar a re-transcrição de vídeos já processados.")
    parser.add_argument("--log-level", default="INFO", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help="Nível de logging.")
    
    args = parser.parse_args()

    # Ajusta o nível de logging global
    logging.getLogger().setLevel(args.log_level.upper())
    logger.info(f"Nível de log configurado para: {args.log_level.upper()}")

    # Cria diretórios de saída se não existirem
    ensure_dir(args.output_dir)
    ensure_dir(AUDIO_TEMP_DIR)
    ensure_dir(os.path.join(args.output_dir, TRANSCRIPTIONS_DIR_NAME))
    ensure_dir(os.path.join(args.output_dir, METADATA_DIR_NAME))

    video_ids = get_video_ids_from_channel(args.channel_url, args.limit)

    if not video_ids:
        logger.info("Nenhum ID de vídeo encontrado ou erro ao buscar. Encerrando.")
        return

    processed_count = 0
    failed_count = 0

    for video_id in video_ids:
        logger.info(f"--- Processando video_id: {video_id} ---")
        
        # Define caminhos para os arquivos de saída desta iteração
        # (usado para verificar se já foi processado)
        # A verificação de "já processado" será feita olhando o .json de metadados final
        final_metadata_file = os.path.join(args.output_dir, METADATA_DIR_NAME, video_id, f"{video_id}.json")

        if os.path.exists(final_metadata_file) and not args.force_retranscribe:
            logger.info(f"Metadados para {video_id} já existem em {final_metadata_file} e --force-retranscribe não foi usado. Pulando.")
            continue

        video_meta = get_video_metadata_yt_dlp(video_id) # Pega o título
        video_title = video_meta.get("title", "Título Desconhecido")

        audio_filepath = download_audio(video_id, AUDIO_TEMP_DIR)
        if not audio_filepath:
            logger.error(f"Falha no download do áudio para {video_id}. Pulando para o próximo.")
            failed_count += 1
            continue
        
        transcript_text, transcript_data, assemblyai_id = transcribe_audio_with_assemblyai(audio_filepath, video_id)
        
        if not transcript_text or not transcript_data or not assemblyai_id:
            logger.error(f"Falha na transcrição do áudio para {video_id}. Pulando para o próximo.")
            failed_count += 1
            # Tenta limpar o áudio baixado se a transcrição falhar
            try:
                os.remove(audio_filepath)
                logger.info(f"Arquivo de áudio temporário removido: {audio_filepath}")
            except OSError as e:
                logger.warning(f"Não foi possível remover o arquivo de áudio temporário {audio_filepath}: {e}")
            continue

        save_transcription_and_metadata(
            video_id, 
            transcript_text, 
            transcript_data,
            assemblyai_id,
            video_title,
            args.channel_url,
            args.output_dir
        )

        # Limpa o arquivo de áudio baixado após o sucesso
        try:
            os.remove(audio_filepath)
            logger.info(f"Arquivo de áudio temporário removido: {audio_filepath}")
        except OSError as e:
            logger.warning(f"Não foi possível remover o arquivo de áudio temporário {audio_filepath}: {e}")

        processed_count += 1
        logger.info(f"--- Concluído processamento para video_id: {video_id} ---")
        # Adicionar um pequeno delay para não sobrecarregar APIs (se necessário)
        # time.sleep(1) 

    logger.info("=================================================================")
    logger.info(f"Processamento de transcrições concluído.")
    logger.info(f"Total de vídeos para processar (conforme limite/disponível): {len(video_ids)}")
    logger.info(f"Vídeos processados com sucesso: {processed_count}")
    logger.info(f"Vídeos com falha: {failed_count}")
    logger.info(f"Transcrições e metadados salvos em: {args.output_dir}")
    logger.info("=================================================================")

if __name__ == "__main__":
    main() 