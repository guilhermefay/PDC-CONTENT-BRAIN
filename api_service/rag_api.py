# rag_api.py
"""
API principal para o serviço R2R (RAG API).

Define endpoints para:
- Health check.
- Busca semântica.
- RAG (Retrieval Augmented Generation).
- Agentic RAG.
- Upload de documentos (delegação para R2RClientWrapper).
- Overview de documentos.
- Listagem de chunks de documentos.
- Novo endpoint para ingestão de chunks pré-processados.

Utiliza FastAPI e Pydantic para validação e serialização.
"""

import os
import logging
import time
import datetime
from1 typing import List, Optional, Dict, Any

from1 fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends
from1 fastapi.responses import JSONResponse
from1 pydantic import BaseModel, Field, ConfigDict # Adicionar ConfigDict
from1 dotenv import load_dotenv

# Importar o R2RClientWrapper do novo local
from1 infra.r2r_client import R2RClientWrapper

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do logger
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Inicialização da aplicação FastAPI
app = FastAPI(
    title="PDC R2R API Service",
    description="API para interagir com o sistema R2R (Retrieval Augmented Generation) self-hosted.",
    version="0.1.0",
)

# Inicializar o cliente R2R. Esta instância será compartilhada.
# A URL base do R2R é lida de variáveis de ambiente dentro do wrapper.
r2r_client_wrapper = R2RClientWrapper()

# --- Modelos Pydantic para requisições e respostas ---

class SearchRequest(BaseModel):
    query: str = Field(description="Texto da busca.")
    limit: Optional[int] = Field(default=10, description="Número máximo de resultados.")

class RAGRequest(BaseModel):
    query: str = Field(description="Texto da pergunta para RAG.")
    limit: Optional[int] = Field(default=10, description="Número máximo de chunks para RAG.")
    # Adicionar outros campos conforme necessário para config de geração

class AgenticRAGRequest(BaseModel):
    query: str = Field(description="Texto da pergunta para Agentic RAG.")
    limit: Optional[int] = Field(default=10, description="Número máximo de chunks.")
    # Adicionar outros campos para config de geração e agent

class DocumentOverviewRequest(BaseModel):
    document_id: str = Field(description="ID do documento para obter o overview.")

class ListChunksRequest(BaseModel):
    document_id: str = Field(description="ID do documento para listar os chunks.")
    limit: Optional[int] = Field(default=10, description="Número máximo de chunks.")
    skip: Optional[int] = Field(default=0, description="Número de chunks a pular.")

# Modelos Pydantic para o novo endpoint de ingestão de chunks
class IngestChunkItem(BaseModel):
    content: str = Field(description="Conteúdo textual do chunk.")
    # Adicionar outros campos por chunk se necessário no futuro, ex: chunk_metadata

class IngestChunksRequest(BaseModel):
    document_id: Optional[str] = Field(default=None, description="ID opcional do documento ao qual os chunks pertencem.")
    chunks: List[IngestChunkItem] = Field(description="Lista de chunks a serem ingeridos.")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Metadados a serem associados com os chunks ou o documento.") # Corrigido default_else_factory

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "document_id": "doc-abc-123",
                "chunks": [
                    {"content": "Este é o conteúdo do primeiro chunk."},
                    {"content": "Este é o conteúdo do segundo chunk."}
                ],
                "metadata": {"source": "gdrive", "file_name": "relatorio_anual.pdf"}
            }
        }
    )

class IngestChunksResponse(BaseModel):
    status: str
    message: str
    document_id: Optional[str] = None
    processed_chunks: Optional[int] = None

# --- Endpoints da API ---

@app.get("/health", summary="Verifica a saúde da API")
async def health_check():
    """Endpoint para verificar se a API está ativa e respondendo."""
    logger.info("Health check endpoint chamado.")
    return {
        "status": "healthy",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "message": "R2R API Service is running.",
    }

@app.post("/v1/search", summary="Realiza uma busca semântica")
async def search_documents(request: SearchRequest):
    """Recebe uma query e retorna resultados da busca semântica do R2R."""
    logger.info(f"Busca recebida: query='{request.query}', limit={request.limit}")
    try:
        results = r2r_client_wrapper.search(query=request.query, limit=request.limit)
        return results
    except Exception as e:
        logger.error(f"Erro na busca: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/rag", summary="Realiza RAG (Recuperação Aumentada por Geração)")
async def rag_query(request: RAGRequest):
    """Recebe uma query, recupera chunks relevantes e gera uma resposta."""
    logger.info(f"RAG recebido: query='{request.query}', limit={request.limit}")
    try:
        results = r2r_client_wrapper.rag(query=request.query, limit=request.limit)
        return results
    except Exception as e:
        logger.error(f"Erro no RAG: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/agentic_rag", summary="Realiza Agentic RAG")
async def agentic_rag_query(request: AgenticRAGRequest):
    """
    Recebe uma query e utiliza um fluxo de agente com RAG para gerar uma resposta.
    A implementação exata dependerá das capacidades do R2RClientWrapper.
    """
    logger.info(
        f"Agentic RAG recebido: query='{request.query}', limit={request.limit}"
    )
    try:
        results = r2r_client_wrapper.agentic_rag(
            query=request.query, limit=request.limit
        )
        return results
    except Exception as e:
        logger.error(f"Erro no Agentic RAG: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/documents/upload", summary="Faz upload de um documento para o R2R")
async def upload_document_endpoint(
    file: UploadFile = File(...),
    document_id: Optional[str] = Form(None),
    metadata_json: Optional[str] = Form("{}"), # Receber metadados como string JSON
):
    """
    Faz upload de um arquivo para o R2R. 
    O arquivo é salvo temporariamente e passado para o R2RClientWrapper.
    Metadados podem ser enviados como uma string JSON no campo 'metadata_json'.
    """
    logger.info(
        f"Upload de documento recebido: filename='{file.filename}', document_id='{document_id}'"
    )
    temp_file_path = None
    try:
        # Salvar arquivo temporariamente
        # TODO: Considerar usar um diretório temporário mais robusto (tempfile module)
        # e garantir limpeza em caso de erro.
        temp_file_path = f"/tmp/{file.filename}" # Cuidado com concorrência e segurança aqui em produção real
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        logger.info(f"Arquivo temporário salvo em: {temp_file_path}")

        # Parsear metadados JSON
        try:
            metadata = json.loads(metadata_json)
            if not isinstance(metadata, dict):
                raise ValueError("Metadados devem ser um objeto JSON.")
        except json.JSONDecodeError:
            logger.error("Erro ao decodificar metadata_json. Usando dicionário vazio.")
            raise HTTPException(
                status_code=400, detail="metadata_json inválido. Deve ser um JSON válido."
            )
        except ValueError as ve:
            logger.error(f"Erro de valor nos metadados: {ve}")
            raise HTTPException(status_code=400, detail=str(ve))

        response = r2r_client_wrapper.upload_file(
            file_path=temp_file_path, metadata=metadata, id=document_id
        )
        return response
    except HTTPException: # Re-lançar HTTPExceptions para que o FastAPI as trate
        raise
    except Exception as e:
        logger.error(f"Erro no upload do documento: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro no servidor: {str(e)}")
    finally:
        # Limpar arquivo temporário
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                logger.info(f"Arquivo temporário {temp_file_path} removido.")
            except Exception as e_rm:
                logger.error(
                    f"Erro ao remover arquivo temporário {temp_file_path}: {e_rm}"
                )
        if file:
            await file.close() # Garantir que o UploadFile seja fechado

@app.post("/v1/documents/overview", summary="Obtém o overview de um documento")
async def get_document_overview(request: DocumentOverviewRequest):
    """Obtém um resumo ou informações chave de um documento existente no R2R."""
    logger.info(f"Requisição de overview para document_id: {request.document_id}")
    try:
        overview = r2r_client_wrapper.overview(document_id=request.document_id)
        return overview
    except Exception as e:
        logger.error(f"Erro ao obter overview: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/documents/chunks", summary="Lista chunks de um documento")
async def list_document_chunks(request: ListChunksRequest):
    """Lista os chunks de um documento específico armazenado no R2R."""
    logger.info(
        f"Requisição para listar chunks: document_id={request.document_id}, limit={request.limit}, skip={request.skip}"
    )
    try:
        chunks = r2r_client_wrapper.list_chunks(
            document_id=request.document_id, limit=request.limit, skip=request.skip
        )
        return chunks
    except Exception as e:
        logger.error(f"Erro ao listar chunks: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# Novo endpoint para ingestão de chunks pré-processados
@app.post("/internal/v1/ingest_chunks", 
            response_model=IngestChunksResponse, 
            summary="Ingere uma lista de chunks pré-processados para um documento.")
async def ingest_preprocessed_chunks(request_data: IngestChunksRequest):
    """
    Recebe uma lista de chunks de texto e os ingere no R2R associados a um document_id (opcional)
    e metadados (opcional).
    Este é um endpoint interno, idealmente chamado pelo etl-worker.
    """
    logger.info(f"Recebida requisição para ingestão de {len(request_data.chunks)} chunks para document_id: {request_data.document_id}")
    try:
        # Delega a lógica de ingestão para o R2RClientWrapper, que usa o cliente assíncrono
        # Os chunks já vêm como List[IngestChunkItem], precisamos passar List[Dict]
        chunks_to_ingest = [chunk.model_dump() for chunk in request_data.chunks]
        
        response = await r2r_client_wrapper.ingest_chunks(
            chunks=chunks_to_ingest,
            document_id=request_data.document_id,
            metadata=request_data.metadata
        )
        # A resposta do wrapper.ingest_chunks já deve ser um dicionário.
        # Se for um objeto Pydantic, precisaria de .model_dump()
        # Assumindo que é um dict como definido no wrapper (simulado).
        return IngestChunksResponse(
            status=response.get("status", "error"),
            message=response.get("message", "Erro desconhecido na ingestão de chunks."),
            document_id=response.get("document_id"),
            processed_chunks=len(request_data.chunks) if response.get("status") == "success" else 0
        )

    except Exception as e:
        logger.error(f"Erro durante a ingestão de chunks: {e}", exc_info=True)
        # Retornar uma resposta de erro padronizada
        return JSONResponse(
            status_code=500,
            content=IngestChunksResponse(
                status="error",
                message=f"Erro interno do servidor: {str(e)}",
                document_id=request_data.document_id,
                processed_chunks=0
            ).model_dump()
        )

# Adicionar importação de shutil se não estiver presente
import shutil

# Ponto de entrada para desenvolvimento local (se não estiver usando uvicorn diretamente)
if __name__ == "__main__":
    import uvicorn
    logger.info("Iniciando R2R API Service localmente com Uvicorn na porta 8000.")
    uvicorn.run("rag_api:app", host="0.0.0.0", port=8000, reload=True) # Adicionado reload=True para dev
