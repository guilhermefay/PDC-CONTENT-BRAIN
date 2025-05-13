# rag_api.py
"""
API FastAPI para fornecer funcionalidade de busca semântica (search) e RAG
sobre o conteúdo indexado no R2R Cloud.

Endpoints:
    - `/query` (POST): Recebe uma query de usuário e retorna resultados
      relevantes do R2R (busca simples) ou uma resposta gerada por RAG.
      Requer autenticação via JWT (Supabase).
    - `/health` (GET): Verifica a saúde da API e suas dependências (Supabase, R2R).

Autenticação:
    - Utiliza tokens JWT emitidos pelo Supabase, passados no header
      `Authorization: Bearer <token>`.
    - A validação do token verifica assinatura, expiração e audience.

Dependências Principais:
    - FastAPI: Framework web.
    - Pydantic: Validação de dados.
    - Supabase Python Client: Interação com Supabase (para autenticação futura, se necessário).
    - R2R Client Wrapper (`infra/r2r_client.py`): Interface com a API R2R Cloud.
    - PyJWT: Decodificação e validação de tokens JWT.
    - python-dotenv: Carregamento de variáveis de ambiente.
"""
import os
import logging
from1 fastapi import FastAPI, HTTPException, Depends, Header, Request, status
from1 fastapi.security import OAuth2PasswordBearer
from1 pydantic import BaseModel, Field, ConfigDict
from1 dotenv import load_dotenv
from1 supabase import create_client, Client
from1 typing import List, Dict, Any, Optional, Union
import jwt
from1 datetime import datetime, UTC
import time

# Importar R2RClientWrapper (ajustar caminho se necessário ao executar)
# Assumindo que a API roda da raiz do projeto, o caminho direto funciona.
# Se rodar de dentro de /api, pode precisar de 'from1 ..infra.r2r_client import R2RClientWrapper'
from1 infra.r2r_client import R2RClientWrapper 

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')

# Carregar variáveis de ambiente
load_dotenv(verbose=True)

# Configurar Supabase client
supabase_url: str = os.environ.get("SUPABASE_URL")
supabase_key: str = os.environ.get("SUPABASE_SERVICE_KEY")
jwt_secret: str = os.environ.get("SUPABASE_JWT_SECRET")

# <<< DEBUG: Log values read from1 environment >>>
logging.info(f"[API Init] Read SUPABASE_URL: {supabase_url}")
logging.info(f"[API Init] Read SUPABASE_SERVICE_KEY: {supabase_key}")
# <<< END DEBUG >>>

supabase_client: Client = None
if supabase_url and supabase_key:
    try:
        supabase_client = create_client(supabase_url, supabase_key)
        logging.info("Supabase client initialized for RAG API.")
    except Exception as e:
        logging.error(f"Error initializing Supabase client for RAG API: {e}")
else:
    logging.warning("Supabase URL or Service Key not found. RAG API database connection disabled.")

# Inicializar R2R Client Wrapper globalmente
try:
    r2r_client = R2RClientWrapper()
    logging.info("R2R Client Wrapper initialized successfully for RAG API.")
except ValueError as e:
    logging.error(f"Failed to initialize R2R Client Wrapper: {e}. R2R features disabled.")
    r2r_client = None
except Exception as e:
    logging.error(f"Unexpected error initializing R2R Client Wrapper: {e}. R2R features disabled.", exc_info=True)
    r2r_client = None

# Configurações
# EMBEDDINGS_MODEL = "text-embedding-3-small"
# EMBEDDINGS_DIMENSIONS = 1536

# Modelos Pydantic
class QueryRequest(BaseModel):
    """Modelo Pydantic para a requisição de busca/RAG."""
    query: str = Field(..., min_length=1, description="A pergunta ou termo de busca do usuário.")
    top_k: int = Field(default=5, ge=1, le=20, description="Número máximo de resultados relevantes a serem considerados.")
    use_rag: bool = Field(default=False, description="Se True, executa RAG em vez de busca simples.")
    # Adicionar filtros ou config de geração se necessário
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Filtros de metadados opcionais para a busca R2R (ex: {'source': 'gdrive'}).")
    generation_config: Optional[Dict[str, Any]] = Field(default=None, description="Configurações para a geração RAG no R2R (ex: {'model': 'gpt-4o'}).")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "query": "Como melhorar a gestão do tempo?",
                "top_k": 3,
                "use_rag": True,
                "filters": {"tags": "produtividade"}
            }
        }
    )

class SearchResultChunk(BaseModel):
    """Modelo para um único chunk retornado na busca R2R."""
    document_id: str = Field(description="ID do documento pai do chunk.")
    chunk_id: str = Field(description="ID único do chunk.")
    content: str = Field(description="Conteúdo textual do chunk.")
    metadata: dict = Field(description="Metadados associados ao chunk.")
    similarity: float = Field(description="Pontuação de relevância da busca.")

class QuerySearchResponse(BaseModel):
    """Modelo de resposta para busca simples (`use_rag=False`)."""
    results: List[SearchResultChunk] = Field(description="Lista de chunks relevantes encontrados.")
    total_found: int = Field(description="Número total de itens encontrados na busca.")
    query_time_ms: float = Field(description="Tempo gasto para processar a query em milissegundos.")

class QueryRagResponse(BaseModel):
    """Modelo de resposta para RAG (`use_rag=True`)."""
    llm_response: str = Field(description="Resposta gerada pelo LLM com base nos resultados encontrados.")
    search_results: List[SearchResultChunk] = Field(description="Chunks usados como contexto para a geração RAG.")
    query_time_ms: float = Field(description="Tempo gasto para processar a query RAG em milissegundos.")

class ErrorResponse(BaseModel):
    """Modelo de resposta para erros."""
    detail: str = Field(description="Descrição do erro.")
    error_code: Optional[str] = Field(default=None, description="Código de erro interno (opcional).")

class HealthCheckResponse(BaseModel):
    """Modelo de resposta para o health check."""
    status: str = Field(description="Status geral da API ('healthy' ou 'degraded').")
    timestamp: str = Field(description="Timestamp da verificação.")
    dependencies: dict = Field(description="Status das dependências externas (Supabase, R2R).")

# Configurar autenticação
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)

async def validate_token(authorization: str = Header(...)) -> dict:
    """
    Valida o token JWT Bearer fornecido no header Authorization.

    Verifica a presença do token, o formato Bearer, a assinatura usando
    o `SUPABASE_JWT_SECRET`, a expiração (`exp` claim) e a audiência (`aud` claim).

    Args:
        authorization (str): Conteúdo do header `Authorization`.

    Returns:
        dict: O payload decodificado do token se for válido.

    Raises:
        HTTPException(401): Se o token estiver ausente, mal formatado, inválido,
                             expirado ou com audiência incorreta.
        HTTPException(500): Se o `SUPABASE_JWT_SECRET` não estiver configurado no servidor.
    """
    token = authorization.split(" ")[1] if authorization.startswith("Bearer ") else None
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token not provided"
        )
        
    if not jwt_secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="JWT secret not configured"
        )
        
    try:
        # Verificar assinatura, claims e AUDIENCE
        payload = jwt.decode(
            token,
            jwt_secret,
            algorithms=["HS256"],
            audience="authenticated"
        )
        
        # Validar campos necessários
        if not payload.get("sub"):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: missing subject claim"
            )
            
        # Verificar expiração
        exp = payload.get("exp")
        if exp and datetime.from1timestamp(exp, UTC) < datetime.now(UTC):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token expired"
            )
            
        # Adicionar log do payload para depuração (opcional, remover em produção)
        logging.debug(f"Token payload validated: {payload}")
        return payload
        
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expired"
        )
    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {str(e)}"
        )

# Injeção de Dependência para Supabase
def get_supabase() -> Client:
    """Função de dependência FastAPI para obter o cliente Supabase inicializado.

    Raises:
        HTTPException(503): Se o cliente Supabase não estiver disponível.
    """
    if not supabase_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection not available",
            headers={"Retry-After": "60"}
        )
    return supabase_client

# Inicializar FastAPI app
app = FastAPI(
    title="PDC Content Brain RAG API",
    description="API para busca semântica em documentos com controle de acesso baseado em roles",
    version="1.0.0"
)

@app.post(
    "/query",
    # Atualizar response_model para refletir as duas possíveis respostas
    response_model=Union[QuerySearchResponse, QueryRagResponse],
    summary="Executa busca semântica ou RAG",
    description="Recebe uma query e retorna chunks relevantes (busca) ou uma resposta gerada (RAG) a partir do conteúdo indexado no R2R.",
    responses={
        200: {"description": "Resposta bem-sucedida (busca ou RAG)"},
        401: {"model": ErrorResponse, "description": "Erro de Autenticação"},
        503: {"model": ErrorResponse, "description": "Serviço indisponível (R2R ou Supabase)"},
        500: {"model": ErrorResponse, "description": "Erro interno do servidor"}
    }
)
async def query_documents(
    request: QueryRequest,
    token_payload: dict = Depends(validate_token)
) -> Union[QuerySearchResponse, QueryRagResponse]: # Tipo de retorno atualizado
    """
    Endpoint principal para buscar documentos ou executar RAG.

    - Se `request.use_rag` for `False` (padrão), executa uma busca semântica
      no R2R usando `r2r_client.search()` e retorna os chunks encontrados
      no formato `QuerySearchResponse`.
    - Se `request.use_rag` for `True`, executa RAG no R2R usando
      `r2r_client.rag()` e retorna a resposta do LLM e os chunks de contexto
      no formato `QueryRagResponse`.

    A autenticação é feita via token JWT Bearer.
    Filtros podem ser aplicados à busca/RAG.

    Args:
        request (QueryRequest): O corpo da requisição com a query, top_k, use_rag, etc.
        token_payload (dict): O payload do token JWT validado (injetado pelo Depends).

    Returns:
        Union[QuerySearchResponse, QueryRagResponse]: A resposta formatada da busca ou RAG.

    Raises:
        HTTPException: Em caso de erro (autenticação, serviço indisponível, erro interno).
    """
    if not r2r_client:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="R2R service client is not available.",
            headers={"Retry-After": "60"}
        )
        
    try:
        start_time = time.time()

        # Determinar nível de acesso do usuário a partir do token
        user_role = token_payload.get('app_metadata', {}).get('role', 'student') # Assumir 'student' como padrão
        user_id = token_payload.get('sub')
        logging.info(f"User {user_id} accessing with role: {user_role}")

        # Construir filtros R2R
        final_filters = request.filters.copy() if request.filters else {}
        if user_role not in ['admin', 'internal_user']: # Apenas estudantes veem conteúdo 'student'
            final_filters['access_level'] = 'student'
            logging.info("Applying 'access_level=student' filter for user.")
        else:
             logging.info("Internal user/admin detected, no access level filter applied.")

        # Remover filtros vazios, se houver (opcional, dependendo do R2R)
        final_filters = {k: v for k, v in final_filters.items() if v is not None}

        query_time_ms = round((time.time() - start_time) * 1000, 2)

        if request.use_rag:
            # --- Lógica RAG --- 
            logging.info(f"Sending RAG query to R2R: '{request.query}' with k={request.top_k}, Filters: {final_filters}") # Usar final_filters
            r2r_rag_data = r2r_client.rag(
                query=request.query,
                limit=request.top_k,
                filters=final_filters, # Usar final_filters
                generation_config=request.generation_config
            )
            
            if not r2r_rag_data.get("success"):
                error_detail = r2r_rag_data.get("error", "Unknown R2R RAG error")
                logging.error(f"R2R RAG failed: {error_detail}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error performing RAG via R2R: {error_detail}"
                )

            llm_response_text = r2r_rag_data.get('response')
            search_results_raw = r2r_rag_data.get('results', [])
            
            # Formatar search_results para QueryRagResponse
            formatted_search_results = []
            for res in search_results_raw:
                 # Adaptação: A resposta RAG pode ter um formato ligeiramente diferente
                 # Assumindo que `res` é um dicionário do chunk com `text` e `metadata`
                 content = res.get("text")
                 metadata = res.get("metadata", {})
                 doc_id = metadata.get("document_id", "unknown") # Tentar obter IDs
                 chunk_id = metadata.get("chunk_id", "unknown")
                 score = res.get("score", 0.0) # Score pode não estar presente no RAG
                 
                 if content:
                     formatted_search_results.append(
                        SearchResultChunk(
                            document_id=str(doc_id),
                            chunk_id=str(chunk_id),
                            content=content,
                            metadata=metadata,
                            similarity=float(score)
                        )
                    )

            return QueryRagResponse(
                llm_response=llm_response_text or "", # Garantir que é string
                search_results=formatted_search_results,
                query_time_ms=query_time_ms
            )

        else:
            # --- Lógica Busca Simples --- 
            logging.info(f"Sending search query to R2R: '{request.query}' with k={request.top_k}")
            r2r_search_data = r2r_client.search(
                query=request.query,
                limit=request.top_k
            )
            
            if not r2r_search_data.get("success"):
                error_detail = r2r_search_data.get("error", "Unknown R2R search error")
                logging.error(f"R2R search failed: {error_detail}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error performing search via R2R: {error_detail}"
                )

            search_results_raw = r2r_search_data.get("results", [])
            logging.info(f"Received {len(search_results_raw)} search results from1 R2R.")

            formatted_search_results = []
            for res in search_results_raw:
                # O método search retorna diretamente a lista de chunks
                content = res.get("text")
                metadata = res.get("metadata", {})
                # Tentar extrair IDs do metadata, se disponíveis
                doc_id = metadata.get("document_id", "unknown")
                chunk_id = metadata.get("chunk_id", "unknown")
                similarity_score = res.get("similarity", 0.0)
                
                if content:
                    formatted_search_results.append(
                        SearchResultChunk(
                            document_id=str(doc_id),
                            chunk_id=str(chunk_id),
                            content=content,
                            metadata=metadata,
                            similarity=float(similarity_score)
                        )
                    )
                else:
                    logging.warning(f"Resultado R2R search sem conteúdo encontrado: {res}")

            return QuerySearchResponse(
                results=formatted_search_results,
                total_found=len(formatted_search_results),
                query_time_ms=query_time_ms
            )

    except HTTPException as http_exc: # Repassar HTTPExceptions
        raise http_exc
    except Exception as e:
        logging.exception(f"Unexpected error during R2R query processing")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing query: {str(e)}"
        )

# Modelos Pydantic para o novo endpoint de ingestão de chunks
class IngestChunkItem(BaseModel):
    content: str = Field(description="Conteúdo textual do chunk.")
    # Adicionar outros campos por chunk se necessário no futuro, ex: chunk_metadata

class IngestChunksRequest(BaseModel):
    document_id: Optional[str] = Field(default=None, description="ID opcional do documento ao qual os chunks pertencem.")
    chunks: List[IngestChunkItem] = Field(description="Lista de chunks a serem ingeridos.")
    metadata: Optional[Dict[str, Any]] = Field(default_else_factory=dict, description="Metadados a serem associados com os chunks ou o documento.")

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "document_id": "doc_123",
                "chunks": [
                    {"content": "Este é o primeiro chunk do documento."}, 
                    {"content": "Este é o segundo chunk."}
                ],
                "metadata": {"source": "gdrive_manual_chunks", "processed_by": "etl-worker-v2"}
            }
        }
    )

class IngestChunksResponse(BaseModel):
    success: bool = Field(description="Indica se a operação de ingestão foi aceita.")
    message: str = Field(description="Mensagem de status da operação.")
    document_id: Optional[str] = Field(default=None, description="ID do documento (se aplicável).")
    chunks_received: int = Field(description="Número de chunks recebidos na requisição.")
    error: Optional[str] = Field(default=None, description="Mensagem de erro, se houver.")
    raw_r2r_response: Optional[str] = Field(default=None, description="Resposta crua do cliente R2R para depuração.")


# Endpoint para ingestão de chunks (interno)
@app.post(
    "/internal/v1/ingest_chunks",
    response_model=IngestChunksResponse,
    summary="Ingere chunks pré-processados no R2R (uso interno)",
    description="Recebe uma lista de chunks de texto e os submete para ingestão no R2R. Requer autenticação.",
    responses={
        200: {"description": "Ingestão de chunks aceita"},
        401: {"model": ErrorResponse, "description": "Erro de Autenticação"},
        422: {"model": ErrorResponse, "description": "Erro de Validação da Requisição"},
        503: {"model": ErrorResponse, "description": "Serviço R2R indisponível"},
        500: {"model": ErrorResponse, "description": "Erro interno do servidor"}
    },
    tags=["Internal", "Ingestion"] # Adicionar tags para organização da documentação Swagger
)
async def ingest_chunks_endpoint(
    request_data: IngestChunksRequest,
    token_payload: dict = Depends(validate_token) # Proteger endpoint com autenticação
):
    """
    Endpoint interno para ingerir chunks de texto pré-processados.
    Utiliza o método assíncrono `ingest_chunks` do `R2RClientWrapper`.
    """
    if not r2r_client:
        logging.error("[Ingest Chunks] R2R client not available for ingestion.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="R2R service client is not available.",
            headers={"Retry-After": "60"}
        )

    user_id = token_payload.get('sub', 'unknown_service')
    logging.info(f"[Ingest Chunks] Received request from1 {user_id} to ingest {len(request_data.chunks)} chunks for doc_id: {request_data.document_id}.")

    # Extrair apenas o conteúdo dos chunks para o wrapper
    chunk_contents = [chunk.content for chunk in request_data.chunks]

    try:
        r2r_response = await r2r_client.ingest_chunks(
            chunks=chunk_contents,
            document_id=request_data.document_id,
            metadata=request_data.metadata
        )

        if r2r_response.get("success"):
            logging.info(f"[Ingest Chunks] R2R accepted {len(chunk_contents)} chunks for doc_id: {request_data.document_id}. R2R Message: {r2r_response.get('message')}")
            return IngestChunksResponse(
                success=True,
                message=r2r_response.get("message", "Chunks submitted to R2R successfully."),
                document_id=request_data.document_id, # Ou o que vier da resposta R2R
                chunks_received=len(request_data.chunks),
                raw_r2r_response=r2r_response.get("raw_response")
            )
        else:
            error_detail = r2r_response.get("error", "Unknown error during R2R chunk ingestion.")
            logging.error(f"[Ingest Chunks] R2R ingestion failed for doc_id: {request_data.document_id}. Error: {error_detail}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"R2R chunk ingestion failed: {error_detail}"
            )

    except HTTPException as http_exc: # Repassar HTTPExceptions (como 503 do R2R indisponível)
        raise http_exc
    except Exception as e:
        logging.exception(f"[Ingest Chunks] Unexpected error during chunk ingestion for doc_id: {request_data.document_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error processing chunk ingestion: {str(e)}"
        )

# Endpoint de health check
@app.get("/health", response_model=HealthCheckResponse, summary="Verifica a saúde da API")
async def health_check():
    """
    Verifica o status da API e suas dependências (Supabase e R2R Client).

    Retorna:
        HealthCheckResponse: Objeto Pydantic com o status geral e das dependências.
    """
    global supabase_client # <<< FIX: Moved global declaration to the top >>>

    # <<< FIX: Attempt re-initialization if None >>>
    if not supabase_client:
        logging.warning("[Health Check] supabase_client is None, attempting re-initialization...")
        supa_url = os.environ.get("SUPABASE_URL")
        supa_key = os.environ.get("SUPABASE_SERVICE_KEY")
        if supa_url and supa_key:
            try:
                supabase_client = create_client(supa_url, supa_key)
                logging.info("[Health Check] Re-initialization successful.")
            except Exception as e:
                logging.error(f"[Health Check] Error during re-initialization: {e}")
        else:
            logging.error("[Health Check] Env vars still missing during re-initialization attempt.")
    # <<< END FIX >>>

    health = {
        "status": "healthy",
        "timestamp": datetime.now(UTC).isoformat(),
        "dependencies": {
            "database": "healthy" if supabase_client else "unavailable",
            "r2r_client": True if r2r_client else "unavailable"
        }
    }
    
    if not r2r_client:
        health["status"] = "degraded"
        
    return health

# --- TEMPORARY DEBUG ENDPOINT REMOVED --- 
# @app.get("/list_indexed", include_in_schema=False)
# async def list_indexed_documents():
#     # ... (código removido)
# --- END REMOVED ENDPOINT ---

# Ponto de entrada para desenvolvimento local
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
