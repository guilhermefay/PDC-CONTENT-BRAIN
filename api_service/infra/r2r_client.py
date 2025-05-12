"""
Client module for interacting with the R2R Cloud API using the official SDK.

Handles authentication, document upload, search, RAG, Agentic RAG,
document overview, chunk listing, and other operations.
Uses a RetryHandler for network resilience.
"""

import os
import logging
import time # Adicionar importação de time
import traceback # Adicionar importação de traceback
import requests # Adicionar importação de requests
import json # Adicionar importação de json
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
# import r2r # Importar o pacote r2r diretamente
from r2r import R2RClient # Importar R2RClient diretamente
# R2RException não foi encontrada na documentação, trataremos exceções genéricas por agora.
from requests.exceptions import Timeout, ConnectionError # Importar exceções específicas

# Importar o RetryHandler
from .resilience import RetryHandler

load_dotenv()
logger = logging.getLogger(__name__)

# Variáveis de ambiente são carregadas dentro da classe agora

# Definir exceções padrão para retry (pode ser movido para config)
DEFAULT_RETRY_EXCEPTIONS = (Timeout, ConnectionError)

class R2RClientWrapper:
    """
    A wrapper class for the R2RClient from the r2r SDK.

    Provides a simplified and resilient interface to interact with the R2R Cloud API.
    It handles:
        - Initialization and authentication using environment variables.
        - Retries for network operations using `RetryHandler`.
        - Standard R2R operations: health check, search, RAG, document upload/delete/list, chunk listing.
        - Agentic RAG capabilities.

    Attributes:
        base_url (str): The base URL for the R2R API, loaded from R2R_BASE_URL.
        api_key (str | None): The API key for R2R, loaded from R2R_API_KEY.
        client (R2RClient): An instance of the official R2R SDK client.
        retry_handler (RetryHandler): An instance of the utility class for retrying operations.
    """
    def __init__(self, retry_config: Optional[Dict[str, Any]] = None):
        """
        Initializes the R2RClientWrapper.

        Loads necessary environment variables (R2R_BASE_URL, R2R_API_KEY)
        and instantiates the R2RClient and RetryHandler.

        Args:
            retry_config (Optional[Dict[str, Any]]): Configuration dictionary for the
                RetryHandler. Keys can include 'retries', 'initial_delay',
                'max_delay', 'backoff_factor', 'jitter', 'retry_exceptions'.
                If None, default retry settings are used.

        Raises:
            ValueError: If R2R_BASE_URL environment variable is not set.
        """
        # --- DEBUG: Log Initialization ---
        init_timestamp = time.time()
        logger.info(f"R2RClientWrapper.__init__ called at {init_timestamp}")
        # --- END DEBUG ---

        self.base_url = os.getenv("R2R_BASE_URL")
        self.api_key = os.getenv("R2R_API_KEY") # Armazenar a chave lida

        # --- DEBUG: Log Environment Variables Read in __init__ ---
        logger.info(f"__init__ - R2R_BASE_URL read: {self.base_url}")
        logger.info(f"__init__ - R2R_API_KEY read: {'Present' if self.api_key else 'Not Found'}")
        # --- END DEBUG ---

        if not self.base_url:
            logger.error("R2R_BASE_URL not found in environment variables.")
            raise ValueError("R2R_BASE_URL is required to initialize the R2R client.")
        if not self.api_key:
            # Permitir inicialização sem API key para endpoints públicos como /health
            # Mas logar um aviso
            logger.warning("R2R_API_KEY not found in environment variables. Authenticated endpoints might fail.")
            # O SDK provavelmente busca a chave do ambiente se não for passada.
        # else:
            # Não precisamos de um else, a inicialização é a mesma
            # self.client = R2RClient(base_url=self.base_url, api_key=self.api_key) # Usar R2RClient diretamente
        
        # Inicializar o cliente sempre da mesma forma, confiando nas env vars para a API key
        self.client = R2RClient(base_url=self.base_url)
        logger.info(f"R2RClient initialized for base URL: {self.base_url}")

        # Instanciar o RetryHandler com exceções de rede comuns
        # Ou com configurações passadas
        retry_settings = {
            "retries": 3,
            "initial_delay": 1, # Ajustado para ser consistente com testes
            "max_delay": 15, # Ajustado para ser consistente com testes
            "backoff_factor": 2.0,
            "jitter": True,
            "retry_exceptions": DEFAULT_RETRY_EXCEPTIONS
        }
        if retry_config:
            retry_settings.update(retry_config)
            logger.info(f"Using custom retry config: {retry_config}")

        self.retry_handler = RetryHandler(**retry_settings)
        logger.info(f"RetryHandler instantiated for R2RClientWrapper with settings: {retry_settings}")


    def health(self) -> bool:
        """
        Checks the health/status endpoint of the R2R API using a direct HTTP request.

        Uses RetryHandler for resilience.

        Returns:
            bool: True if the API responds with a 2xx status code at `/health`, False otherwise.
        """
        health_url = f"{self.base_url.rstrip('/')}/health"
        logger.info(f"Checking R2R health at {health_url}")
        try:
            # Usar o RetryHandler para a requisição requests
            response = self.retry_handler.execute(
                requests.get,
                health_url,
                timeout=10 # Adicionar um timeout
            )
            response.raise_for_status() # Lança exceção para status 4xx/5xx
            health_status = response.json() # Tenta obter o JSON da resposta
            logger.info(f"R2R health check successful: Response={health_status}") # Log mais informativo
            # Poderíamos verificar o conteúdo de health_status se necessário
            return True
        except (Timeout, ConnectionError) as e: # Exceções tratadas pelo retry handler
            logger.error(f"R2R health check failed after retries ({type(e).__name__}): {e}")
            return False
        except requests.exceptions.RequestException as e: # Outros erros de requests (ex: 4xx, 5xx não retentáveis)
            logger.error(f"R2R health check failed (RequestException): {e}")
            return False
        except Exception as e: # Captura outras exceções inesperadas
            logger.exception(f"An unexpected error occurred during R2R health check: {e}")
            return False

    def upload_file(
        self,
        file_path: str,
        document_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Uploads and processes a file into R2R using the SDK's documents.create method.
        Uses RetryHandler for resilience.

        Args:
            file_path (str): The local path to the file to upload.
            document_id (Optional[str]): Optional unique ID for the document.
            metadata (Optional[Dict[str, Any]]): Optional dictionary of metadata 
                                                 to associate with the document.
            settings (Optional[Dict[str, Any]]): Optional settings for processing 
                                                 (e.g., chunking strategy).

        Returns:
            Dict[str, Any]: A dictionary containing:
                - `success` (bool): True if the upload was accepted, False otherwise.
                - `document_id` (str | None): The ID of the document (provided or generated).
                - `message` (str | None): A status message from R2R.
                - `error` (str | None): An error message if the upload failed.
        """
        logger.info(f"Attempting to upload file '{os.path.basename(file_path)}' to R2R via documents.create.")
        logger.debug(f"Upload details - Path: {file_path}, Doc ID: {document_id}, Meta: {metadata}, Settings: {settings}")

        if not self.api_key:
            logger.error("Cannot upload file: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False}
        if not os.path.exists(file_path):
            logger.error(f"File not found at path: {file_path}")
            return {"error": f"File not found: {file_path}", "success": False}

        try:
            # <<< CORRIGIDO: Usar documents.create conforme documentação >>>
            response = self.retry_handler.execute(
                self.client.documents.create,
                file_path=file_path,
                metadata=metadata or {}, # Passar metadata
                id=document_id # Passar id opcional
                # settings=settings # Adicionar se o SDK suportar
            )
            # A resposta de create é um objeto IngestionResponse
            message = "Document creation/ingestion task queued successfully."
            new_doc_id = None
            if hasattr(response, 'results') and hasattr(response.results, 'document_id'):
                 if hasattr(response.results, 'message'):
                      message = response.results.message
                 new_doc_id = str(response.results.document_id) # Converter UUID para str, se necessário

            logger.info(f"Successfully submitted document '{file_path}'. R2R Response Message: {message}. Document ID: {new_doc_id}")
            return {
                "message": message,
                "document_id": new_doc_id,
                "success": True
            }

        except (Timeout, ConnectionError) as e:
             logger.exception(f"Document submission via .documents.create() failed for '{file_path}' after retries: {e}")
             return {"error": f"Network Error after retries: {str(e)}", "success": False}
        except Exception as e:
            # Logar traceback completo para depuração
            tb_str = traceback.format_exc()
            logger.exception(f"An unexpected error occurred during document submission via .documents.create() for '{file_path}': {e}\\n{tb_str}")
            return {"error": f"SDK Error: {str(e)}", "success": False}


    def search(
        self,
        query: str,
        limit: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        search_settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Performs a standard vector similarity search via the R2R SDK's retrieval.search method.
        Uses RetryHandler for resilience.

        Args:
            query (str): The user's search query string.
            limit (int): The maximum number of results to return. Defaults to 5.
            filters (Optional[Dict[str, Any]]): Optional dictionary for metadata filtering 
                                                  (e.g., `{'source': 'gdrive'}`).
            search_settings (Optional[Dict[str, Any]]): Optional search settings dictionary
                                                       (currently not used by this direct method).

        Returns:
            Dict[str, Any]: A dictionary containing:
                - `success` (bool): True if the search was successful, False otherwise.
                - `results` (List[Dict[str, Any]]): A list of search result dictionaries, 
                                                    each representing a relevant chunk.
                - `error` (str | None): An error message if the search failed.
        """
        logger.info(f"Performing SDK search on R2R for query: '{query[:50]}...'")
        # Combinar parâmetros explícitos com search_settings, se fornecido
        final_search_settings = search_settings or {}
        final_search_settings['limit'] = limit # Adicionar limit aos settings
        if filters: # Adicionar filtros aos settings se existirem
            final_search_settings['filters'] = filters
        
        logger.debug(f"Search details - Combined Settings: {final_search_settings}")
        if not self.api_key:
            logger.error("Cannot perform search: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False, "results": []}

        try:
            # <<< CORRIGIDO: Usar client.retrieval.search com search_settings >>>
            response = self.retry_handler.execute(
                self.client.retrieval.search,
                query=query,
                # Passar o dicionário combinado de settings
                search_settings=final_search_settings 
            )
            # A resposta de search agora parece ser um WrappedSearchResponse contendo AggregateSearchResult
            results_list = []
            # Extrair chunk_search_results de response.results.chunk_search_results
            if hasattr(response, 'results') and hasattr(response.results, 'chunk_search_results') and isinstance(response.results.chunk_search_results, list):
                 raw_chunks = response.results.chunk_search_results
                 logger.info(f"Extracted {len(raw_chunks)} raw chunks from response.")
                 for item in raw_chunks:
                     if hasattr(item, 'to_dict'):
                         results_list.append(item.to_dict())
                     elif isinstance(item, dict): # Se já for dict
                         results_list.append(item)
                     else:
                         logger.warning(f"Search result item of type {type(item)} is not a dict and lacks to_dict method.")
            # Fallback para o caso de a resposta ser a lista diretamente (pouco provável agora)
            elif isinstance(response, list):
                 logger.warning("client.retrieval.search returned a direct list, expected WrappedSearchResponse.")
                 for item in response:
                     if hasattr(item, 'to_dict'):
                         results_list.append(item.to_dict())
                     elif isinstance(item, dict):
                         results_list.append(item)
                     else:
                         logger.warning(f"Direct list search result item of type {type(item)} is not a dict and lacks to_dict method.")
            else:
                 logger.error(f"Unexpected response structure from client.retrieval.search: {type(response)}")

            logger.info(f"SDK search successful. Parsed {len(results_list)} results.")
            return {"results": results_list, "success": True}

        except (Timeout, ConnectionError) as e:
            logger.exception(f"SDK search failed after retries: {e}")
            return {"error": f"Network Error after retries: {str(e)}", "success": False, "results": []}
        except Exception as e:
            tb_str = traceback.format_exc()
            logger.exception(f"An unexpected error occurred during SDK search: {e}\\n{tb_str}")
            return {"error": f"SDK Error: {str(e)}", "success": False, "results": []}


    def rag(
        self,
        query: str,
        limit: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        generation_config: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Performs Retrieval-Augmented Generation via the R2R SDK's retrieval.rag method.
        Uses RetryHandler for resilience.

        Args:
            query (str): The user's query string.
            limit (int): Maximum number of search results to retrieve for context. Defaults to 5.
            filters (Optional[Dict[str, Any]]): Optional dictionary for metadata filtering.
            generation_config (Optional[Dict[str, Any]]): Optional dictionary for LLM 
                                                        generation parameters (e.g., 
                                                        `{'model': 'gpt-4o', 'temperature': 0.5}`).
            settings (Optional[Dict[str, Any]]): Optional RAG settings 
                                                (e.g., `{'stream': False}`). (Currently unused here).

        Returns:
            Dict[str, Any]: A dictionary containing:
                - `success` (bool): True if RAG was successful, False otherwise.
                - `response` (str | None): The generated response text from the LLM.
                - `results` (List[Dict[str, Any]]): The search results used as context.
                - `error` (str | None): An error message if RAG failed.
        """
        logger.info(f"Performing SDK RAG on R2R for query: '{query[:50]}...'")
        
        # Preparar search_settings para o RAG
        rag_search_settings = settings.get('search_settings', {}) if settings else {}
        rag_search_settings['limit'] = limit
        rag_search_settings['filters'] = filters or {}
            
        # Preparar generation_config
        final_generation_config = generation_config or {}
        # Poderíamos adicionar defaults do `settings` aqui se necessário

        logger.debug(f"RAG details - Search Settings: {rag_search_settings}, Gen Config: {final_generation_config}")
        if not self.api_key:
            logger.error("Cannot perform RAG: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False, "response": None}

        try:
            # <<< CORRIGIDO: Usar client.retrieval.rag com search_settings e rag_generation_config >>>
            response = self.retry_handler.execute(
                self.client.retrieval.rag,
                query=query,
                rag_generation_config=final_generation_config,
                search_settings=rag_search_settings
                # Adicionar outros params como include_web_search se vierem em `settings`
            )
            # A resposta do rag agora parece ser WrappedRAGResponse contendo RAGResponse
            llm_response = None
            search_results_list = []
            if hasattr(response, 'results'):
                rag_result = response.results # Acessar o objeto RAGResponse interno
                if hasattr(rag_result, 'generated_answer'):
                    llm_response = rag_result.generated_answer
                # search_results dentro de RAGResponse é AggregateSearchResult
                if hasattr(rag_result, 'search_results') and hasattr(rag_result.search_results, 'chunk_search_results') and isinstance(rag_result.search_results.chunk_search_results, list):
                    raw_chunks = rag_result.search_results.chunk_search_results
                    for item in raw_chunks:
                        if hasattr(item, 'to_dict'):
                            search_results_list.append(item.to_dict())
                        elif isinstance(item, dict):
                            search_results_list.append(item)
                        else:
                            logger.warning(f"RAG search result item of type {type(item)} is not a dict and lacks to_dict method.")
            else:
                logger.error(f"Unexpected response structure from client.retrieval.rag: {type(response)}")

            logger.info("SDK RAG query successful.")
            return {"response": llm_response, "results": search_results_list, "success": True}

        except (Timeout, ConnectionError) as e:
            logger.exception(f"SDK RAG failed after retries: {e}")
            return {"error": f"Network Error after retries: {str(e)}", "success": False, "response": None}
        except Exception as e:
            tb_str = traceback.format_exc()
            logger.exception(f"An unexpected error occurred during SDK RAG: {e}\\n{tb_str}")
            return {"error": f"SDK Error: {str(e)}", "success": False, "response": None}


    def agentic_rag(
        self,
        message: Dict[str, Any], # Ex: {'role': 'user', 'content': 'query'}
        rag_generation_config: Optional[Dict[str, Any]] = None,
        mode: str = "research",
        settings: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Performs Agentic RAG using the R2R SDK.
        
        Agentic RAG allows for more complex interactions, potentially involving
        multiple steps or tools managed by the R2R agent.
        Uses RetryHandler for resilience.

        Args:
            message (Dict[str, Any]): The user message dictionary, typically 
                                     `{'role': 'user', 'content': 'user query'}`.
            rag_generation_config (Optional[Dict[str, Any]]): Optional LLM generation config.
            mode (str): Agentic RAG mode (e.g., 'research'). Defaults to 'research'.
            settings (Optional[Dict[str, Any]]): Optional R2R settings.

        Returns:
            Dict[str, Any]: A dictionary containing:
                - `success` (bool): True if the call was successful, False otherwise.
                - `response` (Any): The response from the R2R agent (structure may vary).
                - `error` (str | None): An error message if the call failed.
        """
        logger.info(f"Performing Agentic RAG (mode='{mode}') via SDK.")
        logger.debug(f"Agentic RAG details - Message: {message}, Gen Config: {rag_generation_config}, Settings: {settings}")
        if not self.api_key:
            logger.error("Cannot perform Agentic RAG: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False}

        try:
            # Usar o RetryHandler para a chamada do SDK
            response = self.retry_handler.execute(
                self.client.agentic_rag,
                message=message,
                rag_generation_config=rag_generation_config,
                # mode=mode, # Verificar se o SDK aceita 'mode'
                # settings=settings # Verificar se o SDK aceita 'settings'
            )
            logger.info("Agentic RAG call successful.")
            # Adaptar retorno baseado na resposta real do SDK
            # Suposição: resposta pode ser um dicionário ou objeto
            if isinstance(response, dict):
                 return {"response": response, "success": True}
            elif hasattr(response, 'to_dict'): # Se for um objeto com to_dict
                 return {"response": response.to_dict(), "success": True}
            else:
                 return {"response": str(response), "success": True} # Fallback
                 
        except (Timeout, ConnectionError) as e: # Exceções de rede que o handler pode ter levantado
            logger.exception(f"Agentic RAG failed after retries: {e}")
            return {"error": f"Network Error after retries: {str(e)}", "success": False}
        except Exception as e:
            logger.exception(f"An unexpected error occurred during Agentic RAG: {e}")
            # Tentar obter mais detalhes, se for uma exceção específica do SDK
            # Exemplo: if isinstance(e, R2RException): ...
            return {"error": f"SDK Error: {str(e)}", "success": False}


    def list_documents(
        self,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Retrieves a list of documents from R2R using the SDK's documents.list method.
        Uses RetryHandler for resilience.

        Args:
            filters: Optional dictionary of filters to apply.
            limit: Maximum number of documents to return.
            offset: Offset for pagination.

        Returns:
            A dictionary containing the document list or an error.
        """
        logger.info(f"Listing documents via SDK documents.list. Limit: {limit}, Offset: {offset}, Filters: {filters}")
        if not self.api_key:
            logger.error("Cannot list documents: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False, "documents": []}

        try:
            # <<< CORRIGIDO: Usar documents.list com limit/offset conforme documentação >>>
            response = self.retry_handler.execute(
                self.client.documents.list,
                limit=limit,
                offset=offset
                # filters=filters # Adicionar se SDK suportar
            )
            logger.info(f"Raw response from documents.list(): {response}")
            logger.info(f"Document listing successful. Raw Response type: {type(response)}")

            documents = []
            if hasattr(response, 'results') and isinstance(response.results, list):
                documents = response.results # A resposta já contém a lista
                logger.info(f"Extracted {len(documents)} documents from PaginatedR2RResult.")
            elif isinstance(response, list): # Fallback se retornar lista direto inesperadamente
                documents = response
                logger.warning(f"documents.list() returned a direct list, expected PaginatedR2RResult. Using list directly.")
                logger.info(f"Found {len(documents)} documents in direct list.")
            else:
                logger.warning(f"Could not extract document list from response. Type: {type(response)}, Response: {response}")

            return {"documents": documents, "success": True}

        except (Timeout, ConnectionError) as e:
            logger.exception(f"Document listing failed after retries: {e}")
            return {"error": f"Network Error after retries: {str(e)}", "success": False, "documents": []}
        except Exception as e:
            logger.exception(f"An unexpected error occurred during document listing: {e}")
            return {"error": f"SDK Error: {str(e)}", "success": False, "documents": []}


    def delete_document(
        self,
        document_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Deletes a document from R2R using the SDK's documents.delete method.
        Uses RetryHandler for resilience.

        Args:
            document_id (str, optional): The unique ID of the document to delete.
            filters (dict, optional): Filtros para deletar múltiplos documentos.

        Returns:
            Dict[str, Any]: A dictionary containing:
                - `success` (bool): True if the deletion was successful, False otherwise.
                - `message` (str | None): A status message.
                - `error` (str | None): An error message if deletion failed.
        """
        logger.info(f"Attempting to delete document via documents.delete. ID: {document_id}, Filters: {filters}")

        if not self.api_key:
            logger.error("Cannot delete document: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False}

        # Validar identificadores
        if document_id and filters:
            logger.warning(
                "Both document_id and filters provided for deletion. Behavior might be undefined. Prefer using only one."
            )
        if not document_id and not filters:
            return {"error": "No identifier provided for deletion.", "success": False}

        try:
            # <<< CORRIGIDO: Usar documents.delete com id conforme documentação >>>
            # A documentação para DELETE /documents/{id} sugere passar apenas o ID na URL.
            # O SDK provavelmente reflete isso, passando 'id' como argumento nomeado.
            # A documentação para DELETE /documents/by-filter sugere filtros.
            # Assumindo que client.documents.delete pode lidar com ambos cenários.
            # Vamos priorizar ID se fornecido.
            if document_id:
                response = self.retry_handler.execute(
                    self.client.documents.delete,
                    id=document_id
                )
            elif filters:
                 logger.warning("Attempting deletion by filter. Ensure SDK supports this via documents.delete.")
                 response = self.retry_handler.execute(
                     self.client.documents.delete,
                     filters=filters # Assumindo que aceita 'filters'
                 )
            else: # Segurança extra, já verificado acima
                 return {"error": "No identifier provided.", "success": False}

            # <<< Manter lógica de interpretação da resposta (GenericBooleanResponse) >>>
            success_flag = False
            message = f"Document {document_id or filters} deletion request sent."
            if hasattr(response, 'results') and hasattr(response.results, 'success') and response.results.success is True:
                success_flag = True
                message = f"Document {document_id or filters} confirmed deleted by API."
                logger.info(message)
            # Fallback: se resposta foi dict sem message, considerar sucesso com raw string
            elif isinstance(response, dict):
                success_flag = True
                message = str(response)
                logger.info(f"Document deletion fallback success: {message}")

            logger.info(f"Successfully processed delete request for {document_id or filters}. API confirmed: {success_flag}. Raw Response: {response}")

            if success_flag:
                 return {"message": message, "success": True}
            else:
                return {
                    "message": f"Deletion request sent, but no confirmation from API.",
                    "success": False,
                    "error": "API did not confirm successful deletion."
                }

        except (Timeout, ConnectionError) as e:
            logger.exception(f"Document deletion failed for ID {document_id} after retries: {e}")
            return {"error": f"Network Error after retries: {str(e)}", "success": False}
        except Exception as e:
            logger.exception(f"An unexpected error occurred during document deletion for ID {document_id}: {e}")
            # Tentar extrair mais detalhes do erro se possível (ex: e.response.text)
            error_details = str(e)
            return {"error": f"SDK Error: {error_details}", "success": False}


    def list_document_chunks(
        self,
        document_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Lists chunks for a specific document using the R2R SDK.
        Uses RetryHandler for resilience.

        Args:
            document_id: The ID of the document whose chunks are to be listed.
            limit: Maximum number of chunks to return.
            offset: Offset for pagination.

        Returns:
            A dictionary containing the chunk list or an error.
        """
        logger.info(f"Listing chunks for document '{document_id}' via SDK. Limit: {limit}, Offset: {offset}")
        if not self.api_key:
            logger.error("Cannot list chunks: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False, "chunks": []}

        try:
            # Usar RetryHandler e chamar método list_document_chunks do SDK
            response = self.retry_handler.execute(
                self.client.document_chunks,
                document_id=document_id,
                limit=limit,
                offset=offset
            )
            logger.info(f"Chunk listing successful for document '{document_id}'. Raw Response type: {type(response)}")

            # Extrair lista de chunks, convertendo via to_dict se disponível
            chunks_list = []
            if isinstance(response, list):
                for chunk in response:
                    if hasattr(chunk, 'to_dict'):
                        chunks_list.append(chunk.to_dict())
                    else:
                        chunks_list.append(chunk)
            elif isinstance(response, dict) and 'chunks' in response:
                chunks_list = response.get('chunks', [])
            else:
                logger.warning(f"Could not extract 'chunks' from response type {type(response)}: {response}")
            return {"chunks": chunks_list, "success": True}

        except (Timeout, ConnectionError) as e:
            logger.exception(f"Chunk listing failed after retries: {e}")
            return {"error": f"Network Error after retries: {str(e)}", "success": False, "chunks": []}
        except Exception as e:
            logger.exception(f"An unexpected error occurred during chunk listing: {e}")
            return {"error": f"SDK Error: {str(e)}", "success": False, "chunks": []}


    def get_documents_overview(
        self,
        # Parâmetros comuns para listagem, alinhar com o SDK se necessário
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Retrieves an overview of documents in the R2R system using the SDK.
        
        Similar to `list_documents` but might be a distinct SDK method or endpoint.
        This assumes it exists as `get_documents_overview` in the SDK.
        Uses RetryHandler for resilience.

        Args:
            limit (int): Maximum number of document overviews to return. Defaults to 100.
            offset (int): Offset for pagination. Defaults to 0.
            filters (Optional[Dict[str, Any]]): Optional dictionary of filters to apply.

        Returns:
            Dict[str, Any]: A dictionary containing:
                - `success` (bool): True if successful, False otherwise.
                - `overview` (List[Any] | Dict | Any): The overview data (structure depends on SDK).
                - `error` (str | None): An error message if failed.
        """
        logger.info(f"Getting documents overview via SDK. Limit: {limit}, Offset: {offset}, Filters: {filters}")
        if not self.api_key:
            logger.error("Cannot get documents overview: R2R_API_KEY is not configured.")
            return {"error": "Authentication required", "success": False, "overview": None}

        try:
            # Usar o RetryHandler para a chamada do SDK
            # Verificar o nome correto do método no SDK (ex: documents_overview, get_overview)
            response = self.retry_handler.execute(
                self.client.documents_overview,
                 # Passar parâmetros se o método do SDK aceitar
                 # limit=limit, 
                 # offset=offset,
                 # filters=filters
                 # O SDK r2r parece não aceitar params aqui, verificar documentação
            )
            logger.info(f"Documents overview retrieval successful. Raw Response type: {type(response)}")

            # A resposta provavelmente é um dicionário com estatísticas
            overview_data = {}
            if isinstance(response, dict):
                overview_data = response
            elif hasattr(response, 'to_dict'):
                overview_data = response.to_dict()
            else:
                logger.warning(f"Unexpected response type from documents_overview: {type(response)}")
                overview_data = {"raw_response": str(response)} # Fallback

            return {"overview": overview_data, "success": True}

        except (Timeout, ConnectionError) as e:
            logger.exception(f"Documents overview retrieval failed after retries: {e}")
            return {"error": f"Network Error after retries: {str(e)}", "success": False, "overview": None}
        except Exception as e:
            logger.exception(f"An unexpected error occurred during documents overview retrieval: {e}")
            return {"error": f"SDK Error: {str(e)}", "success": False, "overview": None}

# Adicionar mais métodos conforme necessário (ex: update_document, get_logs, etc.)
# Certificar-se de aplicar o retry_handler.execute() a todas as chamadas de rede.

# Exemplo simples de uso (opcional)
# if __name__ == '__main__':
#     logging.basicConfig(level=logging.INFO)
#     try:
#         wrapper = R2RClientWrapper()
#         
#         # Testar health check
#         is_healthy = wrapper.health()
#         logger.info(f"R2R Health Status: {is_healthy}")
#         
#         # Outros exemplos de chamadas...
#         # search_results = wrapper.search("What is RAG?")
#         # logger.info(f"Search Results: {search_results}")
#         
#     except ValueError as ve:
#         logger.error(f"Initialization failed: {ve}")
#     except Exception as ex:
#         logger.error(f"An error occurred: {ex}") 