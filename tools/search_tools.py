# tools/search_tools.py
import logging
from crewai.tools import BaseTool
# Importar nosso wrapper R2R
from infra.r2r_client import R2RClientWrapper 
from typing import Type, Any
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# Cache singleton para o cliente R2R
r2r_client_instance = None

def get_r2r_client():
    global r2r_client_instance
    if r2r_client_instance is None:
        logger.info("Initializing R2RClientWrapper for search tools...")
        try:
            r2r_client_instance = R2RClientWrapper()
            # Fazer um health check rápido
            if not r2r_client_instance.health():
                logger.warning("R2R health check failed during tool initialization.")
                # Poderia retornar None ou levantar erro, mas vamos permitir continuar por enquanto
        except Exception as e:
            logger.exception(f"Failed to initialize R2RClientWrapper: {e}")
            # Retornar None ou levantar erro
            r2r_client_instance = None # Garantir que fique None se falhar
    return r2r_client_instance

class R2RSearchInput(BaseModel):
    """Input schema for R2RSearchTool."""
    query: str = Field(description="The search query string.")
    limit: int = Field(default=5, description="Maximum number of results to return.")
    # Adicionar filtros se necessário depois

class R2RSearchTool(BaseTool):
    name: str = "R2R Knowledge Base Search"
    description: str = "Searches the R2R knowledge base (vector store) for relevant text chunks based on a query. Use this to find specific information, examples, or context stored internally."
    args_schema: Type[BaseModel] = R2RSearchInput
    # r2r_client: R2RClientWrapper = None # Instanciar no __init__ ou via get

    def _run(self, query: str, limit: int = 5) -> str:
        """Use the tool."""
        logger.info(f"R2RSearchTool executing with query: '{query}', limit: {limit}")
        r2r_client = get_r2r_client()
        if not r2r_client:
            logger.error("R2RSearchTool cannot execute: R2R client not initialized.")
            return "Error: R2R client is not available."
            
        try:
            search_results = r2r_client.search(query=query, limit=limit)
            if search_results.get("success") and search_results.get("results"):
                # Formatar resultados para o LLM
                formatted_results = "\n".join([
                    f"- Chunk ID: {res.get('id', 'N/A')}\n  Document ID: {res.get('document_id', 'N/A')}\n  Content: {res.get('text', '')[:200]}...\n  Score: {res.get('score', 0.0):.4f}" 
                    for res in search_results["results"]
                ])
                logger.info(f"R2RSearchTool found {len(search_results['results'])} results.")
                return f"Found relevant chunks:\n{formatted_results}"
            elif search_results.get("success"):
                logger.info("R2RSearchTool executed successfully but found no results.")
                return "No relevant chunks found in the knowledge base for this query."
            else:
                error_msg = search_results.get("error", "Unknown error during R2R search.")
                logger.error(f"R2RSearchTool failed: {error_msg}")
                return f"Error searching R2R: {error_msg}"
        except Exception as e:
            logger.exception(f"Exception occurred during R2RSearchTool execution: {e}")
            return f"Error during R2R search execution: {e}"

# TODO: Adicionar WebSearchTool (ex: DuckDuckGoSearchRun from crewai_tools.tools) 