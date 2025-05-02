"""
Smoke test script to verify connection and basic authentication with the R2R Cloud API.
"""

import sys
import os

# Adiciona o diretório raiz do projeto ao sys.path
# Isso garante que 'infra' possa ser encontrado
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import logging
from dotenv import load_dotenv

# Import the new wrapper class
try:
    from infra.r2r_client import R2RClientWrapper
except ImportError:
    print("Error: Could not import R2RClientWrapper from infra.r2r_client. Make sure the script is run from the project root or PYTHONPATH is set.")
    exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def run_connection_test():
    """Runs health check and a simple search test using R2RClientWrapper."""
    logger.info("Starting R2R connection test...")

    # Instantiate the client wrapper
    try:
        client = R2RClientWrapper()
    except ValueError as e:
        logger.error(f"Failed to initialize R2RClientWrapper: {e}")
        logger.error("Ensure R2R_BASE_URL environment variable is set.")
        logger.info("R2R connection test finished with initialization error.")
        return # Cannot continue without the client

    # # 1. Test Health Endpoint
    # logger.info("--- Testing Health Endpoint ---")
    # is_healthy = client.health() # Use the wrapper method
    # if is_healthy:
    #     logger.info("Health check PASSED.")
    # else:
    #     logger.warning("Health check FAILED. Check R2R base URL and API status.")
        # Decide if we should stop here or try authenticated requests anyway
        # The wrapper now handles lack of API key, so let's continue

    # 2. Test Basic Authenticated Search
    logger.info("--- Testing Basic Search (Authentication) ---")
    test_query = "test connection"
    try:
        # Use the wrapper's search method
        search_result = client.search(query=test_query, limit=1)

        # Check the success flag returned by the wrapper
        if search_result.get("success"):
            # Even an empty result list means the API call likely succeeded
            logger.info("Authenticated search test PASSED (API call successful).")
            logger.debug(f"Search results: {search_result.get('results')}")
        else:
            error_message = search_result.get("error", "Unknown error")
            logger.error(f"Authenticated search FAILED: {error_message}")
            if "Authentication required" in error_message:
                 logger.error("Check if R2R_API_KEY environment variable is set correctly.")
            else:
                 logger.error("Check R2R_API_KEY, R2R base URL, and API logs if available.")

    except Exception as e:
        # Catch any unexpected errors during the search call itself
        logger.error(f"Exception during authenticated search test call: {e}", exc_info=True)

    logger.info("R2R connection test finished.")

if __name__ == "__main__":
    run_connection_test() 