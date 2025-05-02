# Marca o diretório 'agents' como um pacote Python
from .base import BaseAgent # Exporta a classe base
from .annotator_agent import AnnotatorAgent
# Adicionar imports para os novos agentes se necessário para descoberta automática
# from .copywriting.carrossel_agents import ResearcherAgent, WriterAgent, EditorAgent