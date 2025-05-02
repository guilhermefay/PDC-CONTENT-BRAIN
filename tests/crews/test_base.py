import pytest
from abc import ABC
from typing import List, Dict, Any

from crews.base import BaseCrew

# --- Testes para a classe BaseCrew Abstrata ---

def test_base_crew_is_abstract():
    """Verifica que BaseCrew é abstrata e não pode ser instanciada diretamente."""
    assert issubclass(BaseCrew, ABC) # Garante que é uma classe abstrata
    with pytest.raises(TypeError, match="Can't instantiate abstract class BaseCrew"):
        BaseCrew() # Tentar instanciar deve falhar

# --- Criar uma implementação mínima para testes ---

class MinimalCrew(BaseCrew):
    """Implementação mínima de BaseCrew para fins de teste."""
    def _create_agents(self) -> List[Any]:
        # print("MinimalCrew._create_agents called") # Debug
        return ["agent1", "agent2"] # Retornar algo simples

    def _create_tasks(self) -> List[Any]:
        # print("MinimalCrew._create_tasks called") # Debug
        return ["task1"] # Retornar algo simples

    def run(self, inputs: Dict[str, Any]) -> Any:
        # print(f"MinimalCrew.run called with inputs: {inputs}") # Debug
        # Simplesmente retornar os agentes e tarefas criados para verificação
        return {"agents": self.agents, "tasks": self.tasks, "inputs": inputs}

# --- Testes para a implementação Mínima ---

def test_minimal_crew_instantiation():
    """Verifica se uma implementação mínima pode ser instanciada."""
    try:
        crew = MinimalCrew()
        assert isinstance(crew, BaseCrew)
        assert crew.agents == ["agent1", "agent2"]
        assert crew.tasks == ["task1"]
    except Exception as e:
        pytest.fail(f"MinimalCrew instantiation failed: {e}")

def test_minimal_crew_instantiation_with_config():
    """Verifica se a config é armazenada corretamente."""
    test_config = {"key": "value", "setting": 123}
    crew = MinimalCrew(config=test_config)
    assert crew.config == test_config
    # Verificar se agents e tasks ainda são criados
    assert crew.agents == ["agent1", "agent2"]
    assert crew.tasks == ["task1"]

def test_minimal_crew_instantiation_no_config():
    """Verifica se a config é um dict vazio por default."""
    crew = MinimalCrew() # Sem passar config
    assert crew.config == {}
    assert crew.agents == ["agent1", "agent2"]
    assert crew.tasks == ["task1"]

def test_minimal_crew_run_method():
    """Testa o método run da implementação mínima."""
    crew = MinimalCrew()
    inputs = {"input_key": "input_value"}
    result = crew.run(inputs)
    
    assert isinstance(result, dict)
    assert result["agents"] == ["agent1", "agent2"]
    assert result["tasks"] == ["task1"]
    assert result["inputs"] == inputs 