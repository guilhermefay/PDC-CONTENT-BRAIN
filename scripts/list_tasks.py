import json
import os
from collections import Counter

TASKS_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', 'tasks', 'tasks.json')

# --- Configuração de Formatação ---
ID_WIDTH = 6
STATUS_WIDTH = 12
PRIORITY_WIDTH = 10
TITLE_WIDTH = 70
SUBTASK_INDENT = "  "
SUBTASK_PREFIX = "└─ "
HEADER = "{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} Dependencies".format('ID', 'Status', 'Priority', 'Title', ID_WIDTH, STATUS_WIDTH, PRIORITY_WIDTH, TITLE_WIDTH)
SEPARATOR = "-" * (ID_WIDTH + STATUS_WIDTH + PRIORITY_WIDTH + TITLE_WIDTH + 15)

def load_tasks(filepath=TASKS_FILE_PATH):
    """Carrega e parseia o arquivo tasks.json."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print("Erro: Arquivo de tarefas não encontrado em '{}'".format(filepath))
        return None
    except json.JSONDecodeError:
        print("Erro: Falha ao parsear o JSON em '{}'. Verifique a formatação.".format(filepath))
        return None
    except Exception as e:
        print("Erro inesperado ao ler o arquivo de tarefas: {}".format(e))
        return None

def format_dependencies(dep_list):
    """Formata a lista de dependências."""
    return ', '.join(map(str, dep_list)) if dep_list else "-"

def format_title(title, width, indent="", prefix=""):
    """Trunca ou ajusta o título para a largura definida."""
    available_width = width - len(indent) - len(prefix)
    if len(title) > available_width:
        return indent + prefix + title[:available_width-3] + "..."
    return (indent + prefix + title).ljust(width)


def print_tasks(tasks_data):
    """Imprime as tarefas e subtarefas formatadas."""
    if not tasks_data or 'tasks' not in tasks_data:
        print("Nenhuma tarefa encontrada ou formato de dados inválido.")
        return

    print(HEADER)
    print(SEPARATOR)

    all_tasks = tasks_data.get('tasks', [])
    for task in sorted(all_tasks, key=lambda t: float(t.get('id', 0)) if str(t.get('id', 0)).replace('.','',1).isdigit() else 0):
        task_id_str = str(task.get('id', 'N/A'))
        status = task.get('status', 'N/A')[:STATUS_WIDTH]
        priority = task.get('priority', 'N/A')[:PRIORITY_WIDTH]
        title = task.get('title', 'N/A')
        deps = format_dependencies(task.get('dependencies', []))

        # Imprime a tarefa principal
        print("{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {}".format(
            task_id_str, status, priority, format_title(title, TITLE_WIDTH), deps,
            ID_WIDTH, STATUS_WIDTH, PRIORITY_WIDTH, TITLE_WIDTH
        ))

        # Imprime subtarefas, se existirem
        subtasks = task.get('subtasks', [])
        if subtasks:
            for subtask in sorted(subtasks, key=lambda s: float(s.get('id', 0)) if str(s.get('id', 0)).replace('.','',1).isdigit() else 0):
                sub_id = f"{task_id_str}.{subtask.get('id', 'N/A')}"
                sub_status = subtask.get('status', 'N/A')[:STATUS_WIDTH]
                sub_priority = '-'
                sub_title = subtask.get('title', 'N/A')
                sub_deps = format_dependencies(subtask.get('dependencies', []))

                print("{{:<{}}} {{:<{}}} {{:<{}}} {{:<{}}} {}".format(
                    sub_id, sub_status, sub_priority, format_title(sub_title, TITLE_WIDTH, indent=SUBTASK_INDENT, prefix=SUBTASK_PREFIX), sub_deps,
                    ID_WIDTH, STATUS_WIDTH, PRIORITY_WIDTH, TITLE_WIDTH
                ))
    print(SEPARATOR)


def print_summary(tasks_data):
    """Calcula e imprime o resumo do progresso das tarefas."""
    if not tasks_data or 'tasks' not in tasks_data:
        return

    status_counts = Counter()
    total_items = 0

    for task in tasks_data.get('tasks', []):
        total_items += 1
        status_counts[task.get('status', 'unknown')] += 1
        for subtask in task.get('subtasks', []):
            total_items += 1
            status_counts[subtask.get('status', 'unknown')] += 1

    if total_items == 0:
        print("Resumo: Nenhuma tarefa ou subtarefa para resumir.")
        return

    print("\nResumo do Progresso:")
    for status, count in status_counts.most_common():
        percentage = (count / total_items) * 100
        print("- {}: {} ({:.1f}%)".format(status.capitalize(), count, percentage))
    print("Total de Itens (Tarefas + Subtarefas): {}".format(total_items))


if __name__ == "__main__":
    tasks_data = load_tasks()
    if tasks_data:
        print_tasks(tasks_data)
        print_summary(tasks_data) 