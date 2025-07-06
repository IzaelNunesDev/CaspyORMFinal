"""
CaspyORM CLI - Ferramenta de linha de comando para interagir com modelos CaspyORM.
"""

import typer
import asyncio
import os
import importlib
import sys
from typing import Optional, List
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Prompt, Confirm

from caspyorm import connection, Model

# --- Configuração ---
app = typer.Typer(
    help="[bold blue]CaspyORM CLI[/bold blue] - Uma CLI poderosa para interagir com seus modelos CaspyORM.",
    add_completion=False,
    rich_markup_mode="rich"
)
console = Console()

# Constantes
CLI_VERSION = "0.1.2"

def get_config():
    """Obtém configuração do CLI."""
    return {
        'hosts': os.getenv("CASPY_HOSTS", "localhost").split(','),
        'keyspace': os.getenv("CASPY_KEYSPACE", "caspyorm_demo"),
        'port': int(os.getenv("CASPY_PORT", "9042")),
        'models_path': os.getenv("CASPY_MODELS_PATH", "models")
    }

def show_models_path_error(module_path: str, error: str):
    """Mostra erro padronizado para problemas com CASPY_MODELS_PATH."""
    console.print(f"[bold red]Erro:[/bold red] Não foi possível importar o módulo '{module_path}': {error}")
    console.print(f"\n[bold]Dica:[/bold] Configure a variável de ambiente CASPY_MODELS_PATH")
    console.print(f"Exemplo: export CASPY_MODELS_PATH='meu_projeto.models'")
    console.print(f"Atual: CASPY_MODELS_PATH='{module_path}'")

async def safe_disconnect():
    """Desconecta do Cassandra de forma segura."""
    try:
        await connection.disconnect_async()
    except:
        pass

def find_model_class(model_name: str) -> type[Model]:
    """Descobre e importa a classe do modelo pelo nome."""
    config = get_config()
    module_path = config['models_path']
    
    try:
        module = importlib.import_module(module_path)
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) and 
                issubclass(attr, Model) and 
                attr != Model and
                attr.__name__.lower() == model_name.lower()):
                return attr
    except (ImportError, AttributeError) as e:
        show_models_path_error(module_path, str(e))
        raise typer.Exit(1)

    console.print(f"[bold red]Erro:[/bold red] Modelo '{model_name}' não encontrado em '{module_path}'.")
    console.print(f"\n[bold]Dica:[/bold] Use 'caspy models' para ver os modelos disponíveis")
    raise typer.Exit(1)

def parse_filters(filters: List[str]) -> dict:
    """Converte filtros da linha de comando em dicionário."""
    result = {}
    for filter_str in filters:
        if '=' in filter_str:
            key, value = filter_str.split('=', 1)
            # Converter tipos especiais
            if value.lower() == 'true':
                result[key] = True
            elif value.lower() == 'false':
                result[key] = False
            elif value.isdigit():
                result[key] = int(value)
            elif value.replace('.', '').replace('-', '').isdigit():
                try:
                    result[key] = float(value)
                except ValueError:
                    result[key] = value
            else:
                # Tentar converter para UUID se o campo terminar com '_id'
                if key.endswith('_id') and len(value) == 36 and '-' in value:
                    try:
                        import uuid
                        result[key] = uuid.UUID(value)
                    except ValueError:
                        result[key] = value
                else:
                    result[key] = value
    return result

async def run_query(model_name: str, command: str, filters: list[str], limit: Optional[int] = None, force: bool = False, keyspace: Optional[str] = None):
    """Executa uma query no banco de dados."""
    config = get_config()
    target_keyspace = keyspace or config['keyspace']
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Conectando ao Cassandra (keyspace: {target_keyspace})...", total=None)
            
            await connection.connect_async(contact_points=config['hosts'], keyspace=target_keyspace)
            progress.update(task, description="Conectado! Buscando modelo...")
            
            ModelClass = find_model_class(model_name)
            filter_dict = parse_filters(filters)
            
            progress.update(task, description=f"Executando '{command}' no modelo '{ModelClass.__name__}'...")
            
            # Executar comando
            if command == "get":
                result = await ModelClass.get_async(**filter_dict)
                if result:
                    console.print_json(result.model_dump_json(indent=2))
                else:
                    console.print("[yellow]Nenhum objeto encontrado.[/yellow]")
                    
            elif command == "filter":
                queryset = ModelClass.filter(**filter_dict)
                if limit:
                    queryset = queryset.limit(limit)
                
                results = await queryset.all_async()
                if not results:
                    console.print("[yellow]Nenhum objeto encontrado.[/yellow]")
                    return

                # Criar tabela com resultados
                table = Table(title=f"Resultados para {ModelClass.__name__}")
                if results:
                    headers = list(results[0].model_fields.keys())
                    for header in headers:
                        table.add_column(header, justify="left")
                    
                    for item in results:
                        table.add_row(*(str(getattr(item, h)) for h in headers))
                
                console.print(table)
                
            elif command == "count":
                count = await ModelClass.filter(**filter_dict).count_async()
                console.print(f"[bold green]Total:[/bold green] {count} registros")
                
            elif command == "exists":
                exists = await ModelClass.filter(**filter_dict).exists_async()
                status = "[bold green]Sim[/bold green]" if exists else "[bold red]Não[/bold red]"
                console.print(f"Existe: {status}")
                
            elif command == "delete":
                if not filter_dict:
                    console.print("[bold red]Erro:[/bold red] Filtros são obrigatórios para delete.")
                    return
                
                # Pular confirmação se force=True
                if force or Confirm.ask(f"Tem certeza que deseja deletar registros com filtros {filter_dict}?"):
                    count = await ModelClass.filter(**filter_dict).delete_async()
                    console.print(f"[bold green]Deletados:[/bold green] {count} registros")
                else:
                    console.print("[yellow]Operação cancelada.[/yellow]")
                    
            else:
                console.print(f"[bold red]Erro:[/bold red] Comando '{command}' não reconhecido.")
                
    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg.lower():
            console.print(f"[bold red]Erro:[/bold red] Tabela não encontrada no keyspace '{target_keyspace}'")
            console.print(f"[bold]Solução:[/bold] Use --keyspace para especificar o keyspace correto")
            console.print(f"Exemplo: caspy query {model_name} {command} --keyspace seu_keyspace")
        else:
            console.print(f"[bold red]Erro:[/bold red] {error_msg}")
        raise typer.Exit(1)
    finally:
        await safe_disconnect()

@app.command(help="Busca ou filtra objetos no banco de dados.")
def query(
    model_name: str = typer.Argument(..., help="Nome do modelo (ex: 'user', 'post')."),
    command: str = typer.Argument(..., help="Comando a ser executado ('get', 'filter', 'count', 'exists', 'delete')."),
    filters: List[str] = typer.Option(None, "--filter", "-f", help="Filtros no formato 'campo=valor'."),
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limitar número de resultados."),
    force: bool = typer.Option(False, "--force", "-F", help="Forçar a exclusão sem confirmação."),
    keyspace: Optional[str] = typer.Option(None, "--keyspace", "-k", help="Keyspace a ser usado (sobrescreve CASPY_KEYSPACE).")
):
    """
    Ponto de entrada síncrono que chama a lógica assíncrona.
    
    Exemplos:
    - caspy query user get --filter "email=joao@email.com"
    - caspy query post filter --filter "author_id=uuid-do-autor" --limit 10 --keyspace biblioteca
    - caspy query user count --filter "is_active=true"
    """
    asyncio.run(run_query(model_name, command, filters or [], limit, force, keyspace))

@app.command(help="Lista todos os modelos disponíveis.")
def models():
    """Lista todos os modelos disponíveis no módulo configurado."""
    config = get_config()
    module_path = config['models_path']
    
    try:
        module = importlib.import_module(module_path)
        model_classes = []
        
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, Model) and attr != Model:
                model_classes.append(attr)
        
        if not model_classes:
            console.print("[yellow]Nenhum modelo encontrado.[/yellow]")
            console.print(f"\n[bold]Dica:[/bold] Verifique se o módulo '{module_path}' contém modelos CaspyORM")
            return
        
        table = Table(title=f"Modelos disponíveis em '{module_path}'")
        table.add_column("Nome", style="cyan")
        table.add_column("Tabela", style="green")
        table.add_column("Campos", style="yellow")
        
        for model_cls in model_classes:
            fields = list(model_cls.model_fields.keys())
            table.add_row(
                model_cls.__name__,
                model_cls.__table_name__,
                ", ".join(fields[:5]) + ("..." if len(fields) > 5 else "")
            )
        
        console.print(table)
        
    except ImportError as e:
        show_models_path_error(module_path, str(e))

@app.command(help="Conecta ao Cassandra e testa a conexão.")
def connect(
    keyspace: Optional[str] = typer.Option(None, "--keyspace", "-k", help="Keyspace para testar (sobrescreve CASPY_KEYSPACE).")
):
    """Testa a conexão com o Cassandra."""
    config = get_config()
    target_keyspace = keyspace or config['keyspace']
    
    async def test_connection():
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task(f"Testando conexão (keyspace: {target_keyspace})...", total=None)
                
                await connection.connect_async(contact_points=config['hosts'], keyspace=target_keyspace)
                progress.update(task, description="Conectado! Testando query...")
                
                # Testar uma query simples
                await connection.execute_async("SELECT release_version FROM system.local")
                
                progress.update(task, description="✅ Conexão bem-sucedida!")
                
            console.print(f"[bold green]✅ Conexão com Cassandra estabelecida com sucesso![/bold green]")
            console.print(f"[bold]Keyspace:[/bold] {target_keyspace}")
            console.print(f"[bold]Hosts:[/bold] {', '.join(config['hosts'])}")
            
        except Exception as e:
            console.print(f"[bold red]❌ Erro na conexão:[/bold red] {e}")
            console.print(f"\n[bold]Dica:[/bold] Verifique se o Cassandra está rodando e acessível")
            console.print(f"Configuração atual: hosts={config['hosts']}, keyspace={target_keyspace}")
            raise typer.Exit(1)
        finally:
            await safe_disconnect()
    
    asyncio.run(test_connection())

@app.command(help="Mostra informações sobre a CLI.")
def info():
    """Mostra informações sobre a CLI e configuração."""
    config = get_config()
    
    info_panel = Panel(
        Text.assemble(
            ("CaspyORM CLI", "bold blue"),
            "\n\n",
            ("Versão: ", "bold"),
            CLI_VERSION,
            "\n",
            ("Python: ", "bold"),
            f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "\n\n",
            ("Configuração:", "bold"),
            "\n",
            ("CASPY_HOSTS: ", "bold"),
            config['hosts'],
            "\n",
            ("CASPY_KEYSPACE: ", "bold"),
            config['keyspace'],
            "\n",
            ("CASPY_PORT: ", "bold"),
            str(config['port']),
            "\n",
            ("CASPY_MODELS_PATH: ", "bold"),
            config['models_path'],
            "\n\n",
            ("Comandos disponíveis:", "bold"),
            "\n• query - Buscar e filtrar objetos",
            "\n• models - Listar modelos disponíveis", 
            "\n• connect - Testar conexão",
            "\n• info - Esta ajuda",
            "\n\n",
            ("Exemplos:", "bold"),
            "\n• caspy query user get --filter email=joao@email.com",
            "\n• caspy query post filter --filter author_id=123 --limit 5 --keyspace biblioteca",
            "\n• caspy query user count --filter is_active=true",
            "\n• caspy connect --keyspace meu_keyspace",
        ),
        title="[bold blue]CaspyORM CLI[/bold blue]",
        border_style="blue"
    )
    console.print(info_panel)

@app.callback()
def main(
    version: bool = typer.Option(False, "--version", "-v", help="Mostra a versão e sai.")
):
    """CaspyORM CLI - Ferramenta de linha de comando para interagir com modelos CaspyORM."""
    if version:
        console.print(f"[bold blue]CaspyORM CLI[/bold blue] v{CLI_VERSION}")
        raise typer.Exit()

if __name__ == "__main__":
    app() 