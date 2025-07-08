from . import __version__ as CLI_VERSION
import asyncio
# Use datetime from datetime for clarity
from datetime import datetime
import importlib
import importlib.util
import os
import sys
from typing import List, Optional
import functools

# Use tomllib for Python 3.11+ TOML parsing
try:
    import tomllib
except ImportError:
    # Fallback for older Python versions if needed, though pyproject.toml requires >=3.8
    # If dependency management ensures tomli is installed for <3.11:
    # try:
    #     import tomli as tomllib
    # except ImportError:
    #     print("Error: 'tomli' must be installed for Python < 3.11 to parse TOML files.")
    #     sys.exit(1)
    pass # Assuming Python 3.11+ based on the provided snippet using tomllib

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm, Prompt
from rich.table import Table
# Alias RichText to avoid conflict with caspyorm.fields.Text
from rich.text import Text as RichText

# Import Text from caspyorm.fields
from caspyorm import Model, connection
from caspyorm.core.fields import UUID, Timestamp, Text
from caspyorm.core.model import (
    Model as CaspyModel,
)
from caspyorm._internal.migration_model import Migration

"""
CaspyORM CLI - Ferramenta de linha de comando para interagir com modelos CaspyORM.
"""

# --- Decorators ---
def run_safe_cli(func):
    """Decorator para tratamento seguro de erros em comandos CLI."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except typer.Exit as e:
            if e.exit_code != 0:
                console.print(f"[bold red]Erro CLI ({e.exit_code})[/bold red]")
            raise  # Sempre re-raise para Typer
        except SystemExit as e:
            if getattr(e, 'code', 0) != 0:
                console.print(f"[bold red]Erro sistêmico (Exit Code: {e.code})[/bold red]")
            raise  # Sempre re-raise para Typer
        except Exception as e:
            console.print(f"[bold red]Erro inesperado:[/bold red] {e}")
            raise typer.Exit(1)
    return wrapper

# --- Configuração ---
app = typer.Typer(
    help="[bold blue]CaspyORM CLI[/bold blue] - Uma CLI poderosa para interagir com seus modelos CaspyORM.",
    add_completion=True,
    rich_markup_mode="rich",
)
migrate_app = typer.Typer(
    help="[bold green]Comandos para gerenciar migrações de schema.[/bold green]",
    rich_markup_mode="rich",
)
app.add_typer(migrate_app, name="migrate")
console = Console()

MIGRATIONS_DIR = "migrations"


def get_config():
    """Obtém configuração do CLI, lendo de caspy.toml, variáveis de ambiente e defaults."""
    config = {
        "hosts": ["127.0.0.1"],
        "keyspace": "caspyorm_demo",
        "port": 9042,
        "model_paths": [],  # Caminhos adicionais para busca de modelos
    }

    # 1. Ler de caspy.toml
    config_file_path = os.path.join(os.getcwd(), "caspy.toml")
    if os.path.exists(config_file_path):
        try:
            with open(config_file_path, "rb") as f:
                toml_config = tomllib.load(f)

            if "cassandra" in toml_config:
                cassandra_config = toml_config["cassandra"]
                if "hosts" in cassandra_config:
                    config["hosts"] = cassandra_config["hosts"]
                if "port" in cassandra_config:
                    config["port"] = cassandra_config["port"]
                if "keyspace" in cassandra_config:
                    config["keyspace"] = cassandra_config["keyspace"]

            if "cli" in toml_config:
                cli_config = toml_config["cli"]
                if "model_paths" in cli_config:
                    config["model_paths"] = cli_config["model_paths"]

        except Exception as e:
            console.print(f"[bold red]Aviso:[/bold red] Erro ao ler caspy.toml: {e}")

    # 2. Sobrescrever com variáveis de ambiente
    caspy_hosts = os.getenv("CASPY_HOSTS")
    if caspy_hosts:
        config["hosts"] = caspy_hosts.split(",")
    caspy_keyspace = os.getenv("CASPY_KEYSPACE")
    if caspy_keyspace:
        config["keyspace"] = caspy_keyspace
    caspy_port = os.getenv("CASPY_PORT")
    if caspy_port:
        try:
            config["port"] = int(caspy_port)
        except ValueError:
            console.print(f"[bold red]Aviso:[/bold red] CASPY_PORT inválido: {caspy_port}. Usando padrão.")

    caspy_models_path = os.getenv("CASPY_MODELS_PATH")
    if caspy_models_path:
        config["model_paths"].extend(caspy_models_path.split(","))

    return config


async def safe_disconnect():
    """Desconecta do Cassandra de forma segura."""
    try:
        await connection.disconnect_async()
    except Exception:
        pass


def discover_models(search_paths: List[str]) -> dict[str, type[Model]]:
    """
    Descobre dinamicamente classes de modelo CaspyORM em uma lista de caminhos.
    """
    models_found = {}
    original_sys_path = list(sys.path)

    # Ensure search paths are unique and absolute
    abs_search_paths = set()
    for search_path in search_paths:
        abs_path = os.path.abspath(search_path)
        if os.path.isdir(abs_path):
            abs_search_paths.add(abs_path)

    for abs_search_path in abs_search_paths:
        # Adiciona o diretório de busca ao sys.path temporariamente
        if abs_search_path not in sys.path:
            sys.path.insert(0, abs_search_path)

        for root, _, files in os.walk(abs_search_path):
            for file in files:
                if file.endswith(".py") and file != "__init__.py":
                    try:
                        relative_path = os.path.relpath(
                            os.path.join(root, file), abs_search_path
                        )
                        module_name = os.path.splitext(relative_path)[0].replace(
                            os.sep, "."
                        )

                        # Tenta importar o módulo
                        module = importlib.import_module(module_name)
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if (
                                isinstance(attr, type)
                                and issubclass(attr, Model)
                                and attr != Model
                                and attr.__module__ == module_name # Ensure it's defined in this module
                            ):
                                models_found[attr.__name__.lower()] = attr
                    except (ImportError, AttributeError, TypeError) as e:
                        # Opcional: Logar avisos se necessário
                        # console.print(f"[yellow]Aviso:[/yellow] Pulando módulo '{module_name}': {e}")
                        pass

    # Restaura o sys.path
    sys.path = original_sys_path
    return models_found


def get_default_search_paths() -> List[str]:
    """Retorna os caminhos de busca padrão para modelos."""
    return [
        os.getcwd(),  # Diretório atual
        os.path.join(os.getcwd(), "models"),  # Subdiretório 'models'
        # Modelos internos (como Migration) são descobertos implicitamente se importados no CLI
    ]

def get_model_names(ctx: typer.Context) -> List[str]:
    """Retorna uma lista de nomes de modelos para autocompletion."""
    config = ctx.obj["config"]
    search_paths = get_default_search_paths()

    for p in config["model_paths"]:
        search_paths.append(os.path.abspath(p))

    all_models = discover_models(search_paths)
    return sorted(all_models.keys())


def get_model_names_for_completion(incomplete: str) -> List[str]:
    """Função de autocompletion que não depende do contexto do Typer."""
    config = get_config()
    search_paths = get_default_search_paths() + config.get("model_paths", [])
    all_models = discover_models(search_paths)
    return [name for name in sorted(all_models.keys()) if name.startswith(incomplete)]


def find_model_class(model_name: str) -> type[Model]:
    """Descobre e retorna a classe do modelo pelo nome, usando a descoberta automática."""
    config = get_config()
    search_paths = get_default_search_paths()

    # Adiciona caminhos de modelo do arquivo de configuração
    for p in config["model_paths"]:
        search_paths.append(os.path.abspath(p))

    all_models = discover_models(search_paths)
    model_class = all_models.get(model_name.lower())

    if model_class:
        return model_class
    else:
        console.print(
            f"[bold red]Erro:[/bold red] Modelo '{model_name}' não encontrado."
        )
        console.print(
            "\n[bold]Dica:[/bold] Verifique se o nome do modelo está correto e se seus arquivos de modelo estão em um dos caminhos de busca padrão ou configurados em caspy.toml."
        )
        # Exibindo apenas caminhos que existem para clareza
        existing_paths = [p for p in search_paths if os.path.exists(p)]
        console.print(f"Caminhos de busca verificados: {', '.join(existing_paths)}")
        console.print(
            f"Modelos disponíveis: {', '.join(all_models.keys()) if all_models else 'Nenhum'}"
        )
        # FIX: Removed 'from e' as 'e' is not defined in this scope.
        raise typer.Exit(1)


def parse_filters(filters: List[str]) -> dict:
    """Converte filtros da linha de comando em dicionário, suportando operadores (gt, lt, in, etc)."""
    result = {}
    for filter_str in filters:
        if "=" in filter_str:
            key, value = filter_str.split("=", 1)
            # Suporte a operadores: key__op=value (já tratado pelo split anterior)

            # Suporte a listas para operador in
            if key.endswith("__in"):
                value_list = [v.strip() for v in value.split(",")]
                # Converter UUIDs na lista se necessário (simplificado)
                if "id" in key:
                    try:
                        import uuid
                        value_list = [
                            uuid.UUID(v) if len(v) == 36 and "-" in v else v
                            for v in value_list
                        ]
                    except ValueError:
                        pass  # Manter como string se não for UUID válido
                result[key] = value_list
                continue

            # Converter tipos especiais
            if value.lower() == "true":
                result[key] = True
            elif value.lower() == "false":
                result[key] = False
            elif value.lower() == "none" or value.lower() == "null":
                result[key] = None
            else:
                try:
                    # Tentativa de conversão para float/int
                    if "." in value or "e" in value.lower():
                        result[key] = float(value)
                    else:
                        result[key] = int(value)
                except ValueError:
                    # Tentar converter para UUID se o campo for 'id' ou terminar com '_id'
                    if key.endswith("id") or key.endswith("_id"):
                        if len(value) == 36 and "-" in value:
                            try:
                                import uuid
                                result[key] = uuid.UUID(value)
                            except ValueError:
                                result[key] = value
                        else:
                            result[key] = value
                    else:
                        result[key] = value
    return result


async def run_query(
    model_name: str,
    command: str,
    filters: list[str],
    limit: Optional[int] = None,
    force: bool = False,
    ctx: Optional[typer.Context] = None,
):
    # Validação de argumentos
    allowed_commands = ['get', 'filter', 'count', 'exists', 'delete']
    if command not in allowed_commands:
        console.print(f"[bold red]Comando inválido: '{command}'. Comandos permitidos: {', '.join(allowed_commands)}[/bold red]")
        raise typer.Exit(1)
    
    if command == 'delete' and not filters and not force:
        console.print("[bold red]⚠️  ATENÇÃO: Comando 'delete' sem filtros pode deletar todos os registros![/bold red]")
        console.print("[yellow]Use --filter para especificar critérios ou --force para confirmar.[/yellow]")
        console.print("[yellow]Exemplo: --filter id=123 --force[/yellow]")
        raise typer.Exit(1)
    if ctx is None:
        config = get_config()
    else:
        config = ctx.obj["config"]
    target_keyspace = config["keyspace"]

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Conectando ao Cassandra (keyspace: {target_keyspace})...", total=None
            )

            await connection.connect_async(
                contact_points=config["hosts"], keyspace=target_keyspace
            )
            progress.update(task, description="Conectado! Buscando modelo...")

            ModelClass = find_model_class(model_name)
            filter_dict = parse_filters(filters)

            progress.update(
                task,
                description=f"Executando '{command}' no modelo '{ModelClass.__name__}'...",
            )

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
                status = (
                    "[bold green]Sim[/bold green]"
                    if exists
                    else "[bold red]Não[/bold red]"
                )
                console.print(f"Existe: {status}")

            elif command == "delete":
                if not filter_dict:
                    console.print(
                        "[bold red]Erro:[/bold red] Filtros são obrigatórios para delete."
                    )
                    return

                # Pular confirmação se force=True
                if force or Confirm.ask(
                    f"Tem certeza que deseja deletar registros com filtros {filter_dict}?"
                ):
                    # count é sempre 0 para delete no Cassandra, mas a operação é executada
                    await ModelClass.filter(**filter_dict).delete_async()
                    console.print(
                        f"[bold green]Operação de deleção enviada.[/bold green]"
                    )
                    console.print(
                        "[yellow]Nota:[/yellow] O Cassandra não retorna o número exato de registros deletados."
                    )
                else:
                    console.print("[yellow]Operação cancelada.[/yellow]")

            else:
                console.print(
                    f"[bold red]Erro:[/bold red] Comando '{command}' não reconhecido."
                )

    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg.lower():
            console.print(
                f"[bold red]Erro:[/bold red] Tabela ou Keyspace não encontrado: '{target_keyspace}'"
            )
            console.print(
                "[bold]Solução:[/bold] Use --keyspace para especificar o keyspace correto ou verifique se a tabela existe."
            )
        else:
            console.print(f"[bold red]Erro:[/bold red] {error_msg}")
        # Ensure 'from e' is used correctly if re-raising
        raise typer.Exit(1)
    finally:
        await safe_disconnect()


@app.command(
    help="Busca ou filtra objetos no banco de dados.\n\nOperadores suportados nos filtros:\n- __gt, __lt, __gte, __lte, __in, __contains\nExemplo: --filter idade__gt=30 --filter nome__in=joao,maria"
)
@run_safe_cli
def query(
    ctx: typer.Context,
    model_name: str = typer.Argument(
        ...,
        help="Nome do modelo (ex: 'usuario', 'livro').",
        autocompletion=get_model_names_for_completion,
    ),
    command: str = typer.Argument(
        ...,
        help="Comando a ser executado ('get', 'filter', 'count', 'exists', 'delete').",
    ),
    filters: List[str] = typer.Option(
        None,
        "--filter",
        "-f",
        help="Filtros no formato 'campo=valor'. Suporta operadores: __gt, __lt, __in, etc.",
    ),
    limit: Optional[int] = typer.Option(
        None, "--limit", "-l", help="Limitar número de resultados."
    ),
    force: bool = typer.Option(
        False, "--force", help="Forçar operação sem confirmação."
    ),
):
    """
    Ponto de entrada síncrono que chama a lógica assíncrona.

    Exemplos:
    - caspy query usuario get --filter nome=joao
    - caspy query livro filter --filter autor_id=123 --limit 5 --keyspace biblioteca
    - caspy query usuario count --filter ativo=true
    - caspy query usuario filter --filter idade__gt=30 --filter nome__in=joao,maria
    """
    asyncio.run(run_query(model_name, command, filters or [], limit, force, ctx))


@app.command(help="Lista todos os modelos disponíveis.")
def models():
    """Lista todos os modelos disponíveis no módulo configurado."""
    config = get_config()
    search_paths = get_default_search_paths() + config.get("model_paths", [])
    all_models = discover_models(search_paths)
    # Remove o modelo de Migration interno da lista pública
    all_models.pop('migration', None)
    
    model_classes = list(all_models.values())

    if not model_classes:
        console.print(
            "[yellow]Nenhum modelo CaspyORM encontrado nos caminhos de busca.[/yellow]"
        )
        console.print(
            "\n[bold]Dica:[/bold] Verifique se seus arquivos de modelo estão no diretório atual, em um subdiretório 'models', ou configurados em caspy.toml/[.env]."
        )
        return

    table = Table(title="Modelos CaspyORM disponíveis")
    table.add_column("Nome", style="cyan")
    table.add_column("Tabela", style="green")
    table.add_column("Campos", style="yellow")

    for model_cls in model_classes:
        fields = list(model_cls.model_fields.keys())
        table.add_row(
            model_cls.__name__,
            model_cls.__table_name__,
            ", ".join(fields[:5]) + ("..." if len(fields) > 5 else ""),
        )

    console.print(table)


@app.command(help="Conecta ao Cassandra e testa a conexão.")
def connect(
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para testar (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Testa a conexão com o Cassandra."""
    config = get_config()
    target_keyspace = keyspace or config["keyspace"]

    async def test_connection():
        config = get_config()
        target_keyspace = keyspace or config["keyspace"]
        
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Conectando ao Cassandra (keyspace: {target_keyspace})...", total=None
                )
                await connection.connect_async(
                    contact_points=config["hosts"], keyspace=target_keyspace
                )
                progress.update(task, description="Conectado! Testando query...")
                
                # Testa uma query simples
                result = await connection.execute_async("SELECT release_version FROM system.local")
                version = result.one().release_version
                progress.update(task, description="✅ Conexão testada com sucesso!")
                
                console.print(f"[bold green]Conexão com Cassandra estabelecida[/bold green]")
                console.print(f"[green]Versão do Cassandra: {version}[/green]")
                
        except Exception as e:
            console.print(f"[bold red]❌ Erro ao conectar:[/bold red] {e}")
            raise typer.Exit(1)
        finally:
            await safe_disconnect()
    
    asyncio.run(test_connection())


@app.command(help="Mostra informações sobre a CLI.")
def info():
    """Mostra informações sobre a CLI e configuração."""
    config = get_config()

    info_panel = Panel(
        RichText.assemble(
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
            ("Hosts (CASPY_HOSTS): ", "bold"),
            ", ".join(config["hosts"]),
            "\n",
            ("Keyspace (CASPY_KEYSPACE): ", "bold"),
            config["keyspace"],
            "\n",
            ("Porta (CASPY_PORT): ", "bold"),
            str(config["port"]),
            "\n",
            ("Model Search Paths (CASPY_MODELS_PATH/caspy.toml): ", "bold"),
            ", ".join(config["model_paths"]) if config["model_paths"] else "(Padrão)",
            "\n\n",
            ("Comandos disponíveis:", "bold"),
            "\n• query - Buscar e filtrar objetos",
            "\n• models - Listar modelos disponíveis",
            "\n• connect - Testar conexão",
            "\n• migrate - Gerenciar migrações de schema",
            "\n• info - Esta ajuda",
            "\n• shell - Iniciar um shell interativo",
        ),
        title="[bold blue]CaspyORM CLI[/bold blue]",
        border_style="blue",
    )
    console.print(info_panel)


# --- Migrations ---

def ensure_migrations_dir():
    """Garante que o diretório de migrações exista."""
    if not os.path.exists(MIGRATIONS_DIR):
        os.makedirs(MIGRATIONS_DIR)
        console.print(f"[yellow]Diretório '{MIGRATIONS_DIR}' criado.[/yellow]")

@migrate_app.command(
    "init", help="Inicializa o sistema de migrações, criando a tabela de controle."
)
def migrate_init_sync(
    ctx: typer.Context,
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para inicializar (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Cria a tabela caspyorm_migrations se ela não existir."""
    ensure_migrations_dir()
    asyncio.run(migrate_init_async(ctx))


async def migrate_init_async(ctx: typer.Context):
    config = ctx.obj["config"]
    target_keyspace = config["keyspace"]

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Conectando ao Cassandra (keyspace: {target_keyspace})...", total=None
            )
            await connection.connect_async(
                contact_points=config["hosts"], keyspace=target_keyspace
            )
            progress.update(
                task, description="Conectado! Verificando tabela de migrações..."
            )

            # Sincroniza a tabela de migrações
            await Migration.sync_table_async(auto_apply=True, verbose=False)
            progress.update(
                task,
                description="✅ Tabela de migrações verificada/criada com sucesso!",
            )
            console.print(
                f"[bold green]Tabela 'caspyorm_migrations' pronta no keyspace '{target_keyspace}'.[/bold green]"
            )

    except Exception as e:
        console.print(f"[bold red]❌ Erro ao inicializar migrações:[/bold red] {e}")
        # Use 'from e' correctly when raising typer.Exit
        raise typer.Exit(1)
    finally:
        await safe_disconnect()


@migrate_app.command("new", help="Cria um novo arquivo de migração.")
def migrate_new(
    name: str = typer.Argument(
        ..., help="Nome descritivo da migração (ex: 'create_users_table')."
    ),
):
    """Cria um novo arquivo de migração com um template básico."""
    ensure_migrations_dir()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    # Sanitizar o nome para ser um nome de arquivo válido (simples)
    sanitized_name = name.replace(" ", "_").lower()
    file_name = f"V{timestamp}__{sanitized_name}.py"
    file_path = os.path.join(MIGRATIONS_DIR, file_name)

    try:
        # Forma segura que funciona tanto em desenvolvimento quanto em produção
        import importlib.resources
        
        template_content = importlib.resources.files('caspyorm_cli.templates').joinpath('migration_template.py.j2').read_text(encoding='utf-8')

        formatted_template = template_content.format(
            name=sanitized_name, created_at=datetime.now()
        )

        with open(file_path, "w", encoding='utf-8') as f:
            f.write(formatted_template)
        console.print(f"[bold green]Migração criada:[/bold green] {file_path}")
    except Exception as e:
        console.print(f"[bold red]Erro ao criar migração:[/bold red] {e}")
        raise typer.Exit(1)


@migrate_app.command(
    "status", help="Mostra o status das migrações (aplicadas vs. pendentes)."
)
def migrate_status_sync(
    ctx: typer.Context,
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para verificar (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Mostra o status das migrações (aplicadas vs. pendentes)."""
    ensure_migrations_dir()
    asyncio.run(migrate_status_async(ctx))


async def migrate_status_async(ctx: typer.Context):
    config = ctx.obj["config"]
    target_keyspace = config["keyspace"]

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Conectando ao Cassandra (keyspace: {target_keyspace})...", total=None
            )
            await connection.connect_async(
                contact_points=config["hosts"], keyspace=target_keyspace
            )
            progress.update(
                task, description="Conectado! Buscando migrações aplicadas..."
            )

            # FIX: Busca as versões (nomes dos arquivos) aplicadas
            try:
                applied_migrations_raw = await Migration.filter().all_async()
                applied_versions = {m.version for m in applied_migrations_raw}
            except Exception as e:
                if "does not exist" in str(e):
                     console.print("[bold yellow]Tabela de migrações não encontrada. Execute 'caspy migrate init' primeiro.[/bold yellow]")
                     raise typer.Exit(1)
                else:
                    raise e


            progress.update(task, description="Buscando arquivos de migração...")

            # FIX: Lista os arquivos no diretório
            migration_files = []
            if os.path.exists(MIGRATIONS_DIR):
                for f in os.listdir(MIGRATIONS_DIR):
                    if f.startswith("V") and f.endswith(".py"):
                        migration_files.append(f)

            # Ordena as migrações por nome de arquivo (que inclui o timestamp)
            migration_files.sort()

            table = Table(title="Status das Migrações")
            table.add_column("Versão (Arquivo)", style="cyan")
            table.add_column("Status", style="green")

            # Adiciona migrações aplicadas que podem não ter um arquivo correspondente (ex: arquivo deletado)
            applied_but_missing = applied_versions - set(migration_files)
            for applied_version in sorted(list(applied_but_missing)):
                 table.add_row(
                    applied_version,
                    "[bold green]APLICADA[/bold green] [red](Arquivo Ausente)[/red]",
                )

            # Adiciona migrações encontradas nos arquivos
            for file_name in migration_files:
                status = (
                    "[bold green]APLICADA[/bold green]"
                    if file_name in applied_versions
                    else "[bold yellow]PENDENTE[/bold yellow]"
                )
                table.add_row(file_name, status)

            console.print(table)

    except typer.Exit:
        # Propagar saídas do typer (como a do init faltante)
        raise
    except Exception as e:
        console.print(
            f"[bold red]❌ Erro ao verificar status das migrações:[/bold red] {e}"
        )
        raise typer.Exit(1)
    finally:
        await safe_disconnect()


@migrate_app.command("apply", help="Aplica migrações pendentes.")
def migrate_apply_sync(
    ctx: typer.Context,
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para aplicar (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Aplica migrações pendentes."""
    ensure_migrations_dir()
    asyncio.run(migrate_apply_async(ctx))


async def migrate_apply_async(ctx: typer.Context):
    config = ctx.obj["config"]
    target_keyspace = config["keyspace"]

    # Adicionar o diretório de migrações ao sys.path temporariamente para imports
    migrations_abs_path = os.path.abspath(MIGRATIONS_DIR)
    if migrations_abs_path not in sys.path:
        sys.path.insert(0, migrations_abs_path)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Conectando ao Cassandra (keyspace: {target_keyspace})...", total=None
            )
            await connection.connect_async(
                contact_points=config["hosts"], keyspace=target_keyspace
            )
            progress.update(
                task, description="Conectado! Buscando migrações aplicadas..."
            )

            # FIX: Busca as versões (nomes dos arquivos) aplicadas
            try:
                applied_migrations_raw = await Migration.filter().all_async()
                applied_versions = {m.version for m in applied_migrations_raw}
            except Exception as e:
                if "does not exist" in str(e):
                     console.print("[bold yellow]Tabela de migrações não encontrada. Execute 'caspy migrate init' primeiro.[/bold yellow]")
                     raise typer.Exit(1)
                else:
                    raise e

            progress.update(task, description="Buscando arquivos de migração...")

            # FIX: Coleta e ordena os arquivos
            migration_files = sorted([
                f for f in os.listdir(MIGRATIONS_DIR)
                if f.startswith("V") and f.endswith(".py")
            ])

            # FIX: Determina quais estão pendentes com base no nome do arquivo
            pending_migrations = [f for f in migration_files if f not in applied_versions]

            if not pending_migrations:
                console.print(
                    "[bold green]✅ Nenhuma migração pendente para aplicar.[/bold green]"
                )
                return

            console.print(
                f"[bold yellow]Aplicando {len(pending_migrations)} migrações pendentes...[/bold yellow]"
            )
            for file_name in pending_migrations:
                progress.update(
                    task,
                    description=f"Aplicando migração: {file_name}...",
                )

                # Importa e executa a migração
                # O nome do módulo deve ser único para o importlib
                module_name = os.path.splitext(file_name)[0]
                migration_full_path = os.path.join(MIGRATIONS_DIR, file_name)
                
                spec = importlib.util.spec_from_file_location(
                    module_name, migration_full_path
                )
                
                if spec is None or spec.loader is None:
                    console.print(
                        f"[bold red]❌ Erro:[/bold red] Não foi possível carregar a especificação para a migração '{file_name}'."
                    )
                    continue

                module = importlib.util.module_from_spec(spec)
                # Registrar o módulo no sys.modules é crucial para imports relativos dentro da migração
                sys.modules[module_name] = module
                
                try:
                    spec.loader.exec_module(module)
                    
                    if hasattr(module, "upgrade") and callable(module.upgrade):
                        await module.upgrade()
                        
                        # FIX: Registra a migração usando o nome do arquivo como versão
                        await Migration(
                            version=file_name, applied_at=datetime.now()
                        ).save_async()
                        console.print(
                            f"[bold green]✅ Migração '{file_name}' aplicada com sucesso.[/bold green]"
                        )
                    else:
                        console.print(
                            f"[bold red]❌ Erro:[/bold red] Migração '{file_name}' não possui função 'upgrade'."
                        )
                        # Parar se uma migração falhar para manter a ordem
                        raise typer.Exit(1)

                except Exception as e:
                    console.print(
                        f"[bold red]❌ Erro ao aplicar migração '{file_name}':[/bold red] {e}"
                    )
                    # Parar se uma migração falhar
                    raise typer.Exit(1)

            console.print(
                "[bold green]✅ Processo de aplicação de migrações concluído.[/bold green]"
            )

    except typer.Exit:
         # Propagar saídas do typer
        raise
    except Exception as e:
        console.print(f"[bold red]❌ Erro geral ao aplicar migrações:[/bold red] {e}")
        raise typer.Exit(1)
    finally:
        # Remover o diretório de migrações do sys.path
        if migrations_abs_path in sys.path:
            sys.path.remove(migrations_abs_path)
        await safe_disconnect()


@migrate_app.command(
    "downgrade", help="Reverte a última migração aplicada."
)
def migrate_downgrade_sync(
    ctx: typer.Context,
    # Removido o argumento 'name' para simplificar o downgrade (sempre o último)
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para reverter (sobrescreve CASPY_KEYSPACE).",
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Forçar o downgrade sem confirmação."
    ),
):
    """Reverte a última migração aplicada."""
    ensure_migrations_dir()
    asyncio.run(migrate_downgrade_async(ctx, force))


async def migrate_downgrade_async(ctx: typer.Context, force: bool):
    config = ctx.obj["config"]
    target_keyspace = config["keyspace"]
    config = ctx.obj["config"]
    target_keyspace = config["keyspace"]

    # Adicionar o diretório de migrações ao sys.path temporariamente
    migrations_abs_path = os.path.abspath(MIGRATIONS_DIR)
    if migrations_abs_path not in sys.path:
        sys.path.insert(0, migrations_abs_path)

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            # ... (Conexão similar ao apply) ...
            await connection.connect_async(
                contact_points=config["hosts"], keyspace=target_keyspace
            )

            # 1. Encontrar a última migração aplicada (baseado na ordem do arquivo)
            applied_migrations_raw = await Migration.filter().all_async()
            if not applied_migrations_raw:
                console.print("[bold yellow]Nenhuma migração aplicada para reverter.[/bold yellow]")
                return

            # Ordenar as aplicadas para encontrar a última (nomes de arquivo são ordenáveis por data)
            last_applied = sorted(applied_migrations_raw, key=lambda m: m.version, reverse=True)[0]
            file_name = last_applied.version

            # 2. Verificar se o arquivo existe
            migration_full_path = os.path.join(MIGRATIONS_DIR, file_name)
            if not os.path.exists(migration_full_path):
                console.print(f"[bold red]Erro:[/bold red] Arquivo da última migração '{file_name}' não encontrado. Não é possível reverter.")
                raise typer.Exit(1)

            # 3. Confirmação
            if not force and not Confirm.ask(f"Tem certeza que deseja reverter a migração: {file_name}?"):
                console.print("[yellow]Downgrade cancelado.[/yellow]")
                return

            console.print(f"[bold yellow]Revertendo migração: {file_name}...[/bold yellow]")
            
            # 4. Importar e executar downgrade
            module_name = os.path.splitext(file_name)[0]
            spec = importlib.util.spec_from_file_location(module_name, migration_full_path)
            
            if spec is None or spec.loader is None:
                 # ... (erro de carregamento) ...
                raise typer.Exit(1)

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module

            try:
                spec.loader.exec_module(module)
                if hasattr(module, "downgrade") and callable(module.downgrade):
                    await module.downgrade()
                    
                    # Remove o registro da migração
                    await last_applied.delete_async()
                    console.print(
                        f"[bold green]✅ Migração '{file_name}' revertida com sucesso.[/bold green]"
                    )
                else:
                    console.print(
                        f"[bold red]❌ Erro:[/bold red] Migração '{file_name}' não possui função 'downgrade'."
                    )
                    raise typer.Exit(1)
            except Exception as e:
                console.print(
                    f"[bold red]❌ Erro ao reverter migração '{file_name}':[/bold red] {e}"
                )
                raise typer.Exit(1)

    except Exception as e:
        # ... (Tratamento de erro geral) ...
        raise typer.Exit(1)
    finally:
        # Remover o diretório de migrações do sys.path
        if migrations_abs_path in sys.path:
            sys.path.remove(migrations_abs_path)
        await safe_disconnect()


@app.command("version", help="Mostra a versão do CaspyORM CLI.")
def version_cmd():
    """Exibe a versão do CLI."""
    console.print(f"[bold blue]CaspyORM CLI[/bold blue] v{CLI_VERSION}")


@app.command(help="Executa uma query SQL direta no Cassandra.")
@run_safe_cli
def sql(
    ctx: typer.Context,
    query: str = typer.Argument(
        ...,
        help="Query SQL/CQL a ser executada.",
    ),
    allow_filtering: bool = typer.Option(
        False,
        "--allow-filtering",
        help="Permitir ALLOW FILTERING na query (use com cautela).",
    ),
):
    """
    Executa uma query SQL/CQL direta no Cassandra.
    
    Exemplos:
    - caspy sql "SELECT * FROM nyc_restaurants LIMIT 5"
    - caspy sql "SELECT name, cuisine FROM nyc_restaurants WHERE borough = 'Manhattan'"
    - caspy sql "SELECT COUNT(*) FROM nyc_restaurants"
    """
    asyncio.run(run_sql_query(query, allow_filtering, ctx))


async def run_sql_query(query: str, allow_filtering: bool, ctx: typer.Context):
    """Executa uma query SQL direta."""
    config = ctx.obj["config"]
    target_keyspace = config["keyspace"]

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Conectando ao Cassandra (keyspace: {target_keyspace})...", total=None
            )

            await connection.connect_async(
                contact_points=config["hosts"], keyspace=target_keyspace
            )
            progress.update(task, description="Conectado! Executando query...")

            # Adicionar ALLOW FILTERING se a flag for True e for uma query SELECT
            if allow_filtering and query.strip().upper().startswith("SELECT"):
                query += " ALLOW FILTERING"
            result = await connection.execute_async(query)
            progress.update(task, description="Query executada! Processando resultados...")

            # Processar resultados
            if result:
                # Criar tabela com resultados
                table = Table(title=f"Resultados da Query")
                
                # Converter ResultSet para lista para facilitar o processamento
                rows = list(result)
                
                if rows:
                    first_row = rows[0]
                    if hasattr(first_row, '_fields'):
                        # ResultSet com namedtuple
                        headers = first_row._fields
                        for header in headers:
                            table.add_column(header, justify="left")

                        for row in rows:
                            table.add_row(*(str(getattr(row, h)) for h in headers))
                    else:
                        # Resultado simples
                        console.print(f"[bold green]Resultado:[/bold green] {result}")
                        return

                    console.print(table)
                    console.print(f"[bold green]Total de registros:[/bold green] {len(rows)}")
                else:
                    console.print("[yellow]Query executada com sucesso, mas não retornou resultados.[/yellow]")
            else:
                console.print("[yellow]Query executada com sucesso, mas não retornou resultados.[/yellow]")

    except Exception as e:
        console.print(f"[bold red]❌ Erro ao executar query:[/bold red] {e}")
        raise typer.Exit(1) from e
    finally:
        await safe_disconnect()


@app.command(help="Inicia um shell interativo Python/IPython com os modelos CaspyORM pré-carregados.")
@run_safe_cli
def shell():
    """Inicia um shell interativo Python/IPython com os modelos CaspyORM disponíveis."""
    import code
    import builtins
    try:
        from IPython import embed
        has_ipython = True
    except ImportError:
        has_ipython = False

    # Descobrir modelos
    search_paths = get_default_search_paths()
    config = get_config()
    for p in config["model_paths"]:
        search_paths.append(os.path.abspath(p))
    all_models = discover_models(search_paths)

    banner = """
[bold green]CaspyORM Shell Interativo[/bold green]
Modelos disponíveis: {model_list}
Exemplo: User.objects.filter(...)
Digite exit() ou Ctrl-D para sair.
""".format(model_list=", ".join(all_models.keys()) if all_models else "Nenhum modelo encontrado")

    # Contexto do shell: todos os modelos + builtins
    context = {**all_models, **vars(builtins)}

    console.print(banner)
    if has_ipython:
        embed(user_ns=context, banner1=banner)
    else:
        code.interact(banner=banner, local=context)


@app.callback()
def main(
    ctx: typer.Context,
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace a ser usado para todos os comandos (sobrescreve CASPY_KEYSPACE e caspy.toml).",
        envvar="CASPY_KEYSPACE",
    ),
    hosts: Optional[List[str]] = typer.Option(
        None,
        "--hosts",
        "-H",
        help="Lista de hosts do Cassandra (separados por vírgula, sobrescreve CASPY_HOSTS e caspy.toml).",
        envvar="CASPY_HOSTS",
    ),
    port: Optional[int] = typer.Option(
        None,
        "--port",
        "-p",
        help="Porta do Cassandra (sobrescreve CASPY_PORT e caspy.toml).",
        envvar="CASPY_PORT",
    ),
):
    """CaspyORM CLI - Ferramenta de linha de comando para interagir com modelos CaspyORM."""
    # Armazena as opções globais no contexto para que os subcomandos possam acessá-las
    ctx.ensure_object(dict)
    config = get_config() # Carrega a configuração base (de toml e env)

    # Sobrescreve com os valores passados via CLI, se existirem
    if keyspace:
        config["keyspace"] = keyspace
    if hosts:
        config["hosts"] = hosts
    if port:
        config["port"] = port

    ctx.obj["config"] = config

if __name__ == "__main__":
    app()