import functools
import importlib
import importlib.util
import os
import sys
from datetime import datetime
from typing import List, Optional

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
    pass  # Assuming Python 3.11+ based on the provided snippet using tomllib

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm
from rich.table import Table
from rich.text import Text as RichText

# Import Text from caspyorm.fields
from caspyorm import Model, connection
from caspyorm._internal.migration_model import Migration
from caspyorm.core.query import QuerySet

from caspyorm_cli import __version__ as CLI_VERSION

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
            if getattr(e, "code", 0) != 0:
                console.print(
                    f"[bold red]Erro sistêmico (Exit Code: {e.code})[/bold red]"
                )
            raise  # Sempre re-raise para Typer
        except Exception as e:
            console.print(f"[bold red]Erro inesperado:[/bold red] {e}")
            raise typer.Exit(1) from e

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
        "hosts": ["cassandra_nyc"],
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
            console.print(
                f"[bold red]Aviso:[/bold red] CASPY_PORT inválido: {caspy_port}. Usando padrão."
            )

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
                                and attr.__module__
                                == module_name  # Ensure it's defined in this module
                            ):
                                models_found[attr.__name__.lower()] = attr
                    except (ImportError, AttributeError, TypeError):
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
    allowed_commands = ["get", "filter", "count", "exists", "delete"]
    if command not in allowed_commands:
        console.print(
            f"[bold red]Comando inválido: '{command}'. Comandos permitidos: {', '.join(allowed_commands)}[/bold red]"
        )
        raise typer.Exit(1)

    if command == "delete" and not filters and not force:
        console.print(
            "[bold red]⚠️  ATENÇÃO: Comando 'delete' sem filtros pode deletar todos os registros![/bold red]"
        )
        console.print(
            "[yellow]Use --filter para especificar critérios ou --force para confirmar.[/yellow]"
        )
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

            # A conexão global já está gerenciada pelo Typer context
            # await connection.connect_async(
            #     contact_points=config["hosts"], keyspace=target_keyspace
            # )
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
                        "[bold green]Operação de deleção enviada.[/bold green]"
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
        raise typer.Exit(1) from e
    finally:
        # A conexão global já está gerenciada pelo Typer context
        # await safe_disconnect()
        pass


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
        help="Comando a ser executada ('get', 'filter', 'count', 'exists', 'delete').",
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
    allow_filtering: bool = typer.Option(
        False,
        "--allow-filtering",
        help="Permitir ALLOW FILTERING em queries (use com cautela).",
    ),
):
    """
    Executa queries usando apenas métodos síncronos.
    """
    from caspyorm.core.connection import connect, disconnect

    config = get_config()
    connect(
        contact_points=config["hosts"], keyspace=config["keyspace"], port=config["port"]
    )
    try:
        model_cls = find_model_class(model_name)
        if not model_cls:
            console.print(f"[red]Modelo '{model_name}' não encontrado.[/red]")
            raise typer.Exit(1)
        # Parse filters
        filter_kwargs = parse_filters(filters or [])
        qs_base = QuerySet(model_cls)
        if allow_filtering:
            qs_base = qs_base.allow_filtering()
        if command == "count":
            qs = qs_base.filter(**filter_kwargs)
            total = qs.count()
            console.print(f"Total de registros: [bold]{total}[/bold]")
        elif command == "get":
            qs = qs_base.filter(**filter_kwargs)
            obj = qs.first()
            if obj:
                console.print(obj.model_dump())
            else:
                console.print("[yellow]Nenhum registro encontrado.[/yellow]")
        elif command == "filter":
            qs = qs_base.filter(**filter_kwargs)
            if limit:
                qs = qs.limit(limit)
            results = qs.all()
            for obj in results:
                console.print(obj.model_dump())
        elif command == "exists":
            qs = qs_base.filter(**filter_kwargs)
            exists = qs.exists()
            console.print(f"Existe? [bold]{exists}[/bold]")
        elif command == "delete":
            qs = qs_base.filter(**filter_kwargs)
            if not force:
                if not Confirm.ask("Tem certeza que deseja deletar os registros?"):
                    console.print("[yellow]Operação cancelada.[/yellow]")
                    raise typer.Exit(0)
            deleted = qs.delete()
            console.print(f"Registros deletados: [bold]{deleted}[/bold]")
        else:
            console.print(f"[red]Comando '{command}' não suportado.[/red]")
            raise typer.Exit(1)
    finally:
        disconnect()


@app.command(help="Lista todos os modelos disponíveis.")
def models():
    """Lista todos os modelos disponíveis no módulo configurado."""
    config = get_config()
    search_paths = get_default_search_paths() + config.get("model_paths", [])
    all_models = discover_models(search_paths)
    # Remove o modelo de Migration interno da lista pública
    all_models.pop("migration", None)

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
    """
    Testa a conexão síncrona com o Cassandra.
    """
    from caspyorm.core.connection import connect, disconnect

    config = get_config()
    if keyspace:
        config["keyspace"] = keyspace
    try:
        connect(
            contact_points=config["hosts"],
            keyspace=config["keyspace"],
            port=config["port"],
        )
        console.print("[bold green]Conexão com o Cassandra bem-sucedida![/bold green]")
    except Exception as e:
        console.print(f"[bold red]Erro ao conectar:[/bold red] {e}")
        raise typer.Exit(1) from e
    finally:
        disconnect()


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
    """Cria a tabela caspyorm_migrations se ela não existir (SÍNCRONO)."""
    ensure_migrations_dir()
    config = ctx.obj["config"] if ctx.obj and "config" in ctx.obj else get_config()
    if keyspace:
        config["keyspace"] = keyspace
    from caspyorm.core.connection import connect, disconnect

    connect(
        contact_points=config["hosts"], keyspace=config["keyspace"], port=config["port"]
    )
    try:
        Migration.sync_table()
        console.print(
            f"[bold green]Tabela 'caspyorm_migrations' pronta no keyspace '{config['keyspace']}'.[/bold green]"
        )
    except Exception as e:
        console.print(f"[bold red]❌ Erro ao inicializar migrações:[/bold red] {e}")
        raise typer.Exit(1) from e
    finally:
        disconnect()


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

        template_content = (
            importlib.resources.files("caspyorm_cli.templates")
            .joinpath("migration_template.py.j2")
            .read_text(encoding="utf-8")
        )

        formatted_template = template_content.format(
            name=sanitized_name, created_at=datetime.now()
        )

        with open(file_path, "w", encoding="utf-8") as f:
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
    # Fallback para config
    config = ctx.obj["config"] if ctx.obj and "config" in ctx.obj else get_config()
    if keyspace:
        config["keyspace"] = keyspace
    from caspyorm.core.connection import connect, disconnect

    connect(
        contact_points=config["hosts"], keyspace=config["keyspace"], port=config["port"]
    )
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Conectando ao Cassandra (keyspace: {config['keyspace']})...",
                total=None,
            )
            progress.update(
                task, description="Conectado! Buscando migrações aplicadas..."
            )
            try:
                applied_migrations_raw = Migration.filter().all()
                applied_versions = {
                    getattr(m, "version", getattr(m, "file_name", None))
                    for m in applied_migrations_raw
                    if hasattr(m, "version") or hasattr(m, "file_name")
                }
            except Exception as e:
                if "does not exist" in str(e):
                    console.print(
                        "[bold yellow]Tabela de migrações não encontrada. Execute 'caspy migrate init' primeiro.[/bold yellow]"
                    )
                    raise typer.Exit(1)
                else:
                    raise e
            progress.update(task, description="Buscando arquivos de migração...")
            migration_files = []
            if os.path.exists(MIGRATIONS_DIR):
                for f in os.listdir(MIGRATIONS_DIR):
                    if f.startswith("V") and f.endswith(".py"):
                        migration_files.append(f)
            migration_files.sort()
            table = Table(title="Status das Migrações")
            table.add_column("Versão (Arquivo)", style="cyan")
            table.add_column("Status", style="green")
            applied_but_missing = applied_versions - set(migration_files)
            for applied_version in sorted(
                [v for v in applied_but_missing if isinstance(v, str)]
            ):
                table.add_row(
                    applied_version,
                    "[bold green]APLICADA[/bold green] [red](Arquivo Ausente)[/red]",
                )
            for file_name in migration_files:
                status = (
                    "[bold green]APLICADA[/bold green]"
                    if file_name in applied_versions
                    else "[bold yellow]PENDENTE[/bold yellow]"
                )
                table.add_row(file_name, status)
            console.print(table)
    except typer.Exit:
        raise
    except Exception as e:
        console.print(
            f"[bold red]❌ Erro ao verificar status das migrações:[/bold red] {e}"
        )
        raise typer.Exit(1)
    finally:
        disconnect()


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
    config = ctx.obj["config"] if ctx.obj and "config" in ctx.obj else get_config()
    if keyspace:
        config["keyspace"] = keyspace
    from caspyorm.core.connection import connect, disconnect

    connect(
        contact_points=config["hosts"], keyspace=config["keyspace"], port=config["port"]
    )
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
                f"Conectando ao Cassandra (keyspace: {config['keyspace']})...",
                total=None,
            )
            progress.update(
                task, description="Conectado! Buscando migrações aplicadas..."
            )
            try:
                applied_migrations_raw = Migration.filter().all()
                applied_versions = {
                    getattr(m, "version", getattr(m, "file_name", None))
                    for m in applied_migrations_raw
                    if hasattr(m, "version") or hasattr(m, "file_name")
                }
            except Exception as e:
                if "does not exist" in str(e):
                    console.print(
                        "[bold yellow]Tabela de migrações não encontrada. Execute 'caspy migrate init' primeiro.[/bold yellow]"
                    )
                    raise typer.Exit(1)
                else:
                    raise e
            progress.update(task, description="Buscando arquivos de migração...")
            migration_files = sorted(
                [
                    f
                    for f in os.listdir(MIGRATIONS_DIR)
                    if f.startswith("V") and f.endswith(".py")
                ]
            )
            pending_migrations = [
                f for f in migration_files if f not in applied_versions
            ]
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
                sys.modules[module_name] = module
                try:
                    spec.loader.exec_module(module)
                    if hasattr(module, "upgrade") and callable(module.upgrade):
                        module.upgrade()
                        mig_kwargs = {
                            "applied_at": datetime.now(),
                            "version": file_name,
                        }
                        instance = Migration(**mig_kwargs)
                        save_fn = getattr(instance, "save", None)
                        if save_fn and callable(save_fn):
                            save_fn()
                        console.print(
                            f"[bold green]✅ Migração '{file_name}' aplicada com sucesso.[/bold green]"
                        )
                    else:
                        console.print(
                            f"[bold red]❌ Erro:[/bold red] Migração '{file_name}' não possui função 'upgrade'."
                        )
                        raise typer.Exit(1)
                except Exception as e:
                    console.print(
                        f"[bold red]❌ Erro ao aplicar migração '{file_name}':[/bold red] {e}"
                    )
                    raise typer.Exit(1)
            console.print(
                "[bold green]✅ Processo de aplicação de migrações concluído.[/bold green]"
            )
    except typer.Exit:
        raise
    except Exception as e:
        console.print(f"[bold red]❌ Erro geral ao aplicar migrações:[/bold red] {e}")
        raise typer.Exit(1)
    finally:
        if migrations_abs_path in sys.path:
            sys.path.remove(migrations_abs_path)
        disconnect()


@migrate_app.command("downgrade", help="Reverte a última migração aplicada.")
def migrate_downgrade_sync(
    ctx: typer.Context,
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
    config = ctx.obj["config"] if ctx.obj and "config" in ctx.obj else get_config()
    if keyspace:
        config["keyspace"] = keyspace
    from caspyorm.core.connection import connect, disconnect

    connect(
        contact_points=config["hosts"], keyspace=config["keyspace"], port=config["port"]
    )
    migrations_abs_path = os.path.abspath(MIGRATIONS_DIR)
    if migrations_abs_path not in sys.path:
        sys.path.insert(0, migrations_abs_path)
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            applied_migrations_raw = Migration.filter().all()
            if not applied_migrations_raw:
                console.print(
                    "[bold yellow]Nenhuma migração aplicada para reverter.[/bold yellow]"
                )
                return
            last_applied = sorted(
                applied_migrations_raw, key=lambda m: m.version, reverse=True
            )[0]
            file_name = last_applied.version
            migration_full_path = os.path.join(MIGRATIONS_DIR, file_name)
            if not os.path.exists(migration_full_path):
                console.print(
                    f"[bold red]Erro:[/bold red] Arquivo da última migração '{file_name}' não encontrado. Não é possível reverter."
                )
                raise typer.Exit(1)
            if not force and not Confirm.ask(
                f"Tem certeza que deseja reverter a migração: {file_name}?"
            ):
                console.print("[yellow]Downgrade cancelado.[/yellow]")
                return
            console.print(
                f"[bold yellow]Revertendo migração: {file_name}...[/bold yellow]"
            )
            module_name = os.path.splitext(file_name)[0]
            spec = importlib.util.spec_from_file_location(
                module_name, migration_full_path
            )
            if spec is None or spec.loader is None:
                raise typer.Exit(1)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            try:
                spec.loader.exec_module(module)
                if hasattr(module, "downgrade") and callable(module.downgrade):
                    module.downgrade()
                    last_applied.delete()
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
    except Exception:
        raise typer.Exit(1)
    finally:
        if migrations_abs_path in sys.path:
            sys.path.remove(migrations_abs_path)
        disconnect()


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
    Executa query CQL usando apenas métodos síncronos.
    """
    from caspyorm.core.connection import connect, disconnect, execute

    config = get_config()
    connect(
        contact_points=config["hosts"], keyspace=config["keyspace"], port=config["port"]
    )
    try:
        q = query
        if allow_filtering and "allow filtering" not in q.lower():
            q = q.rstrip(";") + " ALLOW FILTERING;"
        result = execute(q)
        for row in result:
            console.print(dict(row._asdict()))
    except Exception as e:
        console.print(f"[bold red]Erro ao executar query:[/bold red] {e}")
        raise typer.Exit(1)
    finally:
        disconnect()


@app.command(
    help="Inicia um shell interativo Python/IPython com os modelos CaspyORM pré-carregados."
)
@run_safe_cli
def shell():
    """Inicia um shell interativo Python/IPython com os modelos CaspyORM disponíveis."""
    import builtins
    import code

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
""".format(
        model_list=(
            ", ".join(all_models.keys()) if all_models else "Nenhum modelo encontrado"
        )
    )

    # Contexto do shell: todos os modelos + builtins
    context = {**all_models, **vars(builtins)}

    console.print(banner)
    if has_ipython:
        embed(user_ns=context, banner1=banner)
    else:
        code.interact(banner=banner, local=context)


# --- Gerenciamento global de conexão ---
async def _global_connect(ctx: typer.Context):
    config = ctx.obj["config"]
    await connection.connect_async(
        contact_points=config["hosts"], keyspace=config["keyspace"]
    )
    ctx.obj["connected"] = True


async def _global_disconnect(ctx: typer.Context):
    if ctx.obj.get("connected"):
        await connection.disconnect_async()
        ctx.obj["connected"] = False


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
    """
    Callback principal da CLI. Remove tentativa de conexão global assíncrona.
    """
    pass


if __name__ == "__main__":
    app()
