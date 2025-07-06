from cli import __version__ as CLI_VERSION
import asyncio
import datetime
import importlib
import importlib.util
import os
import sys
from typing import List, Optional

import tomllib  # Import tomllib for TOML parsing
import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Confirm, Prompt
from rich.table import Table
from rich.text import Text

from caspyorm import Model, connection
from caspyorm.fields import UUID, Timestamp
from caspyorm.model import (
    Model as CaspyModel,  # Alias to avoid conflict with cli.main.Model
)

"""
CaspyORM CLI - Ferramenta de linha de comando para interagir com modelos CaspyORM.
"""

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


def get_config():
    """Obtém configuração do CLI, lendo de caspy.toml, variáveis de ambiente e defaults."""
    config = {
        "hosts": ["localhost"],
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
        config["port"] = int(caspy_port)
    # CASPY_MODELS_PATH não é mais usado diretamente, mas model_paths do toml pode ser estendido

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

    for search_path in search_paths:
        abs_search_path = os.path.abspath(search_path)
        if not os.path.isdir(abs_search_path):
            continue

        # Adiciona o diretório de busca ao sys.path temporariamente
        if abs_search_path not in sys.path:
            sys.path.insert(0, abs_search_path)

        for root, _, files in os.walk(abs_search_path):
            for file in files:
                if file.endswith(".py"):
                    relative_path = os.path.relpath(
                        os.path.join(root, file), abs_search_path
                    )
                    module_name = os.path.splitext(relative_path)[0].replace(
                        os.sep, "."
                    )

                    # Evita importar __init__.py diretamente como módulos de modelo
                    if module_name.endswith(".__init__"):
                        module_name = module_name.rsplit(".", 1)[0]  # Remove .__init__
                        if not module_name:  # Se for apenas __init__.py no root
                            continue

                    try:
                        # Tenta importar o módulo
                        module = importlib.import_module(module_name)
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if (
                                isinstance(attr, type)
                                and issubclass(attr, Model)
                                and attr != Model
                            ):  # Garante que não é a própria classe Model
                                models_found[attr.__name__.lower()] = attr
                    except (ImportError, AttributeError, TypeError):
                        # Ignora erros de importação para arquivos que não são modelos
                        # console.print(f"[yellow]Aviso:[/yellow] Não foi possível importar '{module_name}': {e}")
                        pass

    # Restaura o sys.path
    sys.path = original_sys_path
    return models_found


def get_model_names() -> List[str]:
    """Retorna uma lista de nomes de modelos para autocompletion."""
    config = get_config()
    search_paths = [
        os.getcwd(),  # Diretório atual
        os.path.join(os.getcwd(), "models"),  # Subdiretório 'models'
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "caspyorm"
        ),  # Modelos internos do caspyorm
        os.path.join(os.getcwd(), "tests", "fixtures"),  # Modelos de teste, se houver
    ]
    for p in config["model_paths"]:
        abs_path = os.path.abspath(p)
        if os.path.isdir(abs_path):
            search_paths.append(abs_path)

    all_models = discover_models(search_paths)
    return sorted(all_models.keys())


def find_model_class(model_name: str) -> type[Model]:
    """Descobre e retorna a classe do modelo pelo nome, usando a descoberta automática."""
    config = get_config()

    # Define os caminhos de busca padrão
    search_paths = [
        os.getcwd(),  # Diretório atual
        os.path.join(os.getcwd(), "models"),  # Subdiretório 'models'
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "caspyorm"
        ),  # Modelos internos do caspyorm
        os.path.join(os.getcwd(), "tests", "fixtures"),  # Modelos de teste, se houver
    ]

    # Adiciona caminhos de modelo do arquivo de configuração
    for p in config["model_paths"]:
        # Resolve o caminho relativo ao diretório do arquivo de configuração
        # Assumimos que caspy.toml está no diretório de trabalho atual
        abs_path = os.path.abspath(p)
        if os.path.isdir(abs_path):
            search_paths.append(abs_path)

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
        console.print(f"Caminhos de busca: {', '.join(search_paths)}")
        console.print(
            f"Modelos disponíveis: {', '.join(all_models.keys()) if all_models else 'Nenhum'}"
        )
        raise typer.Exit(1) from e


def parse_filters(filters: List[str]) -> dict:
    """Converte filtros da linha de comando em dicionário, suportando operadores (gt, lt, in, etc)."""
    result = {}
    for filter_str in filters:
        if "=" in filter_str:
            key, value = filter_str.split("=", 1)
            # Suporte a operadores: key__op=value
            if "__" in key:
                field, op = key.split("__", 1)
                key = f"{field}__{op}"
            # Suporte a listas para operador in
            if key.endswith("__in"):
                value = [v.strip() for v in value.split(",")]
                # Converter UUIDs na lista se necessário
                if key.startswith("id__") or key.startswith("autor_id__"):
                    try:
                        import uuid

                        value = [
                            uuid.UUID(v) if len(v) == 36 and "-" in v else v
                            for v in value
                        ]
                    except ValueError:
                        pass  # Manter como string se não for UUID válido
                result[key] = value
                continue
            # Converter tipos especiais
            if value.lower() == "true":
                result[key] = True
            elif value.lower() == "false":
                result[key] = False
            else:
                try:
                    if "." in value or "e" in value.lower():
                        result[key] = float(value)
                    else:
                        result[key] = int(value)
                except ValueError:
                    # Tentar converter para UUID se o campo for 'id' ou terminar com '_id'
                    if key == "id" or key.endswith("_id"):
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
    keyspace: Optional[str] = None,
):
    """Executa uma query no banco de dados."""
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
                    count = await ModelClass.filter(**filter_dict).delete_async()
                    console.print(
                        f"[bold green]Operação de deleção enviada:[/bold green] {count} registros processados"
                    )
                    console.print(
                        "[yellow]Nota:[/yellow] O Cassandra não retorna o número exato de registros deletados"
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
                f"[bold red]Erro:[/bold red] Tabela não encontrada no keyspace '{target_keyspace}'"
            )
            console.print(
                "[bold]Solução:[/bold] Use --keyspace para especificar o keyspace correto"
            )
            console.print(
                f"Exemplo: caspy query {model_name} {command} --keyspace seu_keyspace"
            )
        else:
            console.print(f"[bold red]Erro:[/bold red] {error_msg}")
        raise typer.Exit(1) from e
    finally:
        await safe_disconnect()


@app.command(
    help="Busca ou filtra objetos no banco de dados.\n\nOperadores suportados nos filtros:\n- __gt, __lt, __gte, __lte, __in, __contains, __startswith, __endswith\nExemplo: --filter idade__gt=30 --filter nome__in=joao,maria"
)
def query(
    model_name: str = typer.Argument(
        ...,
        help="Nome do modelo (ex: 'usuario', 'livro').",
        autocompletion=get_model_names,
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
        False, "--force", "-F", help="Forçar a exclusão sem confirmação."
    ),
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace a ser usado (sobrescreve CASPY_KEYSPACE).",
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
    asyncio.run(run_query(model_name, command, filters or [], limit, force, keyspace))


@app.command(help="Lista todos os modelos disponíveis.")
def models():
    """Lista todos os modelos disponíveis no módulo configurado."""
    config = get_config()
    search_paths = [
        os.getcwd(),  # Diretório atual
        os.path.join(os.getcwd(), "models"),  # Subdiretório 'models'
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "caspyorm"
        ),  # Modelos internos do caspyorm
        os.path.join(os.getcwd(), "tests", "fixtures"),  # Modelos de teste, se houver
    ]
    # Adiciona caminhos de modelo do arquivo de configuração
    for p in config["model_paths"]:
        abs_path = os.path.abspath(p)
        if os.path.isdir(abs_path):
            search_paths.append(abs_path)

    all_models = discover_models(search_paths)
    model_classes = list(all_models.values())

    if not model_classes:
        console.print(
            "[yellow]Nenhum modelo CaspyORM encontrado nos caminhos de busca.[/yellow]"
        )
        console.print(
            "\n[bold]Dica:[/bold] Verifique se seus arquivos de modelo estão no diretório atual ou em um subdiretório 'models'."
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
    return


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
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Testando conexão (keyspace: {target_keyspace})...", total=None
                )

                await connection.connect_async(
                    contact_points=config["hosts"], keyspace=target_keyspace
                )
                progress.update(task, description="Conectado! Testando query...")

                # Testar uma query simples
                await connection.execute_async(
                    "SELECT release_version FROM system.local"
                )

                progress.update(task, description="✅ Conexão bem-sucedida!")

            console.print(
                "[bold green]✅ Conexão com Cassandra estabelecida com sucesso![/bold green]"
            )
            console.print(f"[bold]Keyspace:[/bold] {target_keyspace}")
            console.print(f"[bold]Hosts:[/bold] {', '.join(config['hosts'])}")

        except Exception as e:
            console.print(f"[bold red]❌ Erro na conexão:[/bold red] {e}")
            console.print(
                "\n[bold]Dica:[/bold] Verifique se o Cassandra está rodando e acessível"
            )
            console.print(
                f"Configuração atual: hosts={config['hosts']}, keyspace={target_keyspace}"
            )
            raise typer.Exit(1) from e
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
            config["hosts"],
            "\n",
            ("CASPY_KEYSPACE: ", "bold"),
            config["keyspace"],
            "\n",
            ("CASPY_PORT: ", "bold"),
            str(config["port"]),
            "\n",
            ("Model Search Paths: ", "bold"),
            ", ".join(config["model_paths"]),
            "\n\n",
            ("Comandos disponíveis:", "bold"),
            "\n• query - Buscar e filtrar objetos",
            "\n• models - Listar modelos disponíveis",
            "\n• connect - Testar conexão",
            "\n• info - Esta ajuda",
            "\n• shell - Iniciar um shell interativo",
            "\n\n",
            ("Exemplos:", "bold"),
            "\n• caspy query usuario get --filter nome=joao",
            "\n• caspy query livro filter --filter autor_id=123 --limit 5 --keyspace biblioteca",
            "\n• caspy query usuario count --filter ativo=true",
            "\n• caspy connect --keyspace meu_keyspace",
        ),
        title="[bold blue]CaspyORM CLI[/bold blue]",
        border_style="blue",
    )
    console.print(info_panel)


class Migration(CaspyModel):
    __table_name__ = "caspyorm_migrations"
    id = UUID(primary_key=True)
    name = Text(required=True)
    applied_at = Timestamp(required=True)


@migrate_app.command(
    "init", help="Inicializa o sistema de migrações, criando a tabela de controle."
)
def migrate_init_sync(
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para inicializar (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Cria a tabela caspyorm_migrations se ela não existir."""
    asyncio.run(migrate_init_async(keyspace))


async def migrate_init_async(keyspace: Optional[str]):
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
                f"[bold green]Tabela 'caspyorm_migrations' verificada/criada no keyspace '{target_keyspace}'.[/bold green]"
            )

    except Exception as e:
        console.print(f"[bold red]❌ Erro ao inicializar migrações:[/bold red] {e}")
        raise typer.Exit(1) from e
    finally:
        await safe_disconnect()


@migrate_app.command("new", help="Cria um novo arquivo de migração.")
def migrate_new(
    name: str = typer.Argument(
        ..., help="Nome da migração (ex: 'create_users_table')."
    ),
):
    """Cria um novo arquivo de migração com um template básico."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"V{timestamp}__{name}.py"
    file_path = os.path.join("migrations", file_name)

    try:
        template_path = os.path.join(
            os.path.dirname(__file__), "migration_template.py.j2"
        )
        with open(template_path) as f:
            template_content = f.read()

        formatted_template = template_content.format(
            name=name, created_at=datetime.now()
        )

        with open(file_path, "w") as f:
            f.write(formatted_template)
        console.print(f"[bold green]Migração criada:[/bold green] {file_path}")
    except Exception as e:
        console.print(f"[bold red]Erro ao criar migração:[/bold red] {e}")
        raise typer.Exit(1) from e


@migrate_app.command(
    "status", help="Mostra o status das migrações (aplicadas vs. pendentes)."
)
def migrate_status_sync(
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para verificar (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Mostra o status das migrações (aplicadas vs. pendentes)."""
    asyncio.run(migrate_status_async(keyspace))


async def migrate_status_async(keyspace: Optional[str]):
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
            progress.update(
                task, description="Conectado! Buscando migrações aplicadas..."
            )

            applied_migrations_raw = await Migration.filter().all_async()
            applied_migrations = {m.name for m in applied_migrations_raw}  # type: ignore

            progress.update(task, description="Buscando arquivos de migração...")

            migration_files = []
            migrations_dir = "migrations"
            if os.path.exists(migrations_dir):
                for f in os.listdir(migrations_dir):
                    if f.startswith("V") and f.endswith(".py"):
                        # Extrai o nome da migração do nome do arquivo (ex: V20250706035805__create_users_table.py -> create_users_table)
                        name_part = f.split("__", 1)[-1]
                        migration_name = os.path.splitext(name_part)[0]
                        migration_files.append((f, migration_name))

            # Ordena as migrações por nome de arquivo (que inclui o timestamp)
            migration_files.sort()

            table = Table(title="Status das Migrações")
            table.add_column("Nome da Migração", style="cyan")
            table.add_column("Arquivo", style="magenta")
            table.add_column("Status", style="green")

            all_migration_names = {name for _, name in migration_files}

            # Adiciona migrações aplicadas que podem não ter um arquivo correspondente (ex: arquivo deletado)
            for applied_name in applied_migrations:
                if applied_name not in all_migration_names:
                    table.add_row(
                        applied_name,
                        "[red]Arquivo Ausente[/red]",
                        "[bold green]APLICADA[/bold green]",
                    )

            for file_name, migration_name in migration_files:
                status = (
                    "[bold green]APLICADA[/bold green]"
                    if migration_name in applied_migrations
                    else "[bold yellow]PENDENTE[/bold yellow]"
                )
                table.add_row(migration_name, file_name, status)

            console.print(table)

    except Exception as e:
        console.print(
            f"[bold red]❌ Erro ao verificar status das migrações:[/bold red] {e}"
        )
        raise typer.Exit(1) from e
    finally:
        await safe_disconnect()


@migrate_app.command("apply", help="Aplica migrações pendentes.")
def migrate_apply_sync(
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para aplicar (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Aplica migrações pendentes."""
    asyncio.run(migrate_apply_async(keyspace))


async def migrate_apply_async(keyspace: Optional[str]):
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
            progress.update(
                task, description="Conectado! Buscando migrações aplicadas..."
            )

            applied_migrations_raw = await Migration.filter().all_async()
            applied_migrations = {m.name for m in applied_migrations_raw}  # type: ignore

            progress.update(task, description="Buscando arquivos de migração...")

            migration_files = []
            migrations_dir = "migrations"
            if os.path.exists(migrations_dir):
                for f in os.listdir(migrations_dir):
                    if f.startswith("V") and f.endswith(".py"):
                        name_part = f.split("__", 1)[-1]
                        migration_name = os.path.splitext(name_part)[0]
                        migration_files.append((f, migration_name))

            migration_files.sort()  # Garante a ordem de aplicação

            pending_migrations = []
            for file_name, migration_name in migration_files:
                if migration_name not in applied_migrations:
                    pending_migrations.append((file_name, migration_name))

            if not pending_migrations:
                console.print(
                    "[bold green]✅ Nenhuma migração pendente para aplicar.[/bold green]"
                )
                return

            console.print(
                f"[bold yellow]Aplicando {len(pending_migrations)} migrações pendentes...[/bold yellow]"
            )
            for file_name, migration_name in pending_migrations:
                progress.update(
                    task,
                    description=f"Aplicando migração: {migration_name} ({file_name})...",
                )

                # Importa e executa a migração
                migration_full_path = os.path.join(migrations_dir, file_name)
                spec = importlib.util.spec_from_file_location(
                    migration_name, migration_full_path
                )
                if spec is None:
                    console.print(
                        f"[bold red]❌ Erro:[/bold red] Não foi possível carregar a especificação para a migração '{migration_name}'."
                    )
                    continue

                module = importlib.util.module_from_spec(spec)
                try:
                    if spec.loader is not None:  # type: ignore
                        spec.loader.exec_module(module)
                    if hasattr(module, "upgrade") and callable(module.upgrade):
                        await module.upgrade()  # type: ignore
                        # Registra a migração como aplicada
                        await Migration(
                            name=migration_name, applied_at=datetime.now()
                        ).save_async()
                        console.print(
                            f"[bold green]✅ Migração '{migration_name}' aplicada com sucesso.[/bold green]"
                        )
                    else:
                        console.print(
                            f"[bold red]❌ Erro:[/bold red] Migração '{migration_name}' não possui função 'upgrade'."
                        )
                except Exception as e:
                    console.print(
                        f"[bold red]❌ Erro ao aplicar migração '{migration_name}':[/bold red] {e}"
                    )
                    # Não levanta o erro para continuar com as próximas migrações, mas registra o problema

            console.print(
                "[bold green]✅ Processo de aplicação de migrações concluído.[/bold green]"
            )

    except Exception as e:
        console.print(f"[bold red]❌ Erro geral ao aplicar migrações:[/bold red] {e}")
        raise typer.Exit(1) from e
    finally:
        await safe_disconnect()


@migrate_app.command(
    "downgrade", help="Reverte a última migração aplicada ou uma migração específica."
)
def migrate_downgrade_sync(
    name: Optional[str] = typer.Argument(
        None,
        help="Nome da migração a ser revertida (ex: 'create_users_table'). Se omitido, reverte a última aplicada.",
    ),
    keyspace: Optional[str] = typer.Option(
        None,
        "--keyspace",
        "-k",
        help="Keyspace para reverter (sobrescreve CASPY_KEYSPACE).",
    ),
):
    """Reverte a última migração aplicada ou uma migração específica."""
    asyncio.run(migrate_downgrade_async(name, keyspace))


async def migrate_downgrade_async(name: Optional[str], keyspace: Optional[str]):
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
            progress.update(
                task, description="Conectado! Buscando migrações aplicadas..."
            )

            applied_migrations_raw = await Migration.filter().all_async()
            applied_migrations_dict = {m.name: m for m in applied_migrations_raw}  # type: ignore

            migration_files = []
            migrations_dir = "migrations"
            if os.path.exists(migrations_dir):
                for f in os.listdir(migrations_dir):
                    if f.startswith("V") and f.endswith(".py"):
                        name_part = f.split("__", 1)[-1]
                        migration_name = os.path.splitext(name_part)[0]
                        migration_files.append((f, migration_name))

            migration_files.sort(reverse=True)  # Ordem inversa para downgrade

            migration_to_downgrade = None
            if name:
                # Busca a migração específica pelo nome
                for file_name, mig_name in migration_files:
                    if mig_name == name:
                        if mig_name in applied_migrations_dict:
                            migration_to_downgrade = (
                                file_name,
                                mig_name,
                                applied_migrations_dict[mig_name].id,
                            )  # type: ignore
                            break
                        else:
                            console.print(
                                f"[bold red]Erro:[/bold red] Migração '{name}' não está aplicada."
                            )
                            return
                if not migration_to_downgrade:
                    console.print(
                        f"[bold red]Erro:[/bold red] Migração '{name}' não encontrada nos arquivos de migração."
                    )
                    return
            else:
                # Reverte a última migração aplicada
                if not applied_migrations_raw:
                    console.print(
                        "[bold yellow]Nenhuma migração aplicada para reverter.[/bold yellow]"
                    )
                    return

                # Encontra a última migração aplicada que também tem um arquivo correspondente
                for file_name, mig_name in migration_files:
                    if mig_name in applied_migrations_dict:
                        migration_to_downgrade = (
                            file_name,
                            mig_name,
                            applied_migrations_dict[mig_name].id,
                        )  # type: ignore
                        break

                if not migration_to_downgrade:
                    console.print(
                        "[bold yellow]Nenhuma migração aplicada com arquivo correspondente para reverter.[/bold yellow]"
                    )
                    return

            file_name, migration_name, migration_id = migration_to_downgrade
            console.print(
                f"[bold yellow]Revertendo migração: {migration_name} ({file_name})...[/bold yellow]"
            )
            progress.update(
                task,
                description=f"Revertendo migração: {migration_name} ({file_name})...",
            )

            # Importa e executa a migração
            migration_full_path = os.path.join(migrations_dir, file_name)
            spec = importlib.util.spec_from_file_location(
                migration_name, migration_full_path
            )
            if spec is None:
                console.print(
                    f"[bold red]❌ Erro:[/bold red] Não foi possível carregar a especificação para a migração '{migration_name}'."
                )
                return

            module = importlib.util.module_from_spec(spec)
            try:
                if spec.loader is not None:  # type: ignore
                    spec.loader.exec_module(module)
                if hasattr(module, "downgrade") and callable(module.downgrade):
                    await module.downgrade()  # type: ignore
                    # Remove o registro da migração
                    await Migration(id=migration_id).delete_async()
                    console.print(
                        f"[bold green]✅ Migração '{migration_name}' revertida com sucesso.[/bold green]"
                    )
                else:
                    console.print(
                        f"[bold red]❌ Erro:[/bold red] Migração '{migration_name}' não possui função 'downgrade'."
                    )
            except Exception as e:
                console.print(
                    f"[bold red]❌ Erro ao reverter migração '{migration_name}':[/bold red] {e}"
                )
                raise typer.Exit(
                    1
                )  # Levanta o erro para parar o processo em caso de falha no downgrade

            console.print(
                "[bold green]✅ Processo de reversão de migração concluído.[/bold green]"
            )

    except Exception as e:
        console.print(f"[bold red]❌ Erro geral ao reverter migrações:[/bold red] {e}")
        raise typer.Exit(1) from e
    finally:
        await safe_disconnect()


@app.command("version", help="Mostra a versão do CaspyORM CLI.")
def version_cmd():
    """Exibe a versão do CLI."""
    console.print(f"[bold blue]CaspyORM CLI[/bold blue] v{CLI_VERSION}")


@app.command(help="Inicia um shell interativo para executar comandos CaspyORM.")
def shell():
    """Inicia um shell interativo."""
    console.print("[bold green]Bem-vindo ao CaspyORM Shell![/bold green]")
    console.print("Digite 'exit' ou 'quit' para sair. Use 'help' para ver os comandos.")

    while True:
        try:
            command_line = Prompt.ask("[bold blue]caspy>[/bold blue]").strip()
            if not command_line:
                continue

            if command_line.lower() in ["exit", "quit"]:
                break

            # Divide a linha de comando em argumentos, como se fosse digitado no terminal
            # Adiciona 'caspy' como o primeiro argumento para simular a chamada real
            args = [sys.argv[0]] + command_line.split()

            # Salva sys.argv original e substitui para a execução do comando
            original_argv = sys.argv
            sys.argv = args

            try:
                app()  # Executa o comando usando a aplicação Typer
            except typer.Exit as e:
                if e.exit_code != 0:  # Não mostra erro para saídas normais (ex: --help)
                    console.print(
                        f"[bold red]Erro na execução do comando:[/bold red] Código de saída {e.exit_code}"
                    )
            except Exception as e:
                console.print(f"[bold red]Erro inesperado:[/bold red] {e}")
            finally:
                sys.argv = original_argv  # Restaura sys.argv

        except EOFError:
            console.print("\n[bold yellow]Saindo do shell.[/bold yellow]")
            break
        except Exception as e:
            console.print(f"[bold red]Erro no shell:[/bold red] {e}")


@app.callback()
def main():
    """CaspyORM CLI - Ferramenta de linha de comando para interagir com modelos CaspyORM."""
    pass


if __name__ == "__main__":
    app()
