import os
import subprocess
import sys
import time
import pytest
from caspyorm.core.connection import connect, disconnect, get_session

MIGRATIONS_DIR = "migrations"
KEYSPACE = "nyc_data"
TABLE_NAME = "mig_test_table"

# Define o caminho para o diretório 'src'
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')

@pytest.fixture(scope="module", autouse=True)
def cleanup_migrations(db_connection):
    # Cria caspy.toml temporário
    toml_path = "caspy.toml"
    toml_content = '''
[cassandra]
hosts = ["localhost"]
keyspace = "nyc_data"
port = 9042
'''
    with open(toml_path, "w") as f:
        f.write(toml_content)
    # Limpa arquivos de migration antigos
    if os.path.exists(MIGRATIONS_DIR):
        for f_ in os.listdir(MIGRATIONS_DIR):
            if f_.startswith("V") and f_.endswith(".py"):
                os.remove(os.path.join(MIGRATIONS_DIR, f_))
    yield
    # Limpeza pós-teste
    if os.path.exists(MIGRATIONS_DIR):
        for f_ in os.listdir(MIGRATIONS_DIR):
            if f_.startswith("V") and f_.endswith(".py"):
                os.remove(os.path.join(MIGRATIONS_DIR, f_))
    if os.path.exists(toml_path):
        os.remove(toml_path)

def run_cli(cmd):
    """Executa comando CLI com o ambiente correto e retorna a saída."""
    env = os.environ.copy()
    env['PYTHONPATH'] = SRC_DIR + os.pathsep + env.get('PYTHONPATH', '')
    cli_command = [sys.executable, "-m", "caspyorm_cli.main"] + cmd
    result = subprocess.run(cli_command, capture_output=True, text=True, env=env)
    print(result.stdout)
    print(result.stderr)
    return result

def test_migration_flow(cleanup_migrations):
    # 1. Init
    res = run_cli(["migrate", "init", "--keyspace", KEYSPACE])
    assert "Tabela 'caspyorm_migrations' pronta" in res.stdout

    # 2. New migration
    res = run_cli(["migrate", "new", "create_mig_test_table"])
    assert "Migração criada" in res.stdout
    # Descobre o nome do arquivo
    files = [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".py") and "mig_test_table" in f]
    assert files, "Arquivo de migration não criado"
    mig_file = files[0]

    # 3. Edita migration para criar tabela
    mig_path = os.path.join(MIGRATIONS_DIR, mig_file)
    with open(mig_path, "a") as f:
        f.write(f'''
def upgrade():
    from caspyorm.core.connection import get_session
    session = get_session()
    session.execute("CREATE TABLE IF NOT EXISTS {TABLE_NAME} (id int PRIMARY KEY, nome text)")

def downgrade():
    from caspyorm.core.connection import get_session
    session = get_session()
    session.execute("DROP TABLE IF EXISTS {TABLE_NAME}")
''')

    # 4. Status (deve estar pendente)
    res = run_cli(["migrate", "status", "--keyspace", KEYSPACE])
    assert "PENDENTE" in res.stdout

    # 5. Apply
    res = run_cli(["migrate", "apply", "--keyspace", KEYSPACE])
    assert "aplicada com sucesso" in res.stdout or "concluído" in res.stdout
    time.sleep(1)  # Aguarda propagação

    # 6. Verifica se a tabela foi criada
    connect(contact_points=["localhost"], keyspace=KEYSPACE, port=9042)
    session = get_session()
    tables = [row.table_name for row in session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{KEYSPACE}'")]
    assert TABLE_NAME in tables
    disconnect()

    # 7. Status (deve estar aplicada)
    res = run_cli(["migrate", "status", "--keyspace", KEYSPACE])
    assert "APLICADA" in res.stdout

    # 8. Downgrade
    res = run_cli(["migrate", "downgrade", "--keyspace", KEYSPACE, "--force"])
    assert "revertida com sucesso" in res.stdout
    time.sleep(1)
    # 9. Verifica se a tabela foi removida
    connect(contact_points=["localhost"], keyspace=KEYSPACE, port=9042)
    session = get_session()
    tables = [row.table_name for row in session.execute(f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{KEYSPACE}'")]
    assert TABLE_NAME not in tables
    disconnect()

def test_migration_upgrade_error(cleanup_migrations):
    # Cria migration com erro em upgrade
    res = run_cli(["migrate", "new", "erro_upgrade"])
    assert "Migração criada" in res.stdout
    files = [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".py") and "erro_upgrade" in f]
    assert files, "Arquivo de migration não criado"
    mig_file = files[0]
    mig_path = os.path.join(MIGRATIONS_DIR, mig_file)
    with open(mig_path, "a") as f:
        f.write("\ndef upgrade():\n    raise Exception('erro proposital upgrade')\n\ndef downgrade():\n    pass\n")
    # Tenta aplicar
    res = run_cli(["migrate", "apply", "--keyspace", KEYSPACE])
    assert "Erro ao aplicar migração" in res.stdout or "erro proposital upgrade" in res.stdout
    # Remove a migration com erro para não interferir nos próximos testes
    os.remove(mig_path)

def test_migration_downgrade_error(cleanup_migrations):
    # Cria migration com erro em downgrade
    res = run_cli(["migrate", "new", "erro_downgrade"])
    assert "Migração criada" in res.stdout
    files = [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".py") and "erro_downgrade" in f]
    assert files, "Arquivo de migration não criado"
    mig_file = files[0]
    mig_path = os.path.join(MIGRATIONS_DIR, mig_file)
    with open(mig_path, "a") as f:
        f.write(f'''
def upgrade():
    pass

def downgrade():
    raise Exception('erro proposital downgrade')
''')
    # Aplica
    res = run_cli(["migrate", "apply", "--keyspace", KEYSPACE])
    assert "aplicada com sucesso" in res.stdout or "concluído" in res.stdout
    # Tenta reverter
    res = run_cli(["migrate", "downgrade", "--keyspace", KEYSPACE, "--force"])
    assert "Erro ao reverter migração" in res.stdout or "erro proposital downgrade" in res.stdout

def test_migration_corrupted_file(cleanup_migrations):
    # Cria migration corrompida (sem upgrade/downgrade)
    res = run_cli(["migrate", "new", "corrompida"])
    assert "Migração criada" in res.stdout
    files = [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".py") and "corrompida" in f]
    assert files, "Arquivo de migration não criado"
    mig_file = files[0]
    mig_path = os.path.join(MIGRATIONS_DIR, mig_file)
    with open(mig_path, "w") as f:
        f.write("# arquivo corrompido\n")
    # Tenta aplicar
    res = run_cli(["migrate", "apply", "--keyspace", KEYSPACE])
    assert "não possui função 'upgrade'" in res.stdout or "Erro ao aplicar migração" in res.stdout
 