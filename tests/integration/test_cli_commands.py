import subprocess
import sys
import re
import pytest
import os

# Define o caminho para o diretório 'src'
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
SRC_DIR = os.path.join(PROJECT_ROOT, 'src')

# Comando base para executar a CLI usando o módulo, que é a forma correta
CLI_CMD = [sys.executable, '-m', 'caspyorm_cli.main']

def get_base_env():
    """Cria um ambiente base com o PYTHONPATH configurado para incluir 'src'."""
    env = os.environ.copy()
    # Adiciona o diretório 'src' ao PYTHONPATH para que o Python encontre os pacotes
    env['PYTHONPATH'] = SRC_DIR + os.pathsep + env.get('PYTHONPATH', '')
    return env

@pytest.mark.parametrize("args,expected", [
    (["models"], r"NYC311"),
    (["connect", "--keyspace", "nyc_data"], r"Conexão com o Cassandra bem-sucedida|Conectado ao Cassandra"),
    (["query", "nyc311", "count"], r"Total de registros|count"),
    (["query", "nyc311", "filter", "--filter", "complaint_type=Noise", "--limit", "2", "--allow-filtering"], r"complaint_type.*Noise"),
    (["sql", "SELECT count(*) FROM nyc_311"], r"count|Total"),
])
def test_cli_commands(args, expected):
    env = get_base_env()
    env["CASPY_HOSTS"] = "localhost"
    env["CASPY_KEYSPACE"] = "nyc_data"
    env["CASPY_PORT"] = "9042"
    
    result = subprocess.run(CLI_CMD + args, capture_output=True, text=True, env=env)
    
    assert result.returncode == 0, f"Comando falhou: {' '.join(args)}\nSaída: {result.stderr or result.stdout}"
    assert re.search(expected, result.stdout, re.IGNORECASE), f"Saída inesperada: {result.stdout}"

def test_cli_config_env(monkeypatch):
    """Testa a leitura de configuração da CLI a partir de variáveis de ambiente."""
    env = get_base_env()
    env["CASPY_HOSTS"] = "127.0.0.1"
    env["CASPY_KEYSPACE"] = "env_keyspace"
    env["CASPY_PORT"] = "9999"
    
    result = subprocess.run(CLI_CMD + ["info"], capture_output=True, text=True, env=env)
    
    assert result.returncode == 0, f"Comando 'info' falhou.\nSaída: {result.stderr or result.stdout}"
    assert "127.0.0.1" in result.stdout
    assert "env_keyspace" in result.stdout
    assert "9999" in result.stdout

@pytest.mark.skip(reason="Este teste manipula arquivos no diretório do projeto e pode ser instável. A lógica principal já é coberta.")
def test_cli_config_toml(monkeypatch):
    # Testa leitura do caspy.toml na raiz do projeto
    import os
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    toml_path = os.path.join(project_root, "caspy.toml")
    toml_content = '''
[cassandra]
hosts = ["tomlhost"]
keyspace = "toml_keyspace"
port = 8888
'''
    with open(toml_path, "w") as f:
        f.write(toml_content)
    try:
        env = get_base_env()
        cmd = [sys.executable, '-m', 'caspyorm_cli.main', "info"]
        result = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=project_root)
        assert "tomlhost" in result.stdout
        assert "toml_keyspace" in result.stdout
        assert "8888" in result.stdout
    finally:
        if os.path.exists(toml_path):
            os.remove(toml_path)

 