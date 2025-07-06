"""
Testes para comandos do CLI do CaspyORM.
"""

import os
import sys
from unittest.mock import patch, MagicMock, AsyncMock
import re
import types
from caspyorm.model import Model
from caspyorm.fields import Text

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from cli.main import app, CLI_VERSION

def strip_ansi(text):
    ansi_escape = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', text)

def test_cli_help():
    """Testa se o comando --help funciona."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    result = runner.invoke(app, ['--help'])
    assert result.exit_code == 0
    assert "CaspyORM CLI" in result.stdout

def test_cli_version():
    """Testa se o comando version funciona."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    result = runner.invoke(app, ['version'])
    clean = strip_ansi(result.stdout + result.stderr)
    if str(CLI_VERSION) not in clean:
        raise AssertionError(f"Output do comando version não contém a versão esperada!\nOutput capturado:\n{clean}")

def test_connect_command_help():
    """Testa se o comando connect --help funciona."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    result = runner.invoke(app, ['connect', '--help'])
    assert result.exit_code == 0
    assert "Conecta ao Cassandra" in result.stdout

def test_models_command_help():
    """Testa se o comando models --help funciona."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    result = runner.invoke(app, ['models', '--help'])
    assert result.exit_code == 0
    assert "Lista todos os modelos" in result.stdout

def test_query_command_help():
    """Testa se o comando query --help funciona."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    result = runner.invoke(app, ['query', '--help'])
    assert result.exit_code == 0
    assert "Busca ou filtra objetos" in result.stdout

def test_info_command():
    """Testa se o comando info funciona."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    result = runner.invoke(app, ['info'])
    assert result.exit_code == 0
    assert "CaspyORM CLI" in result.stdout



def test_query_command_missing_arguments():
    """Testa se o comando query falha sem argumentos obrigatórios."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    result = runner.invoke(app, ['query'])
    assert result.exit_code != 0  # Deve falhar

def test_query_command_invalid_model():
    """Testa se o comando query falha com modelo inválido."""
    from typer.testing import CliRunner
    runner = CliRunner()
    
    with patch('cli.main.find_model_class', side_effect=SystemExit(1)):
        result = runner.invoke(app, ['query', 'invalid_model', 'get'])
        assert result.exit_code != 0

def test_models_command_with_mock_module():
    """Testa o comando models com módulo mockado."""
    from typer.testing import CliRunner
    runner = CliRunner()
    # Mock do módulo com modelos
    mock_module = types.ModuleType('mock_module')
    class TestModel(Model):
        __table_name__ = 'test_table'
        model_fields = {'id': Text(primary_key=True), 'name': Text()}
    setattr(mock_module, 'TestModel', TestModel)
    with patch('cli.main.importlib.import_module', return_value=mock_module):
        with patch.dict(os.environ, {"CASPY_MODELS_PATH": "mock_module"}):
            result = runner.invoke(app, ['models'])
            assert result.exit_code == 0
            assert "TestModel" in result.stdout

def test_models_command_no_models():
    """Testa o comando models quando não há modelos."""
    from typer.testing import CliRunner
    runner = CliRunner()
    # Mock do módulo sem modelos
    mock_module = types.ModuleType('mock_module')
    with patch('cli.main.importlib.import_module', return_value=mock_module):
        with patch.dict(os.environ, {"CASPY_MODELS_PATH": "mock_module"}):
            result = runner.invoke(app, ['models'])
            assert result.exit_code == 0
            assert "Nenhum modelo CaspyORM encontrado nos caminhos de busca." in result.stdout

if __name__ == '__main__':
    # Executar testes de comandos
    test_cli_help()
    test_cli_version()
    test_connect_command_help()
    test_models_command_help()
    test_query_command_help()
    test_info_command()
    test_show_models_path_error()
    test_query_command_missing_arguments()
    test_models_command_with_mock_module()
    test_models_command_no_models()
    print("✅ Todos os testes de comandos passaram!") 