"""
Testes para o CLI do CaspyORM.
"""

import pytest
import os
import sys
from unittest.mock import patch, MagicMock, AsyncMock
from typer.testing import CliRunner
from rich.console import Console
import types
from src.caspyorm.core.model import Model
from src.caspyorm.core.fields import Text

# Adicionar o diretório raiz ao path para importar o CLI
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from cli.main import app, get_config, find_model_class, parse_filters

# Configurar o runner do Typer
runner = CliRunner()

class TestCLIConfiguration:
    """Testes para configuração do CLI."""
    
    def test_get_config_defaults(self):
        """Testa se get_config() retorna valores padrão corretos."""
        with patch.dict(os.environ, {}, clear=True):
            config = get_config()
            
            assert config['hosts'] == ['127.0.0.1']
            assert config['keyspace'] == 'caspyorm_app'
            assert config['port'] == 9042
            assert config['model_paths'] == []
    
    def test_get_config_with_env_vars(self):
        """Testa se get_config() lê variáveis de ambiente corretamente."""
        env_vars = {
            'CASPY_HOSTS': 'host1,host2,host3',
            'CASPY_KEYSPACE': 'test_keyspace',
            'CASPY_PORT': '9043',
            'CASPY_MODELS_PATH': 'test_models'
        }
        
        with patch.dict(os.environ, env_vars, clear=True):
            config = get_config()
            
            assert config['hosts'] == ['host1', 'host2', 'host3']
            assert config['keyspace'] == 'test_keyspace'
            assert config['port'] == 9043
            assert config['model_paths'] == ['test_models']

class TestCLICommands:
    """Testes para comandos do CLI."""
    
    def test_cli_help(self):
        """Testa se o comando --help funciona."""
        result = runner.invoke(app, ['--help'])
        assert result.exit_code == 0
        assert "CaspyORM CLI" in result.stdout
    
    def test_cli_version(self):
        """Testa se o comando version funciona."""
        result = runner.invoke(app, ['version'])
        assert result.exit_code == 0
        assert "CaspyORM CLI v" in result.stdout
    
    def test_connect_command_help(self):
        """Testa se o comando connect --help funciona."""
        result = runner.invoke(app, ['connect', '--help'])
        assert result.exit_code == 0
        assert "Conecta ao Cassandra" in result.stdout
    
    def test_models_command_help(self):
        """Testa se o comando models --help funciona."""
        result = runner.invoke(app, ['models', '--help'])
        assert result.exit_code == 0
        assert "Lista todos os modelos" in result.stdout
    
    def test_query_command_help(self):
        """Testa se o comando query --help funciona."""
        result = runner.invoke(app, ['query', '--help'])
        assert result.exit_code == 0
        assert "Busca ou filtra objetos" in result.stdout
    
    def test_info_command(self):
        """Testa se o comando info funciona."""
        result = runner.invoke(app, ['info'])
        assert result.exit_code == 0
        assert "CaspyORM CLI" in result.stdout

class TestCLIUtilities:
    """Testes para funções utilitárias do CLI."""
    
    def test_parse_filters_empty(self):
        """Testa parse_filters com lista vazia."""
        result = parse_filters([])
        assert result == {}
    
    def test_parse_filters_string_values(self):
        """Testa parse_filters com valores string."""
        filters = ['name=João', 'email=joao@email.com']
        result = parse_filters(filters)
        
        assert result['name'] == 'João'
        assert result['email'] == 'joao@email.com'
    
    def test_parse_filters_boolean_values(self):
        """Testa parse_filters com valores booleanos."""
        filters = ['ativo=true', 'deletado=false']
        result = parse_filters(filters)
        
        assert result['ativo'] is True
        assert result['deletado'] is False
    
    def test_parse_filters_numeric_values(self):
        """Testa parse_filters com valores numéricos."""
        filters = ['idade=25', 'altura=1.75']
        result = parse_filters(filters)
        
        assert result['idade'] == 25
        assert result['altura'] == 1.75
    
    def test_parse_filters_invalid_format(self):
        """Testa parse_filters com formato inválido."""
        filters = ['invalid_filter', 'name=value', 'another_invalid']
        result = parse_filters(filters)
        
        assert result['name'] == 'value'
        assert len(result) == 1  # Apenas o válido deve ser processado



class TestCLIAsyncFunctions:
    """Testes para funções assíncronas do CLI."""
    
    @pytest.mark.asyncio
    async def test_safe_disconnect_success(self):
        """Testa safe_disconnect quando a desconexão é bem-sucedida."""
        from cli.main import safe_disconnect
        
        with patch('cli.main.connection') as mock_connection:
            mock_connection.disconnect_async = AsyncMock()
            
            await safe_disconnect()
            
            mock_connection.disconnect_async.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_safe_disconnect_exception(self):
        """Testa safe_disconnect quando a desconexão falha."""
        from cli.main import safe_disconnect
        
        with patch('cli.main.connection') as mock_connection:
            mock_connection.disconnect_async = AsyncMock(side_effect=Exception("Connection error"))
            
            # Não deve levantar exceção
            await safe_disconnect()
            
            mock_connection.disconnect_async.assert_called_once()

class TestCLIIntegration:
    """Testes de integração para o CLI."""
    
    @patch('cli.main.connection')
    @patch('cli.main.find_model_class')
    def test_query_command_mock(self, mock_find_model, mock_connection):
        """Testa o comando query com mocks."""
        import os
        from unittest.mock import patch, AsyncMock
        import types
        from src.caspyorm.core.model import Model
        from src.caspyorm.core.fields import Text
        # Mock do modelo real
        class User(Model):
            __table_name__ = 'user'
            model_fields = {'id': Text(primary_key=True), 'name': Text()}
        User.get_async = AsyncMock(return_value=User(id='123', name='Test'))
        mock_find_model.return_value = User
        # Mock da conexão
        mock_connection.connect_async = AsyncMock()
        mock_connection.disconnect_async = AsyncMock()
        with patch.dict(os.environ, {"CASPY_MODELS_PATH": "mock_module"}):
            result = runner.invoke(app, ['query', 'user', 'get', '--filter', 'id=123'])
            assert result.exit_code == 0
    
    @patch('cli.main.importlib.import_module')
    def test_models_command_mock(self, mock_import_module):
        """Testa o comando models com mocks."""
        # Mock do módulo com modelos
        mock_module = types.ModuleType('mock_module')
        class TestModel(Model):
            __table_name__ = 'test_table'
            model_fields = {'id': Text(primary_key=True), 'name': Text()}
        setattr(mock_module, 'TestModel', TestModel)
        mock_import_module.return_value = mock_module
        with patch.dict(os.environ, {"CASPY_MODELS_PATH": "mock_module"}):
            result = runner.invoke(app, ['models'])
            assert result.exit_code == 0
            assert "TestModel" in result.stdout

class TestCLIEnvironment:
    """Testes para configuração de ambiente do CLI."""
    
    def test_cli_with_custom_keyspace(self):
        """Testa se o CLI aceita keyspace customizado."""
        with patch.dict(os.environ, {'CASPY_KEYSPACE': 'custom_keyspace'}, clear=True):
            config = get_config()
            assert config['keyspace'] == 'custom_keyspace'
    
    def test_cli_with_multiple_hosts(self):
        """Testa se o CLI aceita múltiplos hosts."""
        with patch.dict(os.environ, {'CASPY_HOSTS': 'host1,host2,host3'}, clear=True):
            config = get_config()
            assert config['hosts'] == ['host1', 'host2', 'host3']
    
    def test_cli_with_custom_port(self):
        """Testa se o CLI aceita porta customizada."""
        with patch.dict(os.environ, {'CASPY_PORT': '9043'}, clear=True):
            config = get_config()
            assert config['port'] == 9043

# Removido: execução manual de testes - use pytest para executar os testes 