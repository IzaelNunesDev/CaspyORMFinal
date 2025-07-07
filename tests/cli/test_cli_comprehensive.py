"""
Testes abrangentes para a CLI CaspyORM.
Testa todos os comandos principais e funcionalidades.
"""

import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from typer.testing import CliRunner
import sys
import os

# Adicionar o diretório raiz ao path para importar os módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from cli.main import app
from caspyorm import connection
from caspyorm.model import Model
from caspyorm.fields import UUID, Text, Integer, Boolean


class TestUser(Model):
    """Modelo de teste para os testes da CLI."""
    __table_name__ = "test_users"
    
    id = UUID(primary_key=True)
    name = Text()
    email = Text()
    age = Integer()
    active = Boolean(default=True)


class TestCLIComprehensive:
    """Bateria completa de testes para a CLI."""
    
    @pytest.fixture
    def runner(self):
        """Fixture para o CliRunner."""
        return CliRunner()
    
    @pytest.fixture
    def mock_connection(self):
        """Mock da conexão com Cassandra."""
        with patch('cli.main.connection') as mock_conn:
            mock_conn.connect_async = AsyncMock()
            mock_conn.execute_async = AsyncMock()
            mock_conn.disconnect_async = AsyncMock()
            yield mock_conn
    
    @pytest.fixture
    def mock_config(self):
        """Mock da configuração."""
        with patch('cli.main.get_config') as mock_get_config:
            mock_get_config.return_value = {
                "hosts": ["127.0.0.1"],
                "keyspace": "test_keyspace",
                "port": 9042,
                "model_paths": ["."]
            }
            yield mock_get_config
    
    @pytest.fixture
    def mock_discover_models(self):
        """Mock da descoberta de modelos."""
        with patch('cli.main.discover_models') as mock_discover:
            mock_discover.return_value = {
                'testuser': TestUser
            }
            yield mock_discover

    def test_help_command(self, runner):
        """Testa o comando de ajuda principal."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "CaspyORM CLI" in result.output
        assert "query" in result.output
        assert "models" in result.output
        assert "migrate" in result.output
        assert "sql" in result.output

    def test_version_command(self, runner):
        """Testa o comando de versão."""
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert "CaspyORM CLI" in result.output

    def test_info_command(self, runner, mock_config):
        """Testa o comando de informações."""
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        assert "CaspyORM CLI" in result.output
        assert "Configuração:" in result.output

    def test_connect_command_success(self, runner, mock_connection, mock_config):
        """Testa o comando de conexão com sucesso."""
        # Mock do resultado da query de teste
        mock_result = MagicMock()
        mock_connection.execute_async.return_value = mock_result
        
        result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0
        assert "Conexão com Cassandra estabelecida" in result.output

    def test_connect_command_failure(self, runner, mock_connection, mock_config):
        """Testa o comando de conexão com falha."""
        mock_connection.connect_async.side_effect = Exception("Connection failed")
        
        result = runner.invoke(app, ["connect"])
        assert result.exit_code == 1
        assert "Erro na conexão" in result.output

    def test_models_command(self, runner, mock_discover_models):
        """Testa o comando de listagem de modelos."""
        result = runner.invoke(app, ["models"])
        assert result.exit_code == 0
        assert "Modelos CaspyORM disponíveis" in result.output

    def test_models_command_no_models(self, runner):
        """Testa o comando de modelos quando não há modelos."""
        with patch('cli.main.discover_models') as mock_discover:
            mock_discover.return_value = {}
            
            result = runner.invoke(app, ["models"])
            assert result.exit_code == 0
            assert "Nenhum modelo CaspyORM encontrado" in result.output

    def test_migrate_help(self, runner):
        """Testa a ajuda do comando migrate."""
        result = runner.invoke(app, ["migrate", "--help"])
        assert result.exit_code == 0
        assert "Comandos para gerenciar migrações" in result.output

    def test_migrate_init(self, runner, mock_connection, mock_config):
        """Testa o comando de inicialização de migrações."""
        with patch('cli.main.Migration') as mock_migration:
            mock_migration.sync_table_async = AsyncMock()
            
            result = runner.invoke(app, ["migrate", "init"])
            assert result.exit_code == 0
            assert "Tabela de migrações verificada" in result.output

    def test_migrate_status(self, runner, mock_connection, mock_config):
        """Testa o comando de status das migrações."""
        # Mock dos resultados das queries
        mock_migrations_result = MagicMock()
        mock_migrations_result.__iter__ = lambda self: iter([
            MagicMock(version="V20250706040802__create_users_table.py", applied_at="2025-07-06 10:00:00"),
            MagicMock(version="V20250706072527__nyc_restaurants_data.py", applied_at="2025-07-06 10:30:00")
        ])
        
        mock_files_result = MagicMock()
        mock_files_result.__iter__ = lambda self: iter([
            "V20250706040802__create_users_table.py",
            "V20250706072527__nyc_restaurants_data.py"
        ])
        
        mock_connection.execute_async.side_effect = [mock_migrations_result, mock_files_result]
        
        result = runner.invoke(app, ["migrate", "status"])
        assert result.exit_code == 0
        assert "Status das Migrações" in result.output

    def test_migrate_new(self, runner):
        """Testa o comando de criação de nova migração."""
        with patch('cli.main.os.path.exists') as mock_exists, \
             patch('cli.main.open', create=True) as mock_open, \
             patch('cli.main.datetime') as mock_datetime:
            
            mock_exists.return_value = True
            mock_datetime.now.return_value.strftime.return_value = "20250706120000"
            
            result = runner.invoke(app, ["migrate", "new", "test_migration"])
            assert result.exit_code == 0
            assert "Migração criada:" in result.output

    def test_query_command_help(self, runner):
        """Testa a ajuda do comando query."""
        result = runner.invoke(app, ["query", "--help"])
        assert result.exit_code == 0
        assert "Busca ou filtra objetos" in result.output

    def test_query_command_invalid_model(self, runner, mock_discover_models):
        """Testa o comando query com modelo inválido."""
        result = runner.invoke(app, ["query", "invalid_model", "get"])
        assert result.exit_code == 1
        assert "Modelo 'invalid_model' não encontrado" in result.output

    def test_query_command_invalid_command(self, runner, mock_discover_models):
        """Testa o comando query com comando inválido."""
        result = runner.invoke(app, ["query", "testuser", "invalid_command"])
        assert result.exit_code == 1
        assert "Comando inválido" in result.output

    def test_query_command_delete_without_filters(self, runner, mock_discover_models):
        """Testa o comando query delete sem filtros."""
        result = runner.invoke(app, ["query", "testuser", "delete"])
        assert result.exit_code == 1
        assert "ATENÇÃO: Comando 'delete' sem filtros" in result.output

    def test_sql_command_help(self, runner):
        """Testa a ajuda do comando sql."""
        result = runner.invoke(app, ["sql", "--help"])
        assert result.exit_code == 0
        assert "Executa uma query SQL direta" in result.output

    def test_sql_command_success(self, runner, mock_connection, mock_config):
        """Testa o comando sql com sucesso."""
        # Mock do resultado da query
        mock_row = MagicMock()
        mock_row._fields = ('id', 'name', 'cuisine')
        mock_row.id = '123'
        mock_row.name = 'Test Restaurant'
        mock_row.cuisine = 'Italian'
        
        mock_result = [mock_row]
        mock_connection.execute_async.return_value = mock_result
        
        result = runner.invoke(app, ["sql", "SELECT * FROM restaurants LIMIT 1"])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        assert "Test Restaurant" in result.output

    def test_sql_command_no_results(self, runner, mock_connection, mock_config):
        """Testa o comando sql sem resultados."""
        mock_connection.execute_async.return_value = []
        
        result = runner.invoke(app, ["sql", "SELECT * FROM empty_table"])
        assert result.exit_code == 0
        assert "não retornou resultados" in result.output

    def test_sql_command_error(self, runner, mock_connection, mock_config):
        """Testa o comando sql com erro."""
        mock_connection.execute_async.side_effect = Exception("SQL Error")
        
        result = runner.invoke(app, ["sql", "SELECT * FROM invalid_table"])
        assert result.exit_code == 1
        assert "Erro ao executar query" in result.output

    def test_shell_command(self, runner):
        """Testa o comando shell."""
        # Mock do IPython para evitar erro de importação
        with patch('cli.main.IPython', create=True) as mock_ipython:
            result = runner.invoke(app, ["shell"])
            assert result.exit_code == 0
            # O shell deve tentar iniciar o IPython

    def test_parse_filters(self):
        """Testa a função de parsing de filtros."""
        from cli.main import parse_filters
        
        filters = ["name=joao", "age__gt=30", "city__in=sp,ny"]
        result = parse_filters(filters)
        
        assert result["name"] == "joao"
        assert result["age__gt"] == 30
        assert result["city__in"] == ["sp", "ny"]

    def test_parse_filters_empty(self):
        """Testa a função de parsing de filtros vazia."""
        from cli.main import parse_filters
        
        result = parse_filters([])
        assert result == {}

    def test_get_model_names(self):
        """Testa a função de obtenção de nomes de modelos."""
        with patch('cli.main.discover_models') as mock_discover:
            mock_discover.return_value = {
                'user': TestUser,
                'post': MagicMock(__name__='Post')
            }
            
            from cli.main import get_model_names
            names = get_model_names()
            assert 'user' in names
            assert 'post' in names

    def test_find_model_class_success(self):
        """Testa a função de busca de classe de modelo com sucesso."""
        with patch('cli.main.discover_models') as mock_discover:
            mock_discover.return_value = {'testuser': TestUser}
            
            from cli.main import find_model_class
            model_class = find_model_class('testuser')
            assert model_class == TestUser

    def test_find_model_class_not_found(self):
        """Testa a função de busca de classe de modelo não encontrada."""
        with patch('cli.main.discover_models') as mock_discover:
            mock_discover.return_value = {}
            
            from cli.main import find_model_class
            import typer
            with pytest.raises(typer.Exit):
                find_model_class('invalid')

    def test_get_config_defaults(self):
        """Testa a função de configuração com valores padrão."""
        with patch.dict(os.environ, {}, clear=True):
            from cli.main import get_config
            config = get_config()
            
            assert config["hosts"] == ["127.0.0.1"]
            assert config["keyspace"] == "caspyorm_app"
            assert config["port"] == 9042

    def test_get_config_from_env(self):
        """Testa a função de configuração com variáveis de ambiente."""
        with patch.dict(os.environ, {
            "CASPY_HOSTS": "localhost,192.168.1.1",
            "CASPY_KEYSPACE": "test_keyspace",
            "CASPY_PORT": "9043"
        }, clear=True):
            from cli.main import get_config
            config = get_config()
            
            assert config["hosts"] == ["localhost", "192.168.1.1"]
            assert config["keyspace"] == "test_keyspace"
            assert config["port"] == 9043

    @pytest.mark.asyncio
    async def test_safe_disconnect(self):
        """Testa a função de desconexão segura."""
        with patch('cli.main.connection') as mock_conn:
            mock_conn.disconnect_async = AsyncMock()
            
            from cli.main import safe_disconnect
            await safe_disconnect()
            
            mock_conn.disconnect_async.assert_called_once()

    def test_run_safe_cli_decorator(self):
        """Testa o decorator de execução segura da CLI."""
        from cli.main import run_safe_cli
        
        @run_safe_cli
        def test_function():
            return "success"
        
        result = test_function()
        assert result == "success"

    def test_run_safe_cli_decorator_with_exception(self):
        """Testa o decorator de execução segura da CLI com exceção."""
        from cli.main import run_safe_cli
        import typer
        
        @run_safe_cli
        def test_function():
            raise ValueError("Test error")
        
        with pytest.raises(typer.Exit):
            test_function()


class TestCLIIntegration:
    """Testes de integração da CLI."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    def test_full_workflow(self, runner):
        """Testa um fluxo completo da CLI."""
        # Testa ajuda
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        
        # Testa versão
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        
        # Testa info
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        
        # Testa modelos
        result = runner.invoke(app, ["models"])
        assert result.exit_code == 0

    def test_migrate_workflow(self, runner):
        """Testa o fluxo completo de migrações."""
        with patch('cli.main.connection') as mock_conn, \
             patch('cli.main.Migration') as mock_migration, \
             patch('cli.main.get_config') as mock_config:
            
            mock_conn.connect_async = AsyncMock()
            mock_conn.execute_async = AsyncMock()
            mock_conn.disconnect_async = AsyncMock()
            mock_migration.sync_table_async = AsyncMock()
            mock_config.return_value = {
                "hosts": ["127.0.0.1"],
                "keyspace": "test_keyspace",
                "port": 9042,
                "model_paths": ["."]
            }
            
            # Mock dos resultados das queries para status
            mock_migrations_result = MagicMock()
            mock_migrations_result.__iter__ = lambda self: iter([
                MagicMock(version="V20250706040802__create_users_table.py", applied_at="2025-07-06 10:00:00")
            ])
            
            mock_files_result = MagicMock()
            mock_files_result.__iter__ = lambda self: iter([
                "V20250706040802__create_users_table.py"
            ])
            
            mock_conn.execute_async.side_effect = [mock_migrations_result, mock_files_result]
            
            # Testa init
            result = runner.invoke(app, ["migrate", "init"])
            assert result.exit_code == 0
            
            # Testa status
            result = runner.invoke(app, ["migrate", "status"])
            assert result.exit_code == 0

    def test_query_workflow(self, runner):
        """Testa o fluxo completo de queries."""
        with patch('cli.main.connection') as mock_conn, \
             patch('cli.main.discover_models') as mock_discover, \
             patch('cli.main.get_config') as mock_config, \
             patch('cli.main.find_model_class') as mock_find_model:
            
            mock_conn.connect_async = AsyncMock()
            mock_conn.execute_async = AsyncMock()
            mock_conn.disconnect_async = AsyncMock()
            mock_discover.return_value = {'testuser': TestUser}
            mock_find_model.return_value = TestUser
            mock_config.return_value = {
                "hosts": ["127.0.0.1"],
                "keyspace": "test_keyspace",
                "port": 9042,
                "model_paths": ["."]
            }
            
            # Mock do resultado da query count
            mock_count_result = MagicMock()
            mock_count_result.one.return_value = MagicMock(count=5)
            mock_conn.execute_async.return_value = mock_count_result
            
            # Testa count
            result = runner.invoke(app, ["query", "testuser", "count"])
            assert result.exit_code == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 