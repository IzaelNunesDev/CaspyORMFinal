"""
Configuração do pytest para testes do CaspyORM CLI.
"""

import pytest
import os
import sys
from unittest.mock import MagicMock, AsyncMock

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

@pytest.fixture
def mock_connection():
    """Fixture para mock da conexão Cassandra."""
    with pytest.MonkeyPatch().context() as m:
        mock_conn = MagicMock()
        mock_conn.connect_async = AsyncMock()
        mock_conn.disconnect_async = AsyncMock()
        mock_conn.execute_async = AsyncMock()
        
        m.setattr('cli.main.connection', mock_conn)
        yield mock_conn

@pytest.fixture
def mock_model():
    """Fixture para mock de modelo CaspyORM."""
    model = MagicMock()
    model.__name__ = 'TestModel'
    model.__table_name__ = 'test_table'
    model.model_fields = {'id': None, 'name': None, 'email': None}
    model.get_async = AsyncMock()
    model.filter = MagicMock()
    return model

@pytest.fixture
def sample_config():
    """Fixture para configuração de exemplo."""
    return {
        'hosts': ['localhost'],
        'keyspace': 'test_keyspace',
        'port': 9042,
        'models_path': 'test_models'
    }

@pytest.fixture
def cli_runner():
    """Fixture para runner do CLI."""
    from typer.testing import CliRunner
    return CliRunner()

@pytest.fixture(autouse=True)
def clean_env():
    """Fixture para limpar variáveis de ambiente antes de cada teste."""
    env_vars_to_clear = [
        'CASPY_HOSTS',
        'CASPY_KEYSPACE', 
        'CASPY_PORT',
        'CASPY_MODELS_PATH'
    ]
    
    original_env = {}
    for var in env_vars_to_clear:
        if var in os.environ:
            original_env[var] = os.environ[var]
            del os.environ[var]
    
    yield
    
    # Restaurar variáveis originais
    for var, value in original_env.items():
        os.environ[var] = value 