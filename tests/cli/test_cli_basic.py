"""
Testes básicos para o CLI do CaspyORM.
"""

import os
import sys
from unittest.mock import patch, MagicMock

# Adicionar o diretório raiz ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from cli.main import get_config, parse_filters

def test_get_config_defaults():
    """Testa se get_config() retorna valores padrão corretos."""
    with patch.dict(os.environ, {}, clear=True):
        config = get_config()
        
        assert config['hosts'] == ['127.0.0.1']
        assert config['keyspace'] == 'caspyorm_app'
        assert config['port'] == 9042
        assert config['model_paths'] == []

def test_get_config_with_env_vars():
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

def test_parse_filters_empty():
    """Testa parse_filters com lista vazia."""
    result = parse_filters([])
    assert result == {}

def test_parse_filters_string_values():
    """Testa parse_filters com valores string."""
    filters = ['name=João', 'email=joao@email.com']
    result = parse_filters(filters)
    
    assert result['name'] == 'João'
    assert result['email'] == 'joao@email.com'

def test_parse_filters_boolean_values():
    """Testa parse_filters com valores booleanos."""
    filters = ['ativo=true', 'deletado=false']
    result = parse_filters(filters)
    
    assert result['ativo'] is True
    assert result['deletado'] is False

def test_parse_filters_numeric_values():
    """Testa parse_filters com valores numéricos."""
    filters = ['idade=25', 'altura=1.75']
    result = parse_filters(filters)
    
    assert result['idade'] == 25
    assert result['altura'] == 1.75

def test_parse_filters_invalid_format():
    """Testa parse_filters com formato inválido."""
    filters = ['invalid_filter', 'name=value', 'another_invalid']
    result = parse_filters(filters)
    
    assert result['name'] == 'value'
    assert len(result) == 1  # Apenas o válido deve ser processado

def test_parse_filters_uuid_values():
    """Testa parse_filters com valores UUID."""
    import uuid
    test_uuid = str(uuid.uuid4())
    filters = [f'user_id={test_uuid}']
    result = parse_filters(filters)
    
    assert isinstance(result['user_id'], uuid.UUID)
    assert str(result['user_id']) == test_uuid

if __name__ == '__main__':
    # Executar testes básicos
    test_get_config_defaults()
    test_get_config_with_env_vars()
    test_parse_filters_empty()
    test_parse_filters_string_values()
    test_parse_filters_boolean_values()
    test_parse_filters_numeric_values()
    test_parse_filters_invalid_format()
    test_parse_filters_uuid_values()
    print("✅ Todos os testes básicos passaram!") 