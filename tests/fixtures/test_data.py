"""
Dados de teste e fixtures para CaspyORM.
"""

import pytest
import uuid
from datetime import datetime
from typing import Dict, List, Any

from caspyorm.fields import Text, Integer, UUID, Boolean, Timestamp, List as ListField, Set, Map


# Fixtures para campos
@pytest.fixture
def sample_text_field():
    """Retorna um campo Text de exemplo."""
    return Text(required=True)


@pytest.fixture
def sample_integer_field():
    """Retorna um campo Integer de exemplo."""
    return Integer(default=0)


@pytest.fixture
def sample_uuid_field():
    """Retorna um campo UUID de exemplo."""
    return UUID(primary_key=True)


@pytest.fixture
def sample_boolean_field():
    """Retorna um campo Boolean de exemplo."""
    return Boolean(default=True)


@pytest.fixture
def sample_timestamp_field():
    """Retorna um campo Timestamp de exemplo."""
    return Timestamp(default=lambda: datetime.now())


@pytest.fixture
def sample_list_field():
    """Retorna um campo List de exemplo."""
    return ListField(Text())


@pytest.fixture
def sample_set_field():
    """Retorna um campo Set de exemplo."""
    return Set(Text())


@pytest.fixture
def sample_map_field():
    """Retorna um campo Map de exemplo."""
    return Map(Text(), Integer())


# Dados de exemplo para testes
SAMPLE_USER_DATA = {
    'id': str(uuid.uuid4()),
    'name': 'João Silva',
    'email': 'joao.silva@example.com',
    'age': 30,
    'active': True,
    'created_at': datetime(2023, 1, 1, 12, 0, 0).isoformat()
}

SAMPLE_POST_DATA = {
    'id': str(uuid.uuid4()),
    'title': 'Meu Primeiro Post',
    'content': 'Este é o conteúdo do meu primeiro post.',
    'user_id': str(uuid.uuid4()),
    'published': False,
    'created_at': datetime(2023, 1, 1, 12, 0, 0).isoformat()
}

SAMPLE_COMMENT_DATA = {
    'id': str(uuid.uuid4()),
    'content': 'Excelente post!',
    'user_id': str(uuid.uuid4()),
    'post_id': str(uuid.uuid4()),
    'created_at': datetime(2023, 1, 1, 12, 0, 0).isoformat()
}

# Lista de usuários de exemplo
SAMPLE_USERS = [
    {
        'id': str(uuid.uuid4()),
        'name': 'João Silva',
        'email': 'joao.silva@example.com',
        'age': 30,
        'active': True,
        'created_at': datetime(2023, 1, 1, 12, 0, 0).isoformat()
    },
    {
        'id': str(uuid.uuid4()),
        'name': 'Maria Santos',
        'email': 'maria.santos@example.com',
        'age': 25,
        'active': True,
        'created_at': datetime(2023, 1, 2, 14, 30, 0).isoformat()
    },
    {
        'id': str(uuid.uuid4()),
        'name': 'Pedro Costa',
        'email': 'pedro.costa@example.com',
        'age': 35,
        'active': False,
        'created_at': datetime(2023, 1, 3, 9, 15, 0).isoformat()
    }
]

# Lista de posts de exemplo
SAMPLE_POSTS = [
    {
        'id': str(uuid.uuid4()),
        'title': 'Introdução ao CaspyORM',
        'content': 'CaspyORM é um ORM moderno para Apache Cassandra...',
        'user_id': SAMPLE_USERS[0]['id'],
        'published': True,
        'created_at': datetime(2023, 1, 1, 12, 0, 0).isoformat()
    },
    {
        'id': str(uuid.uuid4()),
        'title': 'Como usar QuerySets',
        'content': 'QuerySets permitem consultas complexas...',
        'user_id': SAMPLE_USERS[1]['id'],
        'published': True,
        'created_at': datetime(2023, 1, 2, 14, 30, 0).isoformat()
    },
    {
        'id': str(uuid.uuid4()),
        'title': 'Rascunho de Post',
        'content': 'Este é um rascunho...',
        'user_id': SAMPLE_USERS[2]['id'],
        'published': False,
        'created_at': datetime(2023, 1, 3, 9, 15, 0).isoformat()
    }
]

# Dados complexos para testes
SAMPLE_COMPLEX_DATA = {
    'id': str(uuid.uuid4()),
    'name': 'Produto Complexo',
    'tags': ['tecnologia', 'python', 'cassandra'],
    'categories': ['software', 'database'],
    'metadata': {
        'version': 1,
        'author': 'João Silva',
        'rating': 5
    },
    'created_at': datetime(2023, 1, 1, 12, 0, 0).isoformat()
}

# Dados para testes de validação
INVALID_USER_DATA = [
    {
        'name': 123,  # Deve ser string
        'email': 'joao@example.com',
        'age': 30
    },
    {
        'name': 'João',
        'email': 456,  # Deve ser string
        'age': 30
    },
    {
        'name': 'João',
        'email': 'joao@example.com',
        'age': 'invalid'  # Deve ser int
    },
    {
        'name': 'João',
        'email': 'joao@example.com',
        'age': -5  # Deve ser positivo
    }
]

# Dados para testes de operadores de query
QUERY_TEST_DATA = {
    'users': [
        {'name': 'João', 'age': 25, 'active': True},
        {'name': 'Maria', 'age': 30, 'active': True},
        {'name': 'Pedro', 'age': 35, 'active': False},
        {'name': 'Ana', 'age': 28, 'active': True},
        {'name': 'Carlos', 'age': 40, 'active': False}
    ],
    'posts': [
        {'title': 'Post 1', 'published': True, 'views': 100},
        {'title': 'Post 2', 'published': False, 'views': 50},
        {'title': 'Post 3', 'published': True, 'views': 200},
        {'title': 'Post 4', 'published': True, 'views': 75}
    ]
}

# Fixtures para modelos de teste
@pytest.fixture
def sample_user_model():
    """Retorna dados de um usuário de exemplo."""
    return SAMPLE_USER_DATA.copy()


@pytest.fixture
def sample_post_model():
    """Retorna dados de um post de exemplo."""
    return SAMPLE_POST_DATA.copy()


@pytest.fixture
def sample_users_list():
    """Retorna lista de usuários de exemplo."""
    return SAMPLE_USERS.copy()


@pytest.fixture
def sample_posts_list():
    """Retorna lista de posts de exemplo."""
    return SAMPLE_POSTS.copy()


@pytest.fixture
def sample_complex_model():
    """Retorna dados complexos de exemplo."""
    return SAMPLE_COMPLEX_DATA.copy()


@pytest.fixture
def invalid_user_data_list():
    """Retorna lista de dados inválidos de usuário."""
    return INVALID_USER_DATA.copy()


@pytest.fixture
def query_test_data():
    """Retorna dados para testes de query."""
    return QUERY_TEST_DATA.copy()


# Fixtures para configuração de teste
@pytest.fixture
def mock_connection():
    """Retorna um mock da conexão."""
    with patch('caspyorm.connection.Connection') as mock_conn:
        yield mock_conn


@pytest.fixture
def mock_session():
    """Retorna um mock da sessão."""
    mock_sess = AsyncMock()
    mock_sess.execute_async = AsyncMock()
    return mock_sess


@pytest.fixture
def mock_cluster():
    """Retorna um mock do cluster."""
    mock_clust = Mock()
    mock_clust.connect_async = AsyncMock()
    mock_clust.shutdown = Mock()
    return mock_clust


# Fixtures para dados de teste específicos
@pytest.fixture
def sample_uuid():
    """Retorna um UUID de exemplo."""
    return uuid.uuid4()


@pytest.fixture
def sample_datetime():
    """Retorna uma data/hora de exemplo."""
    return datetime(2023, 1, 1, 12, 0, 0)


@pytest.fixture
def sample_string():
    """Retorna uma string de exemplo."""
    return "exemplo de string"


@pytest.fixture
def sample_integer():
    """Retorna um inteiro de exemplo."""
    return 42


@pytest.fixture
def sample_boolean():
    """Retorna um booleano de exemplo."""
    return True


@pytest.fixture
def sample_list():
    """Retorna uma lista de exemplo."""
    return ["item1", "item2", "item3"]


@pytest.fixture
def sample_set():
    """Retorna um set de exemplo."""
    return {"item1", "item2", "item3"}


@pytest.fixture
def sample_dict():
    """Retorna um dicionário de exemplo."""
    return {"key1": "value1", "key2": "value2"}


# Fixtures para cenários de teste
@pytest.fixture
def empty_database():
    """Simula um banco de dados vazio."""
    return {
        'users': [],
        'posts': [],
        'comments': []
    }


@pytest.fixture
def populated_database():
    """Simula um banco de dados populado."""
    return {
        'users': SAMPLE_USERS,
        'posts': SAMPLE_POSTS,
        'comments': [SAMPLE_COMMENT_DATA]
    }


@pytest.fixture
def database_with_errors():
    """Simula um banco de dados com dados inconsistentes."""
    return {
        'users': [
            SAMPLE_USERS[0],
            {'id': 'invalid-uuid', 'name': 'Invalid User'},  # UUID inválido
            SAMPLE_USERS[1]
        ],
        'posts': SAMPLE_POSTS
    }


# Fixtures para configuração de ambiente
@pytest.fixture
def test_environment():
    """Configura ambiente de teste."""
    import os
    original_env = os.environ.copy()
    
    # Configurações de teste
    os.environ['CASPY_MODELS_PATH'] = 'test.models'
    os.environ['CASSANDRA_CONTACT_POINTS'] = 'localhost'
    os.environ['CASSANDRA_PORT'] = '9042'
    os.environ['CASSANDRA_KEYSPACE'] = 'test_keyspace'
    
    yield os.environ
    
    # Restaura ambiente original
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def test_logging_config():
    """Configura logging para testes."""
    import logging
    
    # Configura logging para testes
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.NullHandler()]
    )
    
    yield logging
    
    # Limpa handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


# Funções utilitárias para testes
def create_sample_user(**kwargs):
    """Cria um usuário de exemplo com dados customizáveis."""
    base_data = SAMPLE_USER_DATA.copy()
    base_data.update(kwargs)
    return base_data


def create_sample_post(**kwargs):
    """Cria um post de exemplo com dados customizáveis."""
    base_data = SAMPLE_POST_DATA.copy()
    base_data.update(kwargs)
    return base_data


def create_sample_comment(**kwargs):
    """Cria um comentário de exemplo com dados customizáveis."""
    base_data = SAMPLE_COMMENT_DATA.copy()
    base_data.update(kwargs)
    return base_data


def generate_test_data(count: int, template: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Gera dados de teste baseados em um template."""
    data = []
    for i in range(count):
        item = template.copy()
        item['id'] = str(uuid.uuid4())
        item['name'] = f"{item.get('name', 'Item')} {i+1}"
        data.append(item)
    return data


def assert_model_fields(model_data: Dict[str, Any], expected_fields: List[str]):
    """Verifica se o modelo tem os campos esperados."""
    for field in expected_fields:
        assert field in model_data, f"Campo '{field}' não encontrado no modelo"


def assert_data_types(model_data: Dict[str, Any], field_types: Dict[str, type]):
    """Verifica se os dados têm os tipos esperados."""
    for field, expected_type in field_types.items():
        if field in model_data:
            assert isinstance(model_data[field], expected_type), \
                f"Campo '{field}' deve ser do tipo {expected_type.__name__}"


def assert_required_fields(model_data: Dict[str, Any], required_fields: List[str]):
    """Verifica se os campos obrigatórios estão presentes."""
    for field in required_fields:
        assert field in model_data, f"Campo obrigatório '{field}' não encontrado"
        assert model_data[field] is not None, f"Campo obrigatório '{field}' não pode ser None" 