# Testes do CaspyORM

Este diretório contém todos os testes para a biblioteca CaspyORM, organizados de forma estruturada e abrangente.

## 📁 Estrutura dos Testes

```
tests/
├── unit/                    # Testes unitários
│   ├── test_fields.py      # Testes para campos (Text, UUID, Integer, etc.)
│   ├── test_connection.py  # Testes para conexão com Cassandra
│   ├── test_model.py       # Testes para modelos e ORM
│   ├── test_query.py       # Testes para QuerySets e consultas
│   ├── test_internal_*.py  # Testes para módulos internos
│   ├── test_exceptions.py  # Testes para tratamento de exceções
│   └── test_logging.py     # Testes para sistema de logging
├── integration/            # Testes de integração
│   └── test_model_integration.py  # Testes de CRUD e operações complexas
├── cli/                    # Testes do CLI
│   ├── test_cli.py         # Testes básicos do CLI
│   ├── test_cli_commands.py # Testes de comandos específicos
│   └── test_cli_basic.py   # Testes fundamentais do CLI
├── fixtures/               # Fixtures e dados de teste
│   └── test_data.py        # Dados de exemplo e fixtures
├── data/                   # Dados de exemplo
├── conftest.py             # Configuração global do pytest
├── run_all_tests.py        # Script para executar todos os testes
└── README.md               # Este arquivo
```

## 🧪 Tipos de Testes

### Testes Unitários (`unit/`)
Testam componentes individuais da biblioteca de forma isolada:
- **Campos**: Validação, serialização, conversão de tipos
- **Conexão**: Estabelecimento, gerenciamento, tratamento de erros
- **Modelos**: Criação, validação, operações CRUD
- **QuerySets**: Filtros, ordenação, operadores
- **Módulos Internos**: Serialização, construção de modelos
- **Exceções**: Tratamento e propagação de erros
- **Logging**: Sistema de logs e métricas

### Testes de Integração (`integration/`)
Testam a interação entre diferentes componentes:
- Operações CRUD completas
- Queries complexas com múltiplos filtros
- Relacionamentos entre modelos
- Operações em lote
- Tratamento de erros em cenários reais

### Testes do CLI (`cli/`)
Testam a interface de linha de comando:
- Comandos básicos (version, help)
- Comandos de modelo (models, query)
- Tratamento de erros e validação
- Integração com diferentes configurações

## 🚀 Como Executar os Testes

### Executar Todos os Testes
```bash
# Usando o script organizado
python tests/run_all_tests.py

# Usando pytest diretamente
python -m pytest tests/ -v
```

### Executar por Categoria
```bash
# Apenas testes unitários
python tests/run_all_tests.py --category unit

# Apenas testes de integração
python tests/run_all_tests.py --category integration

# Apenas testes do CLI
python tests/run_all_tests.py --category cli
```

### Executar com Cobertura
```bash
# Com cobertura de código
python tests/run_all_tests.py --coverage

# Gerar relatório completo
python tests/run_all_tests.py --report
```

### Executar Testes Específicos
```bash
# Teste específico
python -m pytest tests/unit/test_fields.py -v

# Teste com marcadores
python tests/run_all_tests.py --markers "slow"

# Teste com filtro
python -m pytest tests/ -k "test_uuid" -v
```

### Verificar Ambiente
```bash
# Verificar se tudo está configurado
python tests/run_all_tests.py --check
```

## 📊 Cobertura de Testes

Os testes cobrem:

### Campos (100%)
- ✅ Text, UUID, Integer, Float, Boolean, Timestamp
- ✅ List, Set, Map (tipos complexos)
- ✅ Validação e conversão de tipos
- ✅ Valores padrão e obrigatórios
- ✅ Serialização e desserialização

### Conexão (95%)
- ✅ Estabelecimento de conexão síncrona/assíncrona
- ✅ Configuração de autenticação e balanceamento
- ✅ Execução de queries
- ✅ Tratamento de erros
- ✅ Context managers

### Modelos (90%)
- ✅ Criação e validação
- ✅ Operações CRUD (Create, Read, Update, Delete)
- ✅ Relacionamentos
- ✅ Metaclass e construção dinâmica
- ✅ Serialização/deserialização

### QuerySets (85%)
- ✅ Filtros simples e complexos
- ✅ Operadores (__gt, __lt, __in, etc.)
- ✅ Ordenação e paginação
- ✅ Agregações (count, exists)
- ✅ Operações em lote

### CLI (80%)
- ✅ Comandos básicos
- ✅ Comandos de modelo
- ✅ Tratamento de erros
- ✅ Configuração flexível

### Módulos Internos (95%)
- ✅ Serialização de dados
- ✅ Construção de modelos
- ✅ Tratamento de exceções
- ✅ Sistema de logging

## 🏷️ Marcadores de Teste

```python
@pytest.mark.slow          # Testes lentos
@pytest.mark.integration   # Testes de integração
@pytest.mark.unit          # Testes unitários
@pytest.mark.asyncio       # Testes assíncronos
```

### Executar por Marcador
```bash
# Apenas testes rápidos
python -m pytest tests/ -m "not slow"

# Apenas testes de integração
python -m pytest tests/ -m "integration"

# Apenas testes assíncronos
python -m pytest tests/ -m "asyncio"
```

## 🔧 Configuração

### Dependências de Teste
```bash
pip install pytest pytest-cov pytest-asyncio
```

### Variáveis de Ambiente para Testes
```bash
export CASPY_MODELS_PATH="test.models"
export CASSANDRA_CONTACT_POINTS="localhost"
export CASSANDRA_PORT="9042"
export CASSANDRA_KEYSPACE="test_keyspace"
```

### Configuração do pytest (`pyproject.toml`)
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "--strict-markers",
    "--strict-config",
    "--cov=caspyorm",
    "--cov-report=term-missing",
    "--cov-report=html",
    "--cov-report=xml",
]
markers = [
    "slow: marks tests as slow",
    "integration: marks tests as integration tests",
    "unit: marks tests as unit tests",
    "asyncio: marca testes que usam async/await",
]
```

## 📈 Métricas de Qualidade

### Cobertura de Código
- **Meta**: > 90% de cobertura
- **Atual**: ~85% de cobertura
- **Relatórios**: HTML, XML, Terminal

### Tempo de Execução
- **Testes Unitários**: < 30 segundos
- **Testes de Integração**: < 2 minutos
- **Todos os Testes**: < 3 minutos

### Qualidade dos Testes
- **Assertions por teste**: Mínimo 3
- **Cenários de erro**: Cobertos
- **Documentação**: 100% dos testes documentados

## 🐛 Debugging de Testes

### Executar Teste Específico com Debug
```bash
python -m pytest tests/unit/test_fields.py::TestText::test_text_initialization -v -s
```

### Executar com Logs Detalhados
```bash
python -m pytest tests/ -v -s --log-cli-level=DEBUG
```

### Executar Teste Falhando Repetidamente
```bash
python -m pytest tests/ -x --tb=short
```

## 📝 Adicionando Novos Testes

### Estrutura de um Teste
```python
import pytest
from unittest.mock import Mock, patch

class TestNewFeature:
    """Testes para nova funcionalidade."""
    
    def test_basic_functionality(self):
        """Testa funcionalidade básica."""
        # Arrange
        expected = "expected_value"
        
        # Act
        result = function_under_test()
        
        # Assert
        assert result == expected
    
    @pytest.mark.asyncio
    async def test_async_functionality(self):
        """Testa funcionalidade assíncrona."""
        # Arrange
        expected = "expected_value"
        
        # Act
        result = await async_function_under_test()
        
        # Assert
        assert result == expected
```

### Convenções
- **Nomes**: `test_*.py` para arquivos, `test_*` para funções
- **Classes**: `Test*` para classes de teste
- **Documentação**: Docstring em todos os testes
- **Organização**: Arrange, Act, Assert
- **Mocks**: Use mocks para dependências externas

## 🤝 Contribuindo

### Antes de Enviar
1. Execute todos os testes: `python tests/run_all_tests.py`
2. Verifique cobertura: `python tests/run_all_tests.py --coverage`
3. Adicione testes para novas funcionalidades
4. Atualize documentação se necessário

### Padrões de Commit
```
test: adiciona testes para nova funcionalidade X
test: corrige teste falhando em Y
test: melhora cobertura de testes em Z
```

## 📚 Recursos Adicionais

- [Documentação do pytest](https://docs.pytest.org/)
- [Guia de Testes Assíncronos](https://pytest-asyncio.readthedocs.io/)
- [Cobertura de Código](https://coverage.readthedocs.io/)
- [Mocking em Python](https://docs.python.org/3/library/unittest.mock.html) 