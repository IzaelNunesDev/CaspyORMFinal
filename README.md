# CaspyORM

Um ORM moderno e eficiente para Apache Cassandra, construído com Python e Pydantic.

## Instalação

### Instalação via PyPI (Recomendado)

```bash
pip install caspyorm
```

### Instalação com dependências opcionais

```bash
# Com suporte a FastAPI
pip install caspyorm[fastapi]

# Com suporte a operações assíncronas otimizadas
pip install caspyorm[async]

# Com todas as dependências opcionais
pip install caspyorm[fastapi,async]
```

### Instalação via Git (Desenvolvimento)

```bash
git clone https://github.com/caspyorm/caspyorm.git
cd caspyorm
pip install -e .
```

## Uso Rápido

```python
from caspyorm import Model, connect
from caspyorm.core.fields import Text, Integer

# Conectar ao Cassandra
connect(contact_points=['localhost'], keyspace='my_keyspace')

# Definir um modelo
class User(Model):
    __table_name__ = "users"
    id = Integer(primary_key=True)
    name = Text()
    email = Text()

# Criar um usuário
user = User(id=1, name="João Silva", email="joao@example.com")
user.save()

# Buscar um usuário
user = User.get(id=1)
print(user.name)  # João Silva

# Filtrar usuários
users = User.filter(name__contains="João").all()
```

## CLI

CaspyORM inclui uma CLI poderosa para interagir com seus modelos:

```bash
# Listar modelos disponíveis
caspy models

# Testar conexão
caspy connect

# Executar queries
caspy query User get --filter id=1
caspy query User filter --filter name__contains="João" --limit 10

# Executar SQL direto
caspy sql "SELECT * FROM users LIMIT 5"
```

## Características

- **ORM Moderno**: Baseado em Pydantic para validação de dados
- **Suporte a FastAPI**: Integração nativa com FastAPI
- **CLI Poderosa**: Interface de linha de comando para operações
- **Tipagem Completa**: Suporte completo a type hints
- **Operações Assíncronas**: Suporte a async/await
- **Migrações**: Sistema de migrações de schema
- **Batch Operations**: Operações em lote para melhor performance

## Documentação

Para mais informações, consulte a [documentação completa](https://caspyorm.readthedocs.io).

## Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

## Contribuindo

Contribuições são bem-vindas! Por favor, leia o [guia de contribuição](CONTRIBUTING.md) antes de submeter um pull request.