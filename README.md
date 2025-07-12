# CaspyORM - Documentação Completa

## Índice

1. [Visão Geral](#visão-geral)
2. [Instalação](#instalação)
3. [Configuração](#configuração)
4. [Conceitos Básicos](#conceitos-básicos)
5. [Modelos e Campos](#modelos-e-campos)
6. [Operações CRUD](#operações-crud)
7. [Queries e Filtros](#queries-e-filtros)
8. [Tipos de Dados](#tipos-de-dados)
9. [User-Defined Types (UDT)](#user-defined-types-udt)
10. [Operações em Lote](#operações-em-lote)
11. [Migrações](#migrações)
12. [CLI (Interface de Linha de Comando)](#cli-interface-de-linha-de-comando)
13. [Integração com FastAPI](#integração-com-fastapi)
14. [Performance e Otimização](#performance-e-otimização)
15. [Tratamento de Erros](#tratamento-de-erros)
16. [Exemplos Práticos](#exemplos-práticos)
17. [Referência da API](#referência-da-api)

---

## Visão Geral

CaspyORM é um ORM (Object-Relational Mapping) moderno e eficiente para Apache Cassandra, construído com Python e Pydantic. Ele oferece uma interface intuitiva e type-safe para trabalhar com o Cassandra, combinando a simplicidade do Pydantic com a potência do Cassandra.

### Características Principais

- **Type Safety**: Validação automática de tipos usando Pydantic
- **Interface Intuitiva**: API similar ao Django ORM e SQLAlchemy
- **Suporte Completo ao Cassandra**: Todos os tipos de dados nativos
- **User-Defined Types**: Suporte completo a UDTs
- **Operações em Lote**: Otimizações para grandes volumes de dados
- **Migrações**: Sistema de migrações automático
- **CLI**: Interface de linha de comando para operações comuns
- **Integração FastAPI**: Suporte nativo para FastAPI
- **Performance**: Otimizado para alta performance

---

## Instalação

### Instalação Básica

```bash
pip install caspyorm
```

### Instalação com Dependências Opcionais

```bash
# Com suporte a FastAPI
pip install caspyorm[fastapi]

# Com suporte a operações assíncronas otimizadas
pip install caspyorm[async]

# Com todas as dependências opcionais
pip install caspyorm[fastapi,async]
```

### Instalação para Desenvolvimento

```bash
git clone https://github.com/caspyorm/caspyorm.git
cd caspyorm
pip install -e .
```

---

## Configuração

### Configuração Básica

```python
from caspyorm import connect

# Conectar ao Cassandra
connect(
    contact_points=['localhost'],
    keyspace='my_keyspace',
    port=9042
)
```

### Configuração Avançada

```python
from caspyorm import connect

connect(
    contact_points=['cassandra1.example.com', 'cassandra2.example.com'],
    keyspace='production_keyspace',
    port=9042,
    username='myuser',
    password='mypassword',
    ssl_options={
        'ca_certs': '/path/to/ca.crt',
        'check_hostname': True
    },
    protocol_version=4,
    connect_timeout=10,
    request_timeout=30
)
```

### Configuração via Arquivo

Crie um arquivo `caspy.toml` na raiz do projeto:

```toml
[cassandra]
hosts = ["localhost"]
keyspace = "my_keyspace"
port = 9042
username = "myuser"
password = "mypassword"
ssl = false
protocol_version = 4
connect_timeout = 10
request_timeout = 30
```

### Configuração via Variáveis de Ambiente

```bash
export CASPY_HOSTS="localhost"
export CASPY_KEYSPACE="my_keyspace"
export CASPY_PORT="9042"
export CASPY_USERNAME="myuser"
export CASPY_PASSWORD="mypassword"
```

---

## Conceitos Básicos

### Estrutura de um Modelo

```python
from caspyorm import Model
from caspyorm.core.fields import Text, Integer, Boolean, Timestamp

class User(Model):
    __table_name__ = "users"
    
    id = Integer(primary_key=True)
    username = Text(required=True)
    email = Text(required=True)
    is_active = Boolean(default=True)
    created_at = Timestamp()
```

### Principais Componentes

1. **Model**: Classe base para todos os modelos
2. **Fields**: Definição dos tipos de dados
3. **Connection**: Gerenciamento de conexões
4. **Query**: Construção de consultas
5. **Batch**: Operações em lote

---

## Modelos e Campos

### Definição de Modelos

```python
from caspyorm import Model
from caspyorm.core.fields import (
    Text, Integer, Boolean, Timestamp, 
    UUID, Float, List, Set, Map, Tuple
)

class Product(Model):
    __table_name__ = "products"
    
    # Chaves primárias
    id = UUID(primary_key=True)  # Gera UUID automaticamente
    category_id = Integer(partition_key=True)
    
    # Campos básicos
    name = Text(required=True)
    description = Text()
    price = Float(required=True)
    is_active = Boolean(default=True)
    
    # Campos de data
    created_at = Timestamp()
    updated_at = Timestamp()
    
    # Campos de coleção
    tags = Set(Text(), default=set)
    attributes = Map(Text(), Text(), default=dict)
    dimensions = Tuple(Integer(), Integer(), Integer())
    images = List(Text(), default=list)
```

### Tipos de Chaves

```python
class Order(Model):
    __table_name__ = "orders"
    
    # Chave de partição (obrigatória)
    user_id = Integer(partition_key=True)
    
    # Chaves de clustering (opcionais)
    order_date = Timestamp(clustering_key=True)
    order_id = UUID(clustering_key=True)
    
    # Campos normais
    status = Text(required=True)
    total = Float(required=True)
```

### Campos com Índices

```python
class User(Model):
    __table_name__ = "users"
    
    id = Integer(primary_key=True)
    email = Text(required=True, index=True)  # Índice secundário
    username = Text(required=True, index=True)
```

---

## Operações CRUD

### Create (Criar)

```python
# Criar um objeto
user = User(
    username="john_doe",
    email="john@example.com",
    is_active=True
)

# Salvar no banco
user.save()

# Criar com dados específicos
user = User.create(
    username="jane_doe",
    email="jane@example.com"
)
```

### Read (Ler)

```python
# Buscar por chave primária
user = User.get(id=1)

# Buscar todos os registros
all_users = User.all().all()

# Buscar com limite
users = User.all().limit(10).all()

# Buscar com filtros
active_users = User.all().filter(is_active=True).all()
```

### Update (Atualizar)

```python
# Atualizar um objeto
user = User.get(id=1)
user.email = "new_email@example.com"
user.save()

# Atualizar múltiplos campos
user.update(
    email="new_email@example.com",
    is_active=False
)
```

### Delete (Deletar)

```python
# Deletar um objeto
user = User.get(id=1)
user.delete()

# Deletar por chave primária
User.delete(id=1)
```

---

## Queries e Filtros

### Queries Básicas

```python
# Buscar todos
users = User.all().all()

# Buscar com limite
users = User.all().limit(10).all()

# Buscar com ordenação
users = User.all().order_by('created_at', 'DESC').all()

# Buscar com filtros
users = User.all().filter(
    is_active=True,
    created_at__gte=datetime(2023, 1, 1)
).all()
```

### Filtros Avançados

```python
# Filtros de comparação
users = User.all().filter(
    age__gte=18,
    age__lte=65,
    email__contains="@gmail.com"
).all()

# Filtros em coleções
products = Product.all().filter(
    tags__contains="electronics",
    attributes__contains_key="color"
).all()

# Filtros com operadores lógicos
users = User.all().filter(
    (User.is_active == True) & (User.age >= 18)
).all()
```

### Queries Personalizadas

```python
from caspyorm.core.connection import execute

# Query SQL direta
result = execute("SELECT COUNT(*) FROM users WHERE is_active = true")
count = result[0].count

# Query com parâmetros
result = execute(
    "SELECT * FROM users WHERE age >= %s AND city = %s",
    [18, "New York"]
)
```

---

## Tipos de Dados

### Tipos Básicos

```python
from caspyorm.core.fields import (
    Text, Integer, Float, Boolean, Timestamp, UUID
)

class BasicTypes(Model):
    __table_name__ = "basic_types"
    
    id = Integer(primary_key=True)
    name = Text(required=True)
    age = Integer()
    height = Float()
    is_active = Boolean(default=True)
    created_at = Timestamp()
    user_id = UUID()
```

### Tipos de Coleção

```python
from caspyorm.core.fields import List, Set, Map, Tuple

class CollectionTypes(Model):
    __table_name__ = "collection_types"
    
    id = Integer(primary_key=True)
    
    # Lista
    tags = List(Text(), default=list)
    
    # Conjunto
    categories = Set(Text(), default=set)
    
    # Mapa
    metadata = Map(Text(), Text(), default=dict)
    
    # Tupla
    coordinates = Tuple(Integer(), Integer())
```

### Uso de Coleções

```python
# Criar com coleções
product = Product(
    id=1,
    name="Laptop",
    tags=["electronics", "computer"],
    categories={"tech", "hardware"},
    metadata={"brand": "Dell", "model": "XPS"},
    dimensions=(15, 10, 1)
)

# Modificar coleções
product.tags.append("gaming")
product.categories.add("gaming")
product.metadata["price"] = "999.99"

product.save()
```

---

## User-Defined Types (UDT)

### Definindo UDTs

```python
from caspyorm.types.usertype import UserType
from caspyorm.core.fields import Text, Integer

class Address(UserType):
    street = Text()
    city = Text()
    state = Text()
    zip_code = Text()
    country = Text()

class Contact(UserType):
    phone = Text()
    email = Text()
    website = Text()
```

### Usando UDTs em Modelos

```python
from caspyorm.core.fields import UserDefinedType

class Customer(Model):
    __table_name__ = "customers"
    
    id = Integer(primary_key=True)
    name = Text(required=True)
    address = UserDefinedType(Address)
    contact = UserDefinedType(Contact)
```

### Operações com UDTs

```python
# Criar UDT
address = Address(
    street="123 Main St",
    city="New York",
    state="NY",
    zip_code="10001",
    country="USA"
)

contact = Contact(
    phone="+1-555-1234",
    email="john@example.com",
    website="https://example.com"
)

# Usar em modelo
customer = Customer(
    id=1,
    name="John Doe",
    address=address,
    contact=contact
)

customer.save()

# Acessar campos do UDT
print(customer.address.street)  # "123 Main St"
print(customer.contact.email)   # "john@example.com"

# Modificar UDT
customer.address.city = "Los Angeles"
customer.save()
```

---

## Operações em Lote

### Inserção em Lote

```python
from caspyorm.types.batch import BatchQuery

# Inserir múltiplos registros
users = [
    User(username="user1", email="user1@example.com"),
    User(username="user2", email="user2@example.com"),
    User(username="user3", email="user3@example.com")
]

with BatchQuery():
    for user in users:
        user.save()
```

### Atualização em Lote

```python
# Atualizar múltiplos registros
users = User.all().filter(is_active=False).all()

with BatchQuery():
    for user in users:
        user.is_active = True
        user.save()
```

### Deleção em Lote

```python
# Deletar múltiplos registros
users = User.all().filter(created_at__lt=datetime(2020, 1, 1)).all()

with BatchQuery():
    for user in users:
        user.delete()
```

### Performance de Lote

```python
import time

# Teste de performance
N = 1000
batch_size = 100

t0 = time.time()
for start in range(0, N, batch_size):
    end = min(start + batch_size, N)
    with BatchQuery():
        for i in range(start, end):
            User(username=f"user{i}", email=f"user{i}@example.com").save()
t1 = time.time()

print(f"Tempo para inserir {N} registros: {t1-t0:.2f}s")
```

---

## Migrações

### Inicialização

```bash
# Inicializar sistema de migrações
caspyorm migrate init --keyspace my_keyspace
```

### Criar Nova Migração

```bash
# Criar nova migração
caspyorm migrate new create_users_table
```

### Estrutura de Migração

```python
# migrations/V001_create_users_table.py
from caspyorm.core.connection import get_session
from caspyorm.utils.schema import create_table
from models import User

def upgrade():
    """Executa a migração para cima."""
    session = get_session()
    create_table(session, User)

def downgrade():
    """Reverte a migração."""
    session = get_session()
    session.execute("DROP TABLE IF EXISTS users")
```

### Aplicar Migrações

```bash
# Aplicar todas as migrações pendentes
caspyorm migrate apply --keyspace my_keyspace

# Verificar status das migrações
caspyorm migrate status --keyspace my_keyspace

# Reverter última migração
caspyorm migrate downgrade --keyspace my_keyspace --force
```

### Migrações Complexas

```python
def upgrade():
    session = get_session()
    
    # Criar nova tabela
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id int PRIMARY KEY,
            bio text,
            avatar_url text,
            preferences map<text, text>
        )
    """)
    
    # Adicionar coluna a tabela existente
    session.execute("""
        ALTER TABLE users ADD phone text
    """)
    
    # Criar índice
    session.execute("""
        CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)
    """)

def downgrade():
    session = get_session()
    
    # Remover índice
    session.execute("DROP INDEX IF EXISTS idx_users_email")
    
    # Remover coluna
    session.execute("ALTER TABLE users DROP phone")
    
    # Remover tabela
    session.execute("DROP TABLE IF EXISTS user_profiles")
```

---

## CLI (Interface de Linha de Comando)

### Comandos Básicos

```bash
# Informações da conexão
caspyorm info

# Listar modelos disponíveis
caspyorm models

# Conectar ao Cassandra
caspyorm connect --keyspace my_keyspace

# Executar query SQL
caspyorm sql "SELECT COUNT(*) FROM users"

# Query em modelo
caspyorm query users count
caspyorm query users filter --filter "is_active=true" --limit 10
```

### Comandos de Migração

```bash
# Inicializar migrações
caspyorm migrate init --keyspace my_keyspace

# Criar nova migração
caspyorm migrate new create_table_name

# Aplicar migrações
caspyorm migrate apply --keyspace my_keyspace

# Status das migrações
caspyorm migrate status --keyspace my_keyspace

# Reverter migração
caspyorm migrate downgrade --keyspace my_keyspace --force
```

### Configuração da CLI

```bash
# Via variáveis de ambiente
export CASPY_HOSTS="localhost"
export CASPY_KEYSPACE="my_keyspace"
export CASPY_PORT="9042"

# Via arquivo caspy.toml
caspyorm --config caspy.toml info
```

---

## Integração com FastAPI

### Configuração Básica

```python
from fastapi import FastAPI
from caspyorm.contrib.fastapi import CaspyORM

app = FastAPI()

# Configurar CaspyORM
caspyorm = CaspyORM(
    contact_points=['localhost'],
    keyspace='my_keyspace',
    port=9042
)

# Incluir no app
app.include_router(caspyorm.router)
```

### Modelos FastAPI

```python
from pydantic import BaseModel
from typing import Optional, List

class UserCreate(BaseModel):
    username: str
    email: str
    is_active: bool = True

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    is_active: bool
    created_at: Optional[datetime]

# Endpoints automáticos
@caspyorm.crud(User, UserCreate, UserResponse)
class UserCRUD:
    pass
```

### Endpoints Customizados

```python
from fastapi import APIRouter, HTTPException

router = APIRouter()

@router.get("/users/{user_id}")
async def get_user(user_id: int):
    user = User.get(id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user

@router.post("/users")
async def create_user(user_data: UserCreate):
    user = User(**user_data.dict())
    user.save()
    return user

@router.put("/users/{user_id}")
async def update_user(user_id: int, user_data: UserCreate):
    user = User.get(id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user.update(**user_data.dict())
    return user

@router.delete("/users/{user_id}")
async def delete_user(user_id: int):
    user = User.get(id=user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user.delete()
    return {"message": "User deleted"}
```

---

## Performance e Otimização

### Estratégias de Otimização

1. **Uso de Índices**
```python
class User(Model):
    __table_name__ = "users"
    
    id = Integer(primary_key=True)
    email = Text(required=True, index=True)  # Índice para busca por email
    username = Text(required=True, index=True)
```

2. **Operações em Lote**
```python
# Inserir em lotes para melhor performance
with BatchQuery():
    for user in users:
        user.save()
```

3. **Limitação de Resultados**
```python
# Usar LIMIT para evitar carregar muitos dados
users = User.all().limit(100).all()
```

4. **Filtros Eficientes**
```python
# Usar chaves de partição e clustering
orders = Order.all().filter(
    user_id=123,
    order_date__gte=datetime(2023, 1, 1)
).all()
```

### Monitoramento de Performance

```python
import time
from caspyorm.core.connection import get_session

# Medir tempo de query
t0 = time.time()
users = User.all().all()
t1 = time.time()
print(f"Query executada em {t1-t0:.2f}s")

# Verificar estatísticas da sessão
session = get_session()
print(f"Queries executadas: {session.query_count}")
print(f"Tempo total: {session.total_time:.2f}s")
```

---

## Tratamento de Erros

### Exceções Comuns

```python
from caspyorm.utils.exceptions import (
    ValidationError, ConnectionError, QueryError
)

try:
    user = User(username="john", email="invalid-email")
    user.save()
except ValidationError as e:
    print(f"Erro de validação: {e}")
except ConnectionError as e:
    print(f"Erro de conexão: {e}")
except QueryError as e:
    print(f"Erro de query: {e}")
```

### Validação de Dados

```python
from caspyorm.utils.exceptions import ValidationError

# Validação automática
try:
    user = User(
        id="not_an_integer",  # Erro: deve ser int
        email="invalid-email"  # Erro: formato inválido
    )
except ValidationError as e:
    print(f"Erros de validação: {e.errors}")
```

### Tratamento de Conexão

```python
from caspyorm import connect, disconnect
from caspyorm.utils.exceptions import ConnectionError

try:
    connect(contact_points=['localhost'], keyspace='my_keyspace')
    # Operações com o banco
except ConnectionError as e:
    print(f"Falha na conexão: {e}")
finally:
    disconnect()
```

---

## Exemplos Práticos

### Sistema de Blog

```python
from caspyorm import Model
from caspyorm.core.fields import (
    Integer, Text, Timestamp, Set, Map, UserDefinedType, Boolean
)
from caspyorm.types.usertype import UserType

class Author(UserType):
    name = Text()
    email = Text()
    bio = Text()

class Post(Model):
    __table_name__ = "posts"
    
    id = Integer(primary_key=True)
    title = Text(required=True)
    content = Text(required=True)
    author = UserDefinedType(Author)
    tags = Set(Text(), default=set)
    metadata = Map(Text(), Text(), default=dict)
    created_at = Timestamp()
    updated_at = Timestamp()
    is_published = Boolean(default=False)

# Criar post
author = Author(name="John Doe", email="john@example.com", bio="Tech writer")
post = Post(
    title="Getting Started with CaspyORM",
    content="CaspyORM is a modern ORM for Cassandra...",
    author=author,
    tags={"cassandra", "python", "orm"},
    metadata={"category": "tutorial", "difficulty": "beginner"}
)
post.save()

# Buscar posts
published_posts = Post.all().filter(is_published=True).all()
tech_posts = Post.all().filter(tags__contains="python").all()
```

### Sistema de E-commerce

```python
class Product(Model):
    __table_name__ = "products"
    
    id = Integer(primary_key=True)
    name = Text(required=True)
    description = Text()
    price = Float(required=True)
    category_id = Integer(partition_key=True)
    tags = Set(Text(), default=set)
    attributes = Map(Text(), Text(), default=dict)
    stock = Integer(default=0)
    is_active = Boolean(default=True)
    created_at = Timestamp()

class Order(Model):
    __table_name__ = "orders"
    
    user_id = Integer(partition_key=True)
    order_date = Timestamp(clustering_key=True)
    order_id = UUID(clustering_key=True)
    status = Text(required=True)
    total = Float(required=True)
    items = List(Text(), default=list)  # Lista de IDs de produtos
    shipping_address = UserDefinedType(Address)
    created_at = Timestamp()

# Buscar produtos por categoria
electronics = Product.all().filter(category_id=1, is_active=True).all()

# Buscar pedidos de um usuário
user_orders = Order.all().filter(user_id=123).order_by('order_date', 'DESC').all()

# Buscar pedidos por status
pending_orders = Order.all().filter(status="pending").all()
```

---

## Referência da API

### Model

```python
class Model:
    # Métodos de classe
    @classmethod
    def all(cls) -> Query
    @classmethod
    def get(cls, **kwargs) -> Optional[Model]
    @classmethod
    def create(cls, **kwargs) -> Model
    @classmethod
    def delete(cls, **kwargs) -> bool
    
    # Métodos de instância
    def save(self) -> None
    def update(self, **kwargs) -> None
    def delete(self) -> None
    def model_dump(self) -> dict
    def model_dump_json(self) -> str
```

### Query

```python
class Query:
    def filter(self, **kwargs) -> Query
    def order_by(self, field: str, direction: str = "ASC") -> Query
    def limit(self, limit: int) -> Query
    def allow_filtering(self) -> Query
    def all(self) -> List[Model]
    def first(self) -> Optional[Model]
    def count(self) -> int
```

### Fields

```python
# Tipos básicos
Text(primary_key=False, required=False, default=None, index=False)
Integer(primary_key=False, required=False, default=None, index=False)
Float(primary_key=False, required=False, default=None, index=False)
Boolean(primary_key=False, required=False, default=None, index=False)
Timestamp(primary_key=False, required=False, default=None, index=False)
UUID(primary_key=False, required=False, default=None, index=False)

# Tipos de coleção
List(inner_field: BaseField, **kwargs)
Set(inner_field: BaseField, **kwargs)
Map(key_field: BaseField, value_field: BaseField, **kwargs)
Tuple(*field_types: BaseField, **kwargs)

# User-Defined Types
UserDefinedType(udt_class: Type, **kwargs)
```

### Connection

```python
def connect(
    contact_points: List[str],
    keyspace: str,
    port: int = 9042,
    username: Optional[str] = None,
    password: Optional[str] = None,
    ssl_options: Optional[dict] = None,
    protocol_version: int = 4,
    connect_timeout: int = 10,
    request_timeout: int = 30
) -> None

def disconnect() -> None
def get_session() -> Session
def execute(query: str, params: Optional[List] = None) -> ResultSet
```

### Batch

```python
class BatchQuery:
    def __enter__(self) -> BatchQuery
    def __exit__(self, exc_type, exc_val, exc_tb) -> None
```

### Exceptions

```python
class ValidationError(Exception)
class ConnectionError(Exception)
class QueryError(Exception)
class SchemaError(Exception)
```

---

## Conclusão

CaspyORM oferece uma solução completa e moderna para trabalhar com Apache Cassandra em Python. Com sua interface intuitiva, validação automática de tipos, suporte completo aos recursos do Cassandra e ferramentas de desenvolvimento, é a escolha ideal para projetos que precisam de performance e escalabilidade.

Para mais informações, exemplos e atualizações, visite:
- [Documentação Oficial](https://caspyorm.dev)
- [GitHub Repository](https://github.com/caspyorm/caspyorm)
- [PyPI Package](https://pypi.org/project/caspyorm/)

## Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

## Contribuindo

Contribuições são bem-vindas! Por favor, leia o [guia de contribuição](CONTRIBUTING.md) antes de submeter um pull request.