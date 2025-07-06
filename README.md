# 🚀 CaspyORM

> Um ORM moderno, rápido e Pythonic para Apache Cassandra — com suporte nativo a FastAPI, Pydantic e operações assíncronas.

[![PyPI version](https://badge.fury.io/py/caspyorm.svg)](https://pypi.org/project/caspyorm/)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

CaspyORM é uma biblioteca ORM poderosa e de alta performance para aplicações Python que utilizam o **Apache Cassandra** como banco de dados NoSQL. Inspirada no **Pydantic** e no estilo do **Django ORM**, ela oferece uma API intuitiva, tipada, e com suporte a validação, filtros encadeáveis, modelos dinâmicos e integração com FastAPI.

---

## 🛠️ Recursos Principais

- ✅ Definição de modelos via campos tipados (`fields.Text()`, `fields.UUID()`, etc.)
- ✅ Suporte completo a operações **síncronas** e **assíncronas**
- ✅ Integração com **Pydantic** (`as_pydantic()`)
- ✅ Compatível com **FastAPI** (injeção de sessão, serialização, etc.)
- ✅ CRUD, filtros, ordenação, paginação, contagem, existência
- ✅ Suporte a **tipos compostos**: `List`, `Set`, `Map`
- ✅ `bulk_create`, `update parcial`, `delete`, `collection updates`
- ✅ **Criação dinâmica de modelos** (`Model.create_model`)
- ✅ CLI robusto via `caspy` com mensagens de erro aprimoradas
- ✅ Compatível com Python 3.8+  
- ✅ Testado e com tipagem estática rigorosa (via `mypy`, `ruff`, `black`)

---

## 📦 Instalação

```bash
pip install caspyorm
```

### Requisitos
- Python 3.8 ou superior
- Apache Cassandra acessível (local ou remoto)
- Driver oficial do Cassandra (`cassandra-driver`) será instalado automaticamente

---

## 🚀 Guia de Uso

### 1. Conexão com o Cassandra

CaspyORM gerencia a conexão com o Cassandra através da instância global `connection`. É fundamental estabelecer a conexão antes de interagir com os modelos.

#### Conexão Síncrona

```python
from caspyorm import connection

# Conectar a um cluster local
connection.connect(contact_points=["localhost"], port=9042, keyspace="meu_keyspace")

# Ou com autenticação
# connection.connect(contact_points=["192.168.1.10"], username="user", password="password", keyspace="meu_keyspace")

# Certifique-se de desconectar ao finalizar a aplicação
# connection.disconnect()
```

#### Conexão Assíncrona (para uso com `asyncio` e frameworks como FastAPI)

```python
import asyncio
from caspyorm import connection

async def setup_connection():
    await connection.connect_async(contact_points=["localhost"], port=9042, keyspace="meu_keyspace_async")

async def teardown_connection():
    await connection.disconnect_async()

# Exemplo de uso em uma aplicação FastAPI:
# @app.on_event("startup")
# async def startup_event():
#     await setup_connection()
#
# @app.on_event("shutdown")
# async def shutdown_event():
#     await teardown_connection()
```

### 2. Definição de Modelos

Defina seus modelos herdando de `caspyorm.Model` e declarando os campos usando os tipos fornecidos em `caspyorm.fields`.

```python
from caspyorm import Model, fields
import uuid
from datetime import datetime

class Usuario(Model):
    """Modelo para representar usuários."""
    __table_name__ = "usuarios" # Nome da tabela no Cassandra

    # Campos do modelo
    id = fields.UUID(primary_key=True) # Chave primária, UUID é gerado automaticamente se não fornecido
    nome = fields.Text(required=True) # Campo de texto obrigatório
    email = fields.Text(index=True) # Campo de texto com índice secundário
    idade = fields.Integer(default=0) # Campo inteiro com valor padrão
    ativo = fields.Boolean(default=True) # Campo booleano com valor padrão
    tags = fields.List(fields.Text()) # Lista de textos
    configuracoes = fields.Map(fields.Text(), fields.Text()) # Mapa de texto para texto
    criado_em = fields.Timestamp(default=lambda: datetime.now()) # Timestamp com valor padrão (função)
```

#### Criação Dinâmica de Modelos

Você pode criar modelos programaticamente em tempo de execução:

```python
from caspyorm import Model, fields
import uuid

# Criar um modelo de Produto dinamicamente
Produto = Model.create_model(
    name="Produto",
    fields={
        "id": fields.UUID(primary_key=True),
        "nome": fields.Text(required=True),
        "preco": fields.Float(),
        "categorias": fields.Set(fields.Text())
    },
    table_name="produtos_dinamicos"
)

# Agora você pode usar 'Produto' como qualquer outro modelo
# produto_novo = Produto(id=uuid.uuid4(), nome="Laptop", preco=1200.50)
```

### 3. Sincronização do Schema (Criação de Tabelas)

Para garantir que as tabelas correspondentes aos seus modelos existam no Cassandra, use `sync_table()`.

```python
# Após definir seus modelos e conectar ao Cassandra
Usuario.sync_table() # Sincroniza a tabela 'usuarios'
Produto.sync_table() # Sincroniza a tabela 'produtos_dinamicos'

# Para sincronização assíncrona
# await Usuario.sync_table_async()
```
**Importante:** `sync_table()` criará tabelas se elas não existirem. Ele **não** fará alterações destrutivas (como remover colunas ou alterar chaves primárias) automaticamente. Para essas operações, você precisará de intervenção manual ou ferramentas de migração de schema.

### 4. Operações CRUD (Create, Read, Update, Delete)

#### Criar (Create)

```python
# Síncrono
usuario_novo = Usuario.create(
    id=uuid.uuid4(),
    nome="Alice Wonderland",
    email="alice@example.com",
    idade=30
)
print(f"Usuário criado: {usuario_novo.nome}")

# Assíncrono
async def criar_usuario_async():
    novo_id = uuid.uuid4()
    usuario_async = await Usuario.create_async(
        id=novo_id,
        nome="Bob Esponja",
        email="bob@example.com",
        idade=25
    )
    print(f"Usuário assíncrono criado: {usuario_async.nome}")
```

#### Buscar (Read)

```python
# Buscar um único objeto pela chave primária (síncrono)
usuario_encontrado = Usuario.get(id=usuario_novo.id)
if usuario_encontrado:
    print(f"Usuário encontrado: {usuario_encontrado.nome}")

# Buscar um único objeto (assíncrono)
async def buscar_usuario_async(user_id):
    usuario_async = await Usuario.get_async(id=user_id)
    if usuario_async:
        print(f"Usuário assíncrono encontrado: {usuario_async.nome}")

# Filtrar múltiplos objetos (síncrono)
usuarios_maiores_25 = Usuario.filter(idade__gt=25, ativo=True).all()
for u in usuarios_maiores_25:
    print(f"Usuário > 25: {u.nome}")

# Filtrar múltiplos objetos (assíncrono)
async def filtrar_usuarios_async():
    usuarios_ativos = await Usuario.filter(ativo=True).all_async()
    for u in usuarios_ativos:
        print(f"Usuário ativo: {u.nome}")

# Contar objetos
total_usuarios = Usuario.filter(ativo=True).count()
print(f"Total de usuários ativos: {total_usuarios}")

# Verificar existência
existe_joao = Usuario.filter(nome="João Silva").exists()
print(f"João Silva existe? {existe_joao}")
```

#### Atualizar (Update)

Você pode atualizar uma instância existente ou realizar um update parcial.

```python
# Atualizar uma instância existente (síncrono)
if usuario_encontrado:
    usuario_encontrado.nome = "Alice Smith"
    usuario_encontrado.idade = 31
    usuario_encontrado.save() # Realiza um upsert completo
    print(f"Usuário atualizado via save(): {usuario_encontrado.nome}")

# Update parcial (síncrono) - atualiza apenas os campos fornecidos
if usuario_encontrado:
    usuario_encontrado.update(email="alice.smith@newemail.com")
    print(f"Email atualizado via update(): {usuario_encontrado.email}")

# Update parcial (assíncrono)
async def atualizar_usuario_parcial_async(user_id):
    usuario_para_atualizar = await Usuario.get_async(id=user_id)
    if usuario_para_atualizar:
        await usuario_para_atualizar.update_async(ativo=False)
        print(f"Usuário {usuario_para_atualizar.nome} desativado.")

# Atualizar campos de coleção (List, Set) atomicamente
# Adicionar tags
usuario_novo.update_collection('tags', add=['premium', 'beta'])
# Remover tags
usuario_novo.update_collection('tags', remove=['beta'])
```

#### Deletar (Delete)

```python
# Deletar uma instância específica (síncrono)
if usuario_novo:
    usuario_novo.delete()
    print(f"Usuário {usuario_novo.nome} deletado.")

# Deletar uma instância específica (assíncrono)
async def deletar_usuario_async(user_id):
    usuario_para_deletar = await Usuario.get_async(id=user_id)
    if usuario_para_deletar:
        await usuario_para_deletar.delete_async()
        print(f"Usuário {usuario_para_deletar.nome} deletado assincronamente.")

# Deletar múltiplos objetos por filtro (síncrono)
# CUIDADO: A deleção por filtro no Cassandra exige que todas as partition keys estejam no filtro.
# Exemplo: Usuario.filter(id=some_id, nome=some_name).delete() # Se (id, nome) for a partition key
```

#### Operações em Lote (Bulk Operations)

```python
# Criar múltiplos usuários em lote (síncrono)
usuarios_para_criar = [
    Usuario(id=uuid.uuid4(), nome="Carlos", email="carlos@example.com"),
    Usuario(id=uuid.uuid4(), nome="Diana", email="diana@example.com")
]
Usuario.bulk_create(usuarios_para_criar)
print(f"{len(usuarios_para_criar)} usuários criados em lote.")

# Criar múltiplos usuários em lote (assíncrono)
async def criar_usuarios_em_lote_async():
    mais_usuarios = [
        Usuario(id=uuid.uuid4(), nome="Eva", email="eva@example.com"),
        Usuario(id=uuid.uuid4(), nome="Felipe", email="felipe@example.com")
    ]
    await Usuario.bulk_create_async(mais_usuarios)
    print(f"{len(mais_usuarios)} usuários criados em lote assincronamente.")
```

#### Paginação

```python
# Paginação síncrona
page_size = 2
current_paging_state = None
while True:
    usuarios_pagina, next_paging_state = Usuario.filter(ativo=True).page(page_size=page_size, paging_state=current_paging_state)
    if not usuarios_pagina:
        break
    for u in usuarios_pagina:
        print(f"Página: {u.nome}")
    current_paging_state = next_paging_state
    if not current_paging_state:
        break

# Paginação assíncrona
async def paginar_usuarios_async():
    page_size = 2
    current_paging_state = None
    while True:
        usuarios_pagina, next_paging_state = await Usuario.filter(ativo=True).page_async(page_size=page_size, paging_state=current_paging_state)
        if not usuarios_pagina:
            break
    for u in usuarios_pagina:
        print(f"Página Async: {u.nome}")
    current_paging_state = next_paging_state
    if not current_paging_state:
        break
```

#### `ALLOW FILTERING`

Por padrão, CaspyORM não adiciona `ALLOW FILTERING` automaticamente para consultas que não usam chaves primárias ou índices. Se você precisar de uma consulta que exija `ALLOW FILTERING` (e estiver ciente das implicações de performance), pode especificá-lo explicitamente:

```python
# CUIDADO: Use ALLOW FILTERING com cautela, pode impactar a performance em grandes datasets.
usuarios_por_idade = Usuario.filter(idade=30).all(allow_filtering=True)
```

### 5. Integração com FastAPI

CaspyORM oferece helpers para facilitar a integração com FastAPI, incluindo injeção de dependência de sessão e conversão de modelos para respostas HTTP.

```python
from fastapi import FastAPI, Depends, HTTPException, status
from caspyorm import Model, fields, connection
from caspyorm.contrib.fastapi import get_async_session, as_response_model, create_response_model, handle_caspyorm_errors
import uuid
from datetime import datetime
from typing import List, Optional

# Definição do modelo (pode estar em outro arquivo, ex: models.py)
class Item(Model):
    __table_name__ = "items"
    id = fields.UUID(primary_key=True)
    nome = fields.Text(required=True)
    descricao = fields.Text()
    preco = fields.Float()

# Criar um modelo Pydantic para a resposta (opcional, mas boa prática)
ItemResponse = create_response_model(Item, exclude=["descricao"])

app = FastAPI()

# Eventos de startup/shutdown para gerenciar a conexão Cassandra
@app.on_event("startup")
async def startup_event():
    await connection.connect_async(contact_points=["localhost"], keyspace="fastapi_keyspace")
    await Item.sync_table_async() # Sincroniza a tabela ao iniciar

@app.on_event("shutdown")
async def shutdown_event():
    await connection.disconnect_async()

@app.post("/items/", response_model=ItemResponse)
@handle_caspyorm_errors # Decorador para tratamento de erros CaspyORM
async def create_item(item_data: Item): # Item é um modelo CaspyORM, FastAPI valida com Pydantic
    item_data.id = uuid.uuid4() # Atribui um ID
    await item_data.save_async()
    return as_response_model(item_data)

@app.get("/items/{item_id}", response_model=ItemResponse)
@handle_caspyorm_errors
async def read_item(item_id: uuid.UUID, session = Depends(get_async_session)):
    item = await Item.get_async(id=item_id)
    if not item:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item não encontrado")
    return as_response_model(item)

@app.get("/items/", response_model=List[ItemResponse])
@handle_caspyorm_errors
async def read_items(skip: int = 0, limit: int = 10, session = Depends(get_async_session)):
    # Exemplo de paginação com QuerySet
    items = await Item.all().limit(limit).all_async() # Simplificado, paginação real usaria .page_async
    return [as_response_model(item) for item in items]
```

### 6. CLI (Interface de Linha de Comando)

A ferramenta de linha de comando `caspy` é instalada automaticamente e permite interagir com seus modelos e o Cassandra.

#### Configuração via Variáveis de Ambiente

Você pode configurar a conexão e o caminho dos seus modelos usando variáveis de ambiente:

```bash
export CASPY_HOSTS="localhost,192.168.1.10" # Lista de hosts separados por vírgula
export CASPY_KEYSPACE="minha_aplicacao_keyspace" # Keyspace padrão
export CASPY_PORT="9042" # Porta do Cassandra
export CASPY_MODELS_PATH="meu_projeto.models" # Caminho do módulo onde seus modelos estão definidos
```

Se `CASPY_MODELS_PATH` não for definido, o CLI tentará importar um arquivo `models.py` no diretório de trabalho atual.

#### Comandos Disponíveis

| Comando | Descrição |
|---------|-----------|
| `caspy query <model_name> <command>` | Busca ou filtra objetos no banco de dados. Comandos: `get`, `filter`, `count`, `exists`, `delete`. |
| `caspy models` | Lista todos os modelos CaspyORM disponíveis no módulo configurado. |
| `caspy connect` | Testa a conexão com o cluster Cassandra usando a configuração atual. |
| `caspy info` | Mostra informações sobre a CLI e a configuração atual. |
| `caspy version` | Exibe a versão do CaspyORM CLI. |

#### Filtros Avançados e Operadores

O comando `caspy query` suporta filtros avançados com operadores:

-   **Operadores:** `__gt` (maior que), `__lt` (menor que), `__gte` (maior ou igual), `__lte` (menor ou igual), `__in` (contido em uma lista).
-   **`ALLOW FILTERING`:** Por padrão, o CLI não adiciona `ALLOW FILTERING`. Se sua consulta exigir, você pode adicioná-lo explicitamente (use com cautela).

#### Exemplos de Uso do CLI

```bash
# Testar conexão com keyspace específico
caspy connect --keyspace meu_keyspace

# Listar modelos disponíveis no caminho configurado
caspy models

# Consultar um único usuário pelo nome (exige índice ou ser parte da PK)
caspy query usuario get --filter nome="João Silva"

# Filtrar livros por autor_id e limitar resultados
caspy query livro filter --filter autor_id="123e4567-e89b-12d3-a456-426614174000" --limit 5 --keyspace biblioteca

# Contar usuários ativos
caspy query usuario count --filter ativo=true

# Filtrar usuários por idade e nome (exige ALLOW FILTERING se idade/nome não forem PK/index)
# caspy query usuario filter --filter idade__gt=30 --filter nome__in="joao,maria" --allow-filtering
```

---

## 🤝 Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues, enviar pull requests ou sugerir melhorias.

---

## 🧾 Licença

MIT © 2024 - CaspyORM Team

Desenvolvido com ❤️ para a comunidade Python.