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
- ✅ CLI robusto via `caspy`
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
- Driver oficial do Cassandra (cassandra-driver) será instalado automaticamente

## 🎯 Exemplo Básico

```python
from caspyorm import Model, fields, connection
import uuid

# Conectar ao Cassandra
connection.connect(contact_points=["localhost"], keyspace="meu_keyspace")

# Definição de modelo
class Usuario(Model):
    __table_name__ = "usuarios"
    id = fields.UUID(primary_key=True)
    nome = fields.Text(required=True)
    email = fields.Text(index=True)
    ativo = fields.Boolean(default=True)

# Criar tabela se necessário
Usuario.sync_table()

# Inserir dados
usuario = Usuario.create(
    id=uuid.uuid4(),
    nome="João Silva",
    email="joao@email.com"
)

# Buscar
usuario = Usuario.get(id=usuario.id)
usuarios_ativos = Usuario.filter(ativo=True).all()
```

## ⚡ Integração com FastAPI

```python
from fastapi import FastAPI, Depends
from caspyorm.contrib.fastapi import get_session, as_response_model
from seu_modulo import Usuario

app = FastAPI()

@app.get("/usuarios/{user_id}")
async def get_usuario(user_id: str, session = Depends(get_session)):
    usuario = await Usuario.get_async(id=user_id)
    return as_response_model(usuario)
```

## 🔧 Integração com Pydantic

Transforme modelos CaspyORM em modelos Pydantic automaticamente:

```python
PydanticUsuario = Usuario.as_pydantic()
usuario = Usuario.get(id=...)
usuario_pydantic = usuario.to_pydantic_model()
```

## 🖥️ CLI (Interface de Linha de Comando)

Instalado automaticamente como `caspy`.

### Comandos disponíveis:

| Comando | Descrição |
|---------|-----------|
| `caspy query` | Busca ou filtra objetos no banco de dados |
| `caspy models` | Lista todos os modelos disponíveis |
| `caspy connect` | Testa a conexão com o cluster |
| `caspy info` | Mostra informações sobre a CLI |

### Configuração via Variáveis de Ambiente:

```bash
export CASPY_HOSTS=localhost
export CASPY_KEYSPACE=biblioteca
export CASPY_PORT=9042
export CASPY_MODELS_PATH=models
```

### Exemplos de Uso:

```bash
# Testar conexão com keyspace específico
caspy connect --keyspace biblioteca

# Listar modelos disponíveis
caspy models

# Consultar dados
caspy query autor count --keyspace biblioteca
caspy query livro filter --filter "autor_id=123" --limit 5 --keyspace biblioteca
caspy query autor get --filter "email=joao@email.com" --keyspace biblioteca
```

## 🧾 Licença

MIT © 2024 - CaspyORM Team

Desenvolvido com ❤️ para a comunidade Python. 