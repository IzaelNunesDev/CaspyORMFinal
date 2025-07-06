# CaspyORM

Um ORM moderno e Pythonic para Apache Cassandra, inspirado no Pydantic e focado em produtividade e performance.

## 🚀 Instalação

Instale diretamente do PyPI:

```bash
pip install caspyorm
```

### Requisitos
- Python >= 3.8
- Cassandra rodando e acessível

As dependências principais (cassandra-driver, pydantic, typing-extensions) são instaladas automaticamente via pip.

## 🎯 Exemplo de Uso Básico

```python
from caspyorm import Model, fields, connection
import uuid

# Configurar conexão (ajuste para seu host/keyspace)
connection.setup(['localhost'], 'meu_keyspace')

class Usuario(Model):
    __table_name__ = 'usuarios'
    id = fields.UUID(primary_key=True)
    nome = fields.Text(required=True)
    email = fields.Text(index=True)
    ativo = fields.Boolean(default=True)

# Sincronizar schema (cria tabela e índices)
Usuario.sync_table()

# CRUD básico
usuario = Usuario.create(
    id=uuid.uuid4(),
    nome="João Silva",
    email="joao@email.com"
)

# Buscar por ID
usuario = Usuario.get(id=usuario.id)

# Consultas com filtros
usuarios_ativos = Usuario.filter(ativo=True).all()
usuario_por_email = Usuario.filter(email="joao@email.com").first()
```

## ⚡ Integração com FastAPI (Opcional)

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

## 🔧 Integração com Pydantic (Opcional)

Você pode gerar modelos Pydantic a partir dos seus modelos CaspyORM:

```python
PydanticUsuario = Usuario.as_pydantic()
usuario = Usuario.get(id=...)
usuario_pydantic = usuario.to_pydantic_model()
```

## 📚 Documentação

Para exemplos avançados, testes e documentação completa, acesse:
- [Documentação Oficial](https://caspyorm.readthedocs.io)
- [Repositório no GitHub](https://github.com/caspyorm/caspyorm)

---

Licença MIT. Desenvolvido por CaspyORM Team e colaboradores. 