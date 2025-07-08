from fastapi import FastAPI, Depends
from caspyorm.model import Model
from caspyorm.fields import Text, Integer, UserDefinedType
from caspyorm.usertype import UserType
from caspyorm.contrib.fastapi import get_async_session, as_response_model
import asyncio

# Definição do UDT
class Address(UserType):
    street: Text = Text()
    city: Text = Text()

# Definição do modelo principal
class User(Model):
    __table_name__ = "users"
    id: Text = Text(primary_key=True)
    name: Text = Text()
    address: UserDefinedType = UserDefinedType(Address)

app = FastAPI()

@app.on_event("startup")
async def startup():
    # Conectar e criar tabela/UDT para o exemplo
    from caspyorm.connection import ConnectionManager
    manager = ConnectionManager()
    manager.connect(keyspace="test_keyspace")
    manager.register_udt(Address)
    manager.sync_udts()
    User.create_table()
    # Inserir um usuário de exemplo se não existir
    if not User.objects.filter(id="1").exists():
        User(id="1", name="João", address=Address(street="Rua A", city="Cidade B")).save()

@app.get("/users/{user_id}")
async def get_user(user_id: str, session=Depends(get_async_session)):
    user = await User.get_async(id=user_id)
    if not user:
        return {"error": "Usuário não encontrado"}
    return as_response_model(user) 