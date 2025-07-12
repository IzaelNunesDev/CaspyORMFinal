import pytest
from caspyorm.core.fields import Text, UserDefinedType
from caspyorm.core.model import Model
from caspyorm.types.usertype import UserType
from caspyorm.core.connection import get_session
from caspyorm.types.batch import BatchQuery
from caspyorm.utils.schema import create_udt, create_table

# Definição do UDT Address
class Address(UserType):
    street = Text()
    city = Text()
    zip_code = Text()

# Modelo temporário para teste
class NYC311_UDT(Model):
    __table_name__ = "nyc_311_udt_test"
    unique_key = Text(primary_key=True, required=True)
    complaint_type = Text()
    address = UserDefinedType(Address)

@pytest.fixture(scope="module", autouse=True)
def setup_udt_table(db_connection):
    """Prepara a tabela e o UDT para os testes deste módulo."""
    session = get_session()
    create_udt(session, Address, "nyc_data")
    create_table(session, NYC311_UDT)
    # O yield aqui garante que a limpeza (se houver) aconteça no final do módulo
    yield

def make_address(i):
    return Address(street=f"Rua {i}", city="NYC", zip_code=f"1000{i}")

def make_obj(i):
    return NYC311_UDT(
        unique_key=f"batch_udt_{i}",
        complaint_type="Noise",
        address=make_address(i)
    )

def test_batch_insert_and_read_udt():
    objs = [make_obj(i) for i in range(5)]
    # Inserção em batch
    with BatchQuery():
        for obj in objs:
            obj.save()
    # Leitura e verificação
    results = list(NYC311_UDT.all().all())
    assert len(results) >= 5
    for i, obj in enumerate(objs):
        found = NYC311_UDT.get(unique_key=obj.unique_key)
        assert found is not None
        assert found.address.street == f"Rua {i}"
        assert found.address.city == "NYC"

def test_batch_update_udt():
    # Atualiza todos os endereços
    for i in range(5):
        obj = NYC311_UDT.get(unique_key=f"batch_udt_{i}")
        obj.address.street = f"Avenida {i}"
        obj.save()
    # Verifica atualização
    for i in range(5):
        found = NYC311_UDT.get(unique_key=f"batch_udt_{i}")
        assert found.address.street == f"Avenida {i}"

def test_batch_delete_udt():
    # Deleta todos em batch
    with BatchQuery():
        for i in range(5):
            obj = NYC311_UDT.get(unique_key=f"batch_udt_{i}")
            if obj:
                obj.delete()
    # Verifica deleção
    for i in range(5):
        assert NYC311_UDT.get(unique_key=f"batch_udt_{i}") is None 