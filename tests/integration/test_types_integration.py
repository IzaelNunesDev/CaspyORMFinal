import pytest
from caspyorm.core.fields import Text, Integer, Map, Set, Tuple
from caspyorm.core.model import Model
from caspyorm.core.connection import get_session, connect
from caspyorm.utils.schema import create_table

KEYSPACE = "nyc_data"

class TypesModel(Model):
    __table_name__ = "types_integration_test"
    id = Integer(primary_key=True, required=True)
    tags = Set(Text(), default=set)
    attrs = Map(Text(), Integer(), default=dict)
    coords = Tuple(Integer(), Integer())

@pytest.fixture(scope="module", autouse=True)
def setup_types_table(db_connection):
    # Garante conexão ativa
    connect(contact_points=["172.18.0.2"], keyspace=KEYSPACE, port=9042)
    session = get_session()
    create_table(session, TypesModel)
    yield

def test_insert_and_read_types(db_connection):
    TypesModel(id=1, tags={"a", "b"}, attrs={"x": 10, "y": 20}, coords=(5, 7)).save()
    obj = TypesModel.get(id=1)
    assert obj.tags == {"a", "b"}
    assert obj.attrs["x"] == 10
    assert obj.coords == (5, 7)

def test_update_types(db_connection):
    obj = TypesModel.get(id=1)
    obj.tags.add("c")
    obj.attrs["z"] = 99
    obj.coords = (8, 9)
    obj.save()
    obj2 = TypesModel.get(id=1)
    assert "c" in obj2.tags
    assert obj2.attrs["z"] == 99
    assert obj2.coords == (8, 9)

def test_delete_types(db_connection):
    obj = TypesModel.get(id=1)
    obj.delete()
    assert TypesModel.get(id=1) is None 