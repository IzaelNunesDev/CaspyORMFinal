import pytest
from caspyorm.core.fields import Text, Integer, Boolean, List, UserDefinedType, Map, Set, Tuple
from caspyorm.core.model import Model
from caspyorm.types.usertype import UserType
from caspyorm.utils.exceptions import ValidationError
import json

class AuxModel(Model):
    __table_name__ = "aux_model"
    id = Integer(primary_key=True, required=True)
    name = Text(required=True)
    description = Text(default="sem descrição")
    note = Text()

# --- Testes já existentes ---
def test_required_field():
    with pytest.raises(ValidationError):
        AuxModel(id=1)  # Falta 'name' obrigatório

def test_type_validation():
    with pytest.raises(ValidationError):
        AuxModel(id="abc", name="ok")  # id deve ser int
    with pytest.raises(ValidationError):
        AuxModel(id=1, name=123)  # name deve ser str

def test_default_value():
    obj = AuxModel(id=2, name="Teste")
    assert obj.description == "sem descrição"
    assert obj.note is None

def test_optional_field_omitted():
    obj = AuxModel(id=3, name="Outro")
    assert obj.note is None

def test_model_from_dict():
    data = {"id": 4, "name": "Dict", "description": "desc", "note": "n"}
    obj = AuxModel(**data)
    assert obj.id == 4
    assert obj.name == "Dict"
    assert obj.description == "desc"
    assert obj.note == "n"

def test_model_from_json():
    json_str = '{"id": 5, "name": "Json", "description": "d2", "note": "n2"}'
    data = json.loads(json_str)
    obj = AuxModel(**data)
    assert obj.id == 5
    assert obj.name == "Json"
    assert obj.description == "d2"
    assert obj.note == "n2"

# --- Testes para Integer ---
def test_integer_field():
    class M(Model):
        __table_name__ = "m"
        id = Integer(primary_key=True)
        x = Integer(required=True)
    m = M(id=1, x=10)
    assert m.x == 10
    with pytest.raises(ValidationError):
        M(id=2, x="notint")
    m2 = M(id=3, x=5)
    assert isinstance(m2.x, int)

# --- Testes para Boolean ---
def test_boolean_field():
    class M(Model):
        __table_name__ = "m"
        id = Integer(primary_key=True)
        b = Boolean(default=False)
    m = M(id=1)
    assert m.b is False
    m2 = M(id=2, b=True)
    assert m2.b is True
    m3 = M(id=3, b="true")
    assert m3.b is True
    with pytest.raises(ValidationError):
        M(id=4, b="notbool")

# --- Testes para List ---
def test_list_field():
    class M(Model):
        __table_name__ = "m"
        id = Integer(primary_key=True)
        l = List(Text(), default=list)
    m = M(id=1)
    assert m.l == []
    m2 = M(id=2, l=["a", "b"])
    assert m2.l == ["a", "b"]
    with pytest.raises(ValidationError):
        M(id=3, l=[1, 2])  # Deve ser lista de str

# --- Testes para UDT ---
class Endereco(UserType):
    rua = Text()
    numero = Integer()

class ModelUDT(Model):
    __table_name__ = "model_udt"
    id = Integer(primary_key=True)
    endereco = UserDefinedType(Endereco)

def test_udt_field():
    e = Endereco(rua="Rua X", numero=123)
    m = ModelUDT(id=1, endereco=e)
    assert m.endereco.rua == "Rua X"
    assert m.endereco.numero == 123
    # Serialização
    d = m.model_dump()
    assert d["endereco"].rua == "Rua X"
    # Desserialização de dict
    m2 = ModelUDT(id=2, endereco={"rua": "Rua Y", "numero": 456})
    assert isinstance(m2.endereco, Endereco)
    assert m2.endereco.rua == "Rua Y"
    assert m2.endereco.numero == 456
    # Tipo inválido
    with pytest.raises(ValidationError):
        ModelUDT(id=3, endereco=123) 

# --- Testes para Map ---
def test_map_field():
    class M(Model):
        __table_name__ = "m_map"
        id = Integer(primary_key=True)
        m = Map(Text(), Integer(), default=dict)
    obj = M(id=1)
    assert obj.m == {}
    obj2 = M(id=2, m={"a": 1, "b": 2})
    assert obj2.m["a"] == 1
    with pytest.raises(ValidationError):
        M(id=3, m={1: "x"})  # Chave e valor inválidos

# --- Testes para Set ---
def test_set_field():
    class M(Model):
        __table_name__ = "m_set"
        id = Integer(primary_key=True)
        s = Set(Text(), default=set)
    obj = M(id=1)
    assert obj.s == set()
    obj2 = M(id=2, s={"a", "b"})
    assert "a" in obj2.s
    with pytest.raises(ValidationError):
        M(id=3, s={1, 2})  # Deve ser set de str

# --- Testes para Tuple ---
def test_tuple_field():
    class M(Model):
        __table_name__ = "m_tuple"
        id = Integer(primary_key=True)
        t = Tuple(Text(), Integer(), Boolean())
    obj = M(id=1, t=("x", 2, True))
    assert obj.t == ("x", 2, True)
    with pytest.raises(ValidationError):
        M(id=2, t=(1, "x", "notbool")) 