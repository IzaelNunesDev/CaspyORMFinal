import pytest
from caspyorm.types.usertype import UserType
from caspyorm.core.fields import Text, Integer
from caspyorm.utils.exceptions import ValidationError

class Endereco(UserType):
    rua = Text(required=True)
    numero = Integer(default=0)
    complemento = Text()

def test_usertype_creation():
    e = Endereco(rua="Rua A", numero=10)
    assert e.rua == "Rua A"
    assert e.numero == 10
    assert e.complemento is None

def test_usertype_required():
    with pytest.raises(ValidationError):
        Endereco(numero=5)  # Falta rua obrigatória

def test_usertype_type_validation():
    with pytest.raises(ValidationError):
        Endereco(rua=123, numero=5)
    with pytest.raises(ValidationError):
        Endereco(rua="Rua", numero="dez")

def test_usertype_default():
    e = Endereco(rua="Rua B")
    assert e.numero == 0
    assert e.complemento is None

def test_usertype_serialization():
    e = Endereco(rua="Rua C", numero=7, complemento="apto 1")
    d = e.model_dump()
    assert d["rua"] == "Rua C"
    assert d["numero"] == 7
    assert d["complemento"] == "apto 1"
    # Desserialização
    e2 = Endereco(**d)
    assert e2.rua == "Rua C"
    assert e2.numero == 7
    assert e2.complemento == "apto 1" 