import pytest
from caspyorm.core.fields import Integer, Text
from caspyorm.core.model import Model
from caspyorm.core.connection import get_session
from caspyorm.utils.schema import create_table
from caspyorm.utils.exceptions import ValidationError

KEYSPACE = "nyc_data"

class ErrorModel(Model):
    __table_name__ = "error_integration_test"
    id = Integer(primary_key=True, required=True)
    name = Text(required=True)

@pytest.fixture(scope="module", autouse=True)
def setup_error_table(db_connection):
    """Prepara a tabela para os testes de erro."""
    session = get_session()
    create_table(session, ErrorModel)
    yield

def test_duplicate_primary_key():
    ErrorModel(id=1, name="A").save()
    ErrorModel(id=1, name="B").save()  # Não deve levantar erro, sobrescreve
    obj = ErrorModel.get(id=1)
    assert obj.name == "B"

def test_invalid_type_insert():
    with pytest.raises(ValidationError):
        ErrorModel(id="notint", name="C").save()
    with pytest.raises(ValidationError):
        ErrorModel(id=2, name=123).save()

def test_missing_required_field():
    with pytest.raises(ValidationError):
        ErrorModel(id=3).save()
    with pytest.raises(ValidationError):
        ErrorModel(name="D").save()

def test_update_delete_nonexistent():
    obj = ErrorModel.get(id=999)
    assert obj is None
    # Update não faz nada, delete não levanta erro
    dummy = ErrorModel(id=999, name="X")
    dummy.delete() 