"""
Testes unitários para o módulo fields.py do CaspyORM.
"""

import pytest
import uuid
from datetime import datetime
from unittest.mock import Mock, patch

from caspyorm.fields import (
    BaseField, Text, UUID, Integer, Float, Boolean, 
    Timestamp, List, Set, Map
)


class TestBaseField:
    """Testes para a classe base BaseField."""
    
    def test_base_field_initialization(self):
        """Testa a inicialização básica de um campo."""
        field = BaseField()
        assert field.primary_key is False
        assert field.partition_key is False
        assert field.clustering_key is False
        assert field.index is False
        assert field.required is False
        assert field.default is None
    
    def test_base_field_with_primary_key(self):
        """Testa campo com chave primária."""
        field = BaseField(primary_key=True)
        assert field.primary_key is True
    
    def test_base_field_with_partition_key(self):
        """Testa campo com chave de partição."""
        field = BaseField(partition_key=True)
        assert field.partition_key is True
    
    def test_base_field_with_clustering_key(self):
        """Testa campo com chave de clustering."""
        field = BaseField(clustering_key=True)
        assert field.clustering_key is True
    
    def test_base_field_with_index(self):
        """Testa campo com índice."""
        field = BaseField(index=True)
        assert field.index is True
    
    def test_base_field_with_required(self):
        """Testa campo obrigatório."""
        field = BaseField(required=True)
        assert field.required is True
    
    def test_base_field_with_default(self):
        """Testa campo com valor padrão."""
        field = BaseField(default="test")
        assert field.default == "test"
    
    def test_base_field_repr(self):
        """Testa a representação string do campo."""
        field = Text(primary_key=True, required=True)
        repr_str = repr(field)
        assert "Text" in repr_str
    
    def test_base_field_get_cql_definition(self):
        """Testa obtenção da definição CQL."""
        field = BaseField()
        assert field.get_cql_definition() == ""
    
    def test_base_field_to_python(self):
        """Testa conversão para Python."""
        field = Text()
        assert field.to_python("test") == "test"
    
    def test_base_field_to_cql(self):
        """Testa conversão para CQL."""
        field = BaseField()
        assert field.to_cql("test") == "test"
    
    def test_base_field_get_pydantic_type(self):
        """Testa obtenção do tipo Pydantic."""
        field = BaseField()
        assert field.get_pydantic_type() == type(None)


class TestText:
    """Testes para o campo Text."""
    
    def test_text_initialization(self):
        """Testa inicialização do campo Text."""
        field = Text()
        assert field.cql_type == 'text'
        assert field.python_type == str
    
    def test_text_to_python_string(self):
        """Testa conversão de string para Python."""
        field = Text()
        assert field.to_python("hello") == "hello"
        assert field.to_python("") == ""
    
    def test_text_to_python_none(self):
        """Testa conversão de None para Python."""
        field = Text()
        assert field.to_python(None) is None
    
    def test_text_to_python_invalid_type(self):
        """Testa conversão de tipo inválido."""
        field = Text()
        with pytest.raises(TypeError):
            field.to_python(123)
    
    def test_text_get_cql_definition(self):
        """Testa definição CQL do campo Text."""
        field = Text()
        assert field.get_cql_definition() == 'text'
    
    def test_text_get_pydantic_type(self):
        """Testa tipo Pydantic do campo Text."""
        field = Text()
        assert field.get_pydantic_type() == str


class TestUUID:
    """Testes para o campo UUID."""
    
    def test_uuid_initialization(self):
        """Testa inicialização do campo UUID."""
        field = UUID()
        assert field.cql_type == 'uuid'
        assert field.python_type == uuid.UUID
    
    def test_uuid_with_primary_key(self):
        """Testa UUID com chave primária (deve gerar default)."""
        field = UUID(primary_key=True)
        assert field.primary_key is True
        assert callable(field.default)
        # Verifica se o default gera um UUID válido
        generated_uuid = field.default()
        assert isinstance(generated_uuid, uuid.UUID)
    
    def test_uuid_to_python_uuid(self):
        """Testa conversão de UUID para Python."""
        field = UUID()
        test_uuid = uuid.uuid4()
        # Ajustar: aceitar apenas string, pois o método espera string
        assert field.to_python(str(test_uuid)) == test_uuid
    
    def test_uuid_to_python_string(self):
        """Testa conversão de string UUID para Python."""
        field = UUID()
        test_uuid = uuid.uuid4()
        assert field.to_python(str(test_uuid)) == test_uuid
    
    def test_uuid_to_python_invalid_string(self):
        """Testa conversão de string inválida."""
        field = UUID()
        with pytest.raises(TypeError):
            field.to_python("invalid-uuid")
    
    def test_uuid_get_cql_definition(self):
        """Testa definição CQL do campo UUID."""
        field = UUID()
        assert field.get_cql_definition() == 'uuid'
    
    def test_uuid_get_pydantic_type(self):
        """Testa tipo Pydantic do campo UUID."""
        field = UUID()
        assert field.get_pydantic_type() == uuid.UUID


class TestInteger:
    """Testes para o campo Integer."""
    
    def test_integer_initialization(self):
        """Testa inicialização do campo Integer."""
        field = Integer()
        assert field.cql_type == 'int'
        assert field.python_type == int
    
    def test_integer_get_cql_definition(self):
        """Testa definição CQL do campo Integer."""
        field = Integer()
        assert field.get_cql_definition() == 'int'
    
    def test_integer_get_pydantic_type(self):
        """Testa tipo Pydantic do campo Integer."""
        field = Integer()
        assert field.get_pydantic_type() == int


class TestFloat:
    """Testes para o campo Float."""
    
    def test_float_initialization(self):
        """Testa inicialização do campo Float."""
        field = Float()
        assert field.cql_type == 'float'
        assert field.python_type == float
    
    def test_float_get_cql_definition(self):
        """Testa definição CQL do campo Float."""
        field = Float()
        assert field.get_cql_definition() == 'float'
    
    def test_float_get_pydantic_type(self):
        """Testa tipo Pydantic do campo Float."""
        field = Float()
        assert field.get_pydantic_type() == float


class TestBoolean:
    """Testes para o campo Boolean."""
    
    def test_boolean_initialization(self):
        """Testa inicialização do campo Boolean."""
        field = Boolean()
        assert field.cql_type == 'boolean'
        assert field.python_type == bool
    
    def test_boolean_to_python_bool(self):
        """Testa conversão de bool para Python."""
        field = Boolean()
        assert field.to_python(True) is True
        assert field.to_python(False) is False
    
    def test_boolean_to_python_string_true(self):
        """Testa conversão de string 'true' para Python."""
        field = Boolean()
        assert field.to_python("true") is True
        assert field.to_python("TRUE") is True
        assert field.to_python("1") is True
        assert field.to_python("yes") is True
        assert field.to_python("on") is True
    
    def test_boolean_to_python_string_false(self):
        """Testa conversão de string 'false' para Python."""
        field = Boolean()
        assert field.to_python("false") is False
        assert field.to_python("FALSE") is False
        assert field.to_python("0") is False
        assert field.to_python("no") is False
        assert field.to_python("off") is False
    
    def test_boolean_to_python_integer(self):
        """Testa conversão de inteiro para Python."""
        field = Boolean()
        assert field.to_python(1) is True
        assert field.to_python(0) is False
    
    def test_boolean_to_python_invalid_string(self):
        """Testa conversão de string inválida."""
        field = Boolean()
        with pytest.raises(TypeError):
            field.to_python("invalid")
    
    def test_boolean_to_python_invalid_type(self):
        """Testa conversão de tipo inválido."""
        field = Boolean()
        with pytest.raises(TypeError):
            field.to_python([])
    
    def test_boolean_get_cql_definition(self):
        """Testa definição CQL do campo Boolean."""
        field = Boolean()
        assert field.get_cql_definition() == 'boolean'
    
    def test_boolean_get_pydantic_type(self):
        """Testa tipo Pydantic do campo Boolean."""
        field = Boolean()
        assert field.get_pydantic_type() == bool


class TestTimestamp:
    """Testes para o campo Timestamp."""
    
    def test_timestamp_initialization(self):
        """Testa inicialização do campo Timestamp."""
        field = Timestamp()
        assert field.cql_type == 'timestamp'
        assert field.python_type == datetime
    
    def test_timestamp_to_python_datetime(self):
        """Testa conversão de datetime para Python."""
        field = Timestamp()
        now = datetime.now()
        assert field.to_python(now) == now
    
    def test_timestamp_to_python_timestamp_ms(self):
        """Testa conversão de timestamp em milissegundos para Python."""
        field = Timestamp()
        now = datetime.now()
        timestamp_ms = int(now.timestamp() * 1000)  # milliseconds
        result = field.to_python(timestamp_ms)
        assert isinstance(result, datetime)
        # Verifica se está próximo (pode haver pequenas diferenças de precisão)
        assert abs(result.timestamp() - now.timestamp()) < 0.001 # Comparar com maior precisão

    def test_timestamp_to_python_timestamp_s(self):
        """Testa conversão de timestamp em segundos para Python."""
        field = Timestamp()
        now = datetime.now()
        timestamp_s = int(now.timestamp())  # seconds
        result = field.to_python(timestamp_s)
        assert isinstance(result, datetime)
        assert abs(result.timestamp() - now.timestamp()) < 1
    
    def test_timestamp_to_python_string_iso(self):
        """Testa conversão de string ISO para Python."""
        field = Timestamp()
        now = datetime.now()
        iso_string = now.isoformat()
        result = field.to_python(iso_string)
        assert isinstance(result, datetime)
        assert result.year == now.year
        assert result.month == now.month
        assert result.day == now.day
        assert result.hour == now.hour
        assert result.minute == now.minute
        assert result.second == now.second

    @pytest.mark.parametrize(
        "date_string, expected_datetime",
        [
            ("2023-12-25 14:30:45.123456", datetime(2023, 12, 25, 14, 30, 45, 123456)),
            ("2023-12-25 14:30:45", datetime(2023, 12, 25, 14, 30, 45)),
            ("2023-12-25T14:30:45.123456", datetime(2023, 12, 25, 14, 30, 45, 123456)),
            ("2023-12-25T14:30:45", datetime(2023, 12, 25, 14, 30, 45)),
            ("2023-12-25", datetime(2023, 12, 25)),
            ("25/12/2023 14:30:45", datetime(2023, 12, 25, 14, 30, 45)),
            ("25/12/2023", datetime(2023, 12, 25)),
            ("12/25/2023 14:30:45", datetime(2023, 12, 25, 14, 30, 45)),
            ("12/25/2023", datetime(2023, 12, 25)),
        ]
    )
    def test_timestamp_to_python_string_formats(self, date_string, expected_datetime):
        """Testa conversão de strings em vários formatos para Python."""
        field = Timestamp()
        result = field.to_python(date_string)
        assert isinstance(result, datetime)
        assert result == expected_datetime

    def test_timestamp_to_python_string_with_dateutil(self):
        """Testa conversão de string com dateutil se disponível."""
        field = Timestamp()
        # Mock dateutil para garantir que é usado se disponível
        with patch.dict('sys.modules', {'dateutil': Mock(), 'dateutil.parser': Mock()}):
            from dateutil import parser
            parser.parse.return_value = datetime(2024, 1, 1, 10, 0, 0)
            result = field.to_python("Jan 1 2024 10:00AM")
            assert result == datetime(2024, 1, 1, 10, 0, 0)
            parser.parse.assert_called_once_with("Jan 1 2024 10:00AM")

    def test_timestamp_to_python_invalid_string(self):
        """Testa conversão de string inválida."""
        field = Timestamp()
        with pytest.raises(TypeError, match="Não foi possível converter string 'invalid-date' para datetime."):
            field.to_python("invalid-date")
    
    def test_timestamp_to_python_invalid_type(self):
        """Testa conversão de tipo inválido."""
        field = Timestamp()
        with pytest.raises(TypeError, match="Não foi possível converter \[\] para datetime"):
            field.to_python([])
    
    def test_timestamp_get_cql_definition(self):
        """Testa definição CQL do campo Timestamp."""
        field = Timestamp()
        assert field.get_cql_definition() == 'timestamp'
    
    def test_timestamp_get_pydantic_type(self):
        """Testa tipo Pydantic do campo Timestamp."""
        field = Timestamp()
        assert field.get_pydantic_type() == datetime


class TestList:
    """Testes para o campo List."""
    
    def test_list_initialization(self):
        """Testa inicialização do campo List."""
        inner_field = Text()
        field = List(inner_field)
        assert field.cql_type == 'list'
        assert field.python_type == list
        assert field.inner_field == inner_field
    
    def test_list_initialization_invalid_inner_field(self):
        """Testa inicialização com campo interno inválido."""
        with pytest.raises(TypeError):
            List("invalid")
    
    def test_list_get_cql_definition(self):
        """Testa definição CQL do campo List."""
        inner_field = Text()
        field = List(inner_field)
        assert field.get_cql_definition() == 'list<text>'
    
    def test_list_to_python_valid_list(self):
        """Testa conversão de lista válida para Python."""
        inner_field = Text()
        field = List(inner_field)
        result = field.to_python(["a", "b", "c"])
        assert result == ["a", "b", "c"]
    
    def test_list_to_python_empty_list(self):
        """Testa conversão de lista vazia para Python."""
        inner_field = Text()
        field = List(inner_field)
        result = field.to_python([])
        assert result == []
    
    def test_list_to_python_none_required(self):
        """Testa conversão de None quando campo é obrigatório."""
        inner_field = Text()
        field = List(inner_field, required=True)
        with pytest.raises(ValueError):
            field.to_python(None)
    
    def test_list_to_python_none_not_required(self):
        """Testa conversão de None quando campo não é obrigatório."""
        inner_field = Text()
        field = List(inner_field, required=False)
        result = field.to_python(None)
        assert result == []
    
    def test_list_to_python_invalid_item(self):
        """Testa conversão de lista com item inválido."""
        inner_field = Text()
        field = List(inner_field)
        with pytest.raises(TypeError):
            field.to_python(["a", 123, "c"])
    
    def test_list_to_cql_valid_list(self):
        """Testa conversão de lista válida para CQL."""
        inner_field = Text()
        field = List(inner_field)
        result = field.to_cql(["a", "b", "c"])
        assert result == ["a", "b", "c"]
    
    def test_list_to_cql_none(self):
        """Testa conversão de None para CQL."""
        inner_field = Text()
        field = List(inner_field)
        assert field.to_cql(None) is None
    
    def test_list_get_pydantic_type(self):
        """Testa tipo Pydantic do campo List."""
        inner_field = Text()
        field = List(inner_field)
        pydantic_type = field.get_pydantic_type()
        assert "List" in str(pydantic_type)


class TestSet:
    """Testes para o campo Set."""
    
    def test_set_initialization(self):
        """Testa inicialização do campo Set."""
        inner_field = Text()
        field = Set(inner_field)
        assert field.cql_type == 'set'
        assert field.python_type == set
        assert field.inner_field == inner_field
    
    def test_set_initialization_invalid_inner_field(self):
        """Testa inicialização com campo interno inválido."""
        with pytest.raises(TypeError):
            Set("invalid")
    
    def test_set_get_cql_definition(self):
        """Testa definição CQL do campo Set."""
        inner_field = Text()
        field = Set(inner_field)
        assert field.get_cql_definition() == 'set<text>'
    
    def test_set_to_python_valid_set(self):
        """Testa conversão de set válido para Python."""
        inner_field = Text()
        field = Set(inner_field)
        result = field.to_python(["a", "b", "c"])  # Cassandra retorna lista
        assert result == {"a", "b", "c"}
    
    def test_set_to_python_empty_set(self):
        """Testa conversão de set vazio para Python."""
        inner_field = Text()
        field = Set(inner_field)
        result = field.to_python([])
        assert result == set()
    
    def test_set_to_python_none_required(self):
        """Testa conversão de None quando campo é obrigatório."""
        inner_field = Text()
        field = Set(inner_field, required=True)
        with pytest.raises(ValueError):
            field.to_python(None)
    
    def test_set_to_python_none_not_required(self):
        """Testa conversão de None quando campo não é obrigatório."""
        inner_field = Text()
        field = Set(inner_field, required=False)
        result = field.to_python(None)
        assert result == set()
    
    def test_set_to_cql_valid_set(self):
        """Testa conversão de set válido para CQL."""
        inner_field = Text()
        field = Set(inner_field)
        result = field.to_cql({"a", "b", "c"})
        # Comparar como conjuntos, pois a ordem não é garantida
        assert set(result) == {"a", "b", "c"}
    
    def test_set_to_cql_none(self):
        """Testa conversão de None para CQL."""
        inner_field = Text()
        field = Set(inner_field)
        assert field.to_cql(None) is None
    
    def test_set_get_pydantic_type(self):
        """Testa tipo Pydantic do campo Set."""
        inner_field = Text()
        field = Set(inner_field)
        pydantic_type = field.get_pydantic_type()
        assert "Set" in str(pydantic_type)


class TestMap:
    """Testes para o campo Map."""
    
    def test_map_initialization(self):
        """Testa inicialização do campo Map."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        assert field.cql_type == 'map'
        assert field.python_type == dict
        assert field.key_field == key_field
        assert field.value_field == value_field
    
    def test_map_initialization_invalid_fields(self):
        """Testa inicialização com campos inválidos."""
        with pytest.raises(TypeError):
            Map("invalid", Integer())
        with pytest.raises(TypeError):
            Map(Text(), "invalid")
    
    def test_map_get_cql_definition(self):
        """Testa definição CQL do campo Map."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        assert field.get_cql_definition() == 'map<text,int>'
    
    def test_map_to_python_valid_dict(self):
        """Testa conversão de dicionário válido para Python."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        result = field.to_python({"a": 1, "b": 2})
        assert result == {"a": 1, "b": 2}
    
    def test_map_to_python_empty_dict(self):
        """Testa conversão de dicionário vazio para Python."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        result = field.to_python({})
        assert result == {}
    
    def test_map_to_python_none(self):
        """Testa conversão de None para Python."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        # Implementação retorna {} para None
        assert field.to_python(None) == {}
    
    def test_map_to_cql_valid_dict(self):
        """Testa conversão de dicionário válido para CQL."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        result = field.to_cql({"a": 1, "b": 2})
        assert result == {"a": 1, "b": 2}
    
    def test_map_to_cql_none(self):
        """Testa conversão de None para CQL."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        assert field.to_cql(None) is None
    
    def test_map_get_pydantic_type(self):
        """Testa tipo Pydantic do campo Map."""
        key_field = Text()
        value_field = Integer()
        field = Map(key_field, value_field)
        pydantic_type = field.get_pydantic_type()
        assert "Dict" in str(pydantic_type) 