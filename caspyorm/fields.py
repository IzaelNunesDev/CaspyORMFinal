# caspyorm/fields.py # caspyorm/fields.py

import uuid
from datetime import datetime
from typing import Any, Type

class BaseField:
    """Classe base para todos os tipos de campo da CaspyORM."""

    cql_type: str = ''
    python_type: Type[Any] = type(None)

    def __init__(
        self,
        *,
        primary_key: bool = False,
        partition_key: bool = False,
        clustering_key: bool = False,
        index: bool = False,
        required: bool = False,
        default: Any = None,
        # Em breve: alias para mapear nomes de colunas diferentes
    ):
        if primary_key:
            partition_key = True  # Uma chave primária simples é sempre uma chave de partição

        self.primary_key = primary_key
        self.partition_key = partition_key
        self.clustering_key = clustering_key
        self.index = index
        self.required = required
        self.default = default

        if default is not None and required:
            raise ValueError("Um campo não pode ser 'required' e ter um 'default' ao mesmo tempo.")

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}()'

    def get_cql_definition(self) -> str:
        """Retorna a definição de tipo CQL para este campo."""
        return self.cql_type

    def to_python(self, value: Any) -> Any:
        """Converte um valor vindo do Cassandra para um tipo Python."""
        if value is None:
            return None
        try:
            return self.python_type(value)
        except (TypeError, ValueError) as e:
            raise TypeError(f"Não foi possível converter {value!r} para {self.python_type.__name__}") from e

    def to_cql(self, value: Any) -> Any:
        """Converte um valor Python para um formato serializável pelo driver do Cassandra."""
        if value is None:
            return None
        # A validação de tipo já deve ter acontecido no `__setattr__` do Model
        return value

    def get_pydantic_type(self) -> Type[Any]:
        """Retorna o tipo Pydantic/Python equivalente para este campo."""
        return self.python_type


# --- Definições de Campos Concretos ---

class Text(BaseField):
    cql_type = 'text'
    python_type = str

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"Não foi possível converter {value!r} para str")
        return value

class UUID(BaseField):
    cql_type = 'uuid'
    python_type = uuid.UUID
    
    def __init__(self, **kwargs):
        # Se for chave primária e não tiver default, gerar UUID automaticamente
        if kwargs.get('primary_key', False) and 'default' not in kwargs:
            kwargs['default'] = lambda: uuid.uuid4()
        super().__init__(**kwargs)

class Integer(BaseField):
    cql_type = 'int'
    python_type = int

class Float(BaseField):
    cql_type = 'float'
    python_type = float

class Boolean(BaseField):
    cql_type = 'boolean'
    python_type = bool
    
    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ('true', '1', 'yes', 'on'):
                return True
            elif value.lower() in ('false', '0', 'no', 'off'):
                return False
            else:
                raise TypeError(f"Não foi possível converter string '{value}' para boolean")
        if isinstance(value, int):
            return bool(value)
        raise TypeError(f"Não foi possível converter {value!r} para boolean")

class Timestamp(BaseField):
    cql_type = 'timestamp'
    python_type = datetime  # Usamos datetime para timestamp

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000 if value > 1e12 else value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except Exception:
                pass
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
            except Exception:
                pass
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except Exception:
                pass
            raise TypeError(f"Não foi possível converter string '{value}' para datetime")
        raise TypeError(f"Não foi possível converter {value!r} para datetime")

    def to_cql(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000 if value > 1e12 else value)
        if isinstance(value, str):
            return self.to_python(value)
        raise TypeError(f"Não foi possível converter {value!r} para datetime")

class List(BaseField):
    """
    Representa um campo de lista no Cassandra.
    Uso: fields.List(fields.Text())
    """
    cql_type = 'list'
    python_type = list

    def __init__(self, inner_field: BaseField, **kwargs):
        if not isinstance(inner_field, BaseField):
            raise TypeError("O campo interno de uma Lista deve ser uma instância de BaseField (ex: fields.Text()).")
        
        self.inner_field = inner_field
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        """Retorna a definição CQL completa, e.g., 'list<text>'."""
        return f"{self.cql_type}<{self.inner_field.get_cql_definition()}>"

    def to_python(self, value: Any) -> Any:
        """Converte uma lista do Cassandra para uma lista de tipos Python."""
        if value is None:
            if self.required:
                raise ValueError(f"Campo é obrigatório mas recebeu None")
            return []  # Retorna lista vazia por conveniência
        result = []
        for item in value:
            try:
                result.append(self.inner_field.to_python(item))
            except TypeError as e:
                raise TypeError(f"Não foi possível converter item '{item}' da lista para o tipo {self.inner_field.python_type.__name__}: {e}")
        return result

    def to_cql(self, value: Any) -> Any:
        """Converte uma lista Python para um formato serializável pelo Cassandra."""
        if value is None:
            return None
        result = []
        for item in value:
            try:
                result.append(self.inner_field.to_cql(item))
            except TypeError as e:
                raise TypeError(f"Não foi possível converter item '{item}' da lista para o tipo {self.inner_field.python_type.__name__}: {e}")
        return result

    def get_pydantic_type(self) -> Type[Any]:
        """Retorna o tipo Pydantic/Python equivalente para este campo."""
        from typing import List as ListType
        return ListType[self.inner_field.get_pydantic_type()]

class Set(BaseField):
    """
    Representa um campo de conjunto no Cassandra.
    Uso: fields.Set(fields.Text())
    """
    cql_type = 'set'
    python_type = set

    def __init__(self, inner_field: BaseField, **kwargs):
        if not isinstance(inner_field, BaseField):
            raise TypeError("O campo interno de um Set deve ser uma instância de BaseField (ex: fields.Text()).")
        self.inner_field = inner_field
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        return f"{self.cql_type}<{self.inner_field.get_cql_definition()}>"

    def to_python(self, value: Any) -> Any:
        if value is None:
            if self.required:
                raise ValueError(f"Campo é obrigatório mas recebeu None")
            return set()
        result = set()
        for item in value:
            try:
                result.add(self.inner_field.to_python(item))
            except TypeError as e:
                raise TypeError(f"Não foi possível converter item '{item}' do set para o tipo {self.inner_field.python_type.__name__}: {e}")
        return result

    def to_cql(self, value: Any) -> Any:
        if value is None:
            return None
        result = []
        for item in value:
            try:
                result.append(self.inner_field.to_cql(item))
            except TypeError as e:
                raise TypeError(f"Não foi possível converter item '{item}' do set para o tipo {self.inner_field.python_type.__name__}: {e}")
        return result

    def get_pydantic_type(self) -> Type[Any]:
        from typing import Set as SetType
        return SetType[self.inner_field.get_pydantic_type()]

class Map(BaseField):
    """
    Representa um campo de mapa (dicionário) no Cassandra.
    Uso: fields.Map(fields.Text(), fields.Integer())
    """
    cql_type = 'map'
    python_type = dict

    def __init__(self, key_field: BaseField, value_field: BaseField, **kwargs):
        if not isinstance(key_field, BaseField) or not isinstance(value_field, BaseField):
            raise TypeError("Os campos de chave e valor de um Map devem ser instâncias de BaseField.")
        self.key_field = key_field
        self.value_field = value_field
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        return f"{self.cql_type}<{self.key_field.get_cql_definition()},{self.value_field.get_cql_definition()}>"

    def to_python(self, value: Any) -> Any:
        if value is None:
            if self.required:
                raise ValueError(f"Campo é obrigatório mas recebeu None")
            return {}
        result = {}
        for k, v in value.items():
            try:
                key = self.key_field.to_python(k)
            except TypeError as e:
                raise TypeError(f"Não foi possível converter chave '{k}' do map para o tipo {self.key_field.python_type.__name__}: {e}")
            try:
                val = self.value_field.to_python(v)
            except TypeError as e:
                raise TypeError(f"Não foi possível converter valor '{v}' do map para o tipo {self.value_field.python_type.__name__}: {e}")
            result[key] = val
        return result

    def to_cql(self, value: Any) -> Any:
        if value is None:
            return None
        result = {}
        for k, v in value.items():
            try:
                key = self.key_field.to_cql(k)
            except TypeError as e:
                raise TypeError(f"Não foi possível converter chave '{k}' do map para o tipo {self.key_field.python_type.__name__}: {e}")
            try:
                val = self.value_field.to_cql(v)
            except TypeError as e:
                raise TypeError(f"Não foi possível converter valor '{v}' do map para o tipo {self.value_field.python_type.__name__}: {e}")
            result[key] = val
        return result

    def get_pydantic_type(self) -> Type[Any]:
        from typing import Dict as DictType
        return DictType[self.key_field.get_pydantic_type(), self.value_field.get_pydantic_type()]

# Adicione mais tipos conforme necessário, como Set, Map, etc.