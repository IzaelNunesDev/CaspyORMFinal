# caspyorm/fields.py # caspyorm/fields.py

import uuid
from datetime import datetime
from typing import Any, Type


class BaseField:
    """Classe base para todos os tipos de campo da CaspyORM."""

    cql_type: str = ""
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
            partition_key = (
                True  # Uma chave primária simples é sempre uma chave de partição
            )

        self.primary_key = primary_key
        self.partition_key = partition_key
        self.clustering_key = clustering_key
        self.index = index
        self.required = required
        self.default = default

        if default is not None and required:
            raise ValueError(
                "Um campo não pode ser 'required' e ter um 'default' ao mesmo tempo."
            )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def get_cql_definition(self) -> str:
        """Retorna a definição de tipo CQL para este campo."""
        return self.cql_type

    def to_python(self, value: Any) -> Any:
        """Converte um valor vindo do Cassandra para um tipo Python."""
        if value is None:
            return None
        if isinstance(value, self.python_type):
            return value
        try:
            return self.python_type(value)
        except (TypeError, ValueError) as e:
            raise TypeError(
                f"Não foi possível converter {value!r} para {self.python_type.__name__}"
            ) from e

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
    cql_type = "text"
    python_type = str

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if not isinstance(value, str):
            raise TypeError(f"Não foi possível converter {value!r} para str")
        return value


class UUID(BaseField):
    cql_type = "uuid"
    python_type = uuid.UUID

    def __init__(self, **kwargs):
        # Se for chave primária e não tiver default, gerar UUID automaticamente
        if kwargs.get("primary_key", False) and "default" not in kwargs:
            kwargs["default"] = lambda: uuid.uuid4()
        super().__init__(**kwargs)


class Integer(BaseField):
    cql_type = "int"
    python_type = int


class Float(BaseField):
    cql_type = "float"
    python_type = float


class Boolean(BaseField):
    cql_type = "boolean"
    python_type = bool

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            if value.lower() in ("true", "1", "yes", "on"):
                return True
            elif value.lower() in ("false", "0", "no", "off"):
                return False
            else:
                raise TypeError(
                    f"Não foi possível converter string '{value}' para boolean"
                )
        if isinstance(value, int):
            return bool(value)
        raise TypeError(f"Não foi possível converter {value!r} para boolean")


class Timestamp(BaseField):
    cql_type = "timestamp"
    python_type = datetime  # Usamos datetime para timestamp

    def __init__(self, **kwargs):
        # Formatos de data comuns para tentar converter strings
        self.date_formats = [
            "%Y-%m-%d %H:%M:%S.%f",  # 2023-12-25 14:30:45.123456
            "%Y-%m-%d %H:%M:%S",  # 2023-12-25 14:30:45
            "%Y-%m-%dT%H:%M:%S.%f",  # 2023-12-25T14:30:45.123456
            "%Y-%m-%dT%H:%M:%S",  # 2023-12-25T14:30:45
            "%Y-%m-%d",  # 2023-12-25
            "%d/%m/%Y %H:%M:%S",  # 25/12/2023 14:30:45
            "%d/%m/%Y",  # 25/12/2023
            "%m/%d/%Y %H:%M:%S",  # 12/25/2023 14:30:45
            "%m/%d/%Y",  # 12/25/2023
        ]
        super().__init__(**kwargs)

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            # Assumir que é timestamp Unix (segundos desde epoch)
            # Se for maior que 1e12, provavelmente é em milissegundos
            return datetime.fromtimestamp(value / 1000 if value > 1e12 else value)
        if isinstance(value, str):
            # Tentar ISO format primeiro (mais comum)
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass

            # Tentar formatos conhecidos
            for fmt in self.date_formats:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue

            # Tentar dateutil se disponível
            try:
                from dateutil import parser

                return parser.parse(value)
            except ImportError:
                pass
            except ValueError:
                pass

            raise TypeError(
                f"Não foi possível converter string '{value}' para datetime. "
                f"Formatos suportados: ISO format, {', '.join(self.date_formats)}"
            )
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

    cql_type = "list"
    python_type = list
    collection_type = list

    def __init__(self, inner_field: BaseField, **kwargs):
        if not isinstance(inner_field, BaseField):
            raise TypeError(
                "O campo interno de uma Lista deve ser uma instância de BaseField (ex: fields.Text())."
            )

        self.inner_field = inner_field
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        """Retorna a definição CQL completa, e.g., 'list<text>'."""
        return f"{self.cql_type}<{self.inner_field.get_cql_definition()}>"

    def to_python(self, value: Any) -> Any:
        """Converte uma lista do Cassandra para uma lista de tipos Python."""
        if value is None:
            if self.required:
                raise ValueError("Campo é obrigatório mas recebeu None")
            return []  # Retorna lista vazia por conveniência
        result = []
        for item in value:
            try:
                result.append(self.inner_field.to_python(item))
            except TypeError as e:
                raise TypeError(
                    f"Não foi possível converter item '{item}' da lista para o tipo {self.inner_field.python_type.__name__}: {e}"
                )
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
                raise TypeError(
                    f"Não foi possível converter item '{item}' da lista para o tipo {self.inner_field.python_type.__name__}: {e}"
                )
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

    cql_type = "set"
    python_type = set
    collection_type = set

    def __init__(self, inner_field: BaseField, **kwargs):
        if not isinstance(inner_field, BaseField):
            raise TypeError(
                "O campo interno de um Set deve ser uma instância de BaseField (ex: fields.Text())."
            )
        self.inner_field = inner_field
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        return f"{self.cql_type}<{self.inner_field.get_cql_definition()}>"

    def to_python(self, value: Any) -> Any:
        if value is None:
            if self.required:
                raise ValueError("Campo é obrigatório mas recebeu None")
            return set()
        result = set()
        for item in value:
            try:
                result.add(self.inner_field.to_python(item))
            except TypeError as e:
                raise TypeError(
                    f"Não foi possível converter item '{item}' do set para o tipo {self.inner_field.python_type.__name__}: {e}"
                )
        return result

    def to_cql(self, value: Any) -> Any:
        if value is None:
            return None
        result = []
        for item in value:
            try:
                result.append(self.inner_field.to_cql(item))
            except TypeError as e:
                raise TypeError(
                    f"Não foi possível converter item '{item}' do set para o tipo {self.inner_field.python_type.__name__}: {e}"
                )
        return result

    def get_pydantic_type(self) -> Type[Any]:
        from typing import Set as SetType

        return SetType[self.inner_field.get_pydantic_type()]


class Map(BaseField):
    """
    Representa um campo de mapa (dicionário) no Cassandra.
    Uso: fields.Map(fields.Text(), fields.Integer())
    """

    cql_type = "map"
    python_type = dict
    collection_type = dict

    def __init__(self, key_field: BaseField, value_field: BaseField, **kwargs):
        if not isinstance(key_field, BaseField) or not isinstance(
            value_field, BaseField
        ):
            raise TypeError(
                "Os campos de chave e valor de um Map devem ser instâncias de BaseField."
            )
        self.key_field = key_field
        self.value_field = value_field
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        return f"{self.cql_type}<{self.key_field.get_cql_definition()},{self.value_field.get_cql_definition()}>"

    def to_python(self, value: Any) -> Any:
        if value is None:
            if self.required:
                raise ValueError("Campo é obrigatório mas recebeu None")
            return {}
        result = {}
        for k, v in value.items():
            try:
                key = self.key_field.to_python(k)
            except TypeError as e:
                raise TypeError(
                    f"Não foi possível converter chave '{k}' do map para o tipo {self.key_field.python_type.__name__}: {e}"
                )
            try:
                val = self.value_field.to_python(v)
            except TypeError as e:
                raise TypeError(
                    f"Não foi possível converter valor '{v}' do map para o tipo {self.value_field.python_type.__name__}: {e}"
                )
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
                raise TypeError(
                    f"Não foi possível converter chave '{k}' do map para o tipo {self.key_field.python_type.__name__}: {e}"
                )
            try:
                val = self.value_field.to_cql(v)
            except TypeError as e:
                raise TypeError(
                    f"Não foi possível converter valor '{v}' do map para o tipo {self.value_field.python_type.__name__}: {e}"
                )
            result[key] = val
        return result

    def get_pydantic_type(self) -> Type[Any]:
        from typing import Dict as DictType

        return DictType[
            self.key_field.get_pydantic_type(), self.value_field.get_pydantic_type()
        ]


class Tuple(BaseField):
    """
    Representa um campo de tupla no Cassandra.
    Uso: fields.Tuple(fields.Text(), fields.Integer(), fields.Boolean())
    """

    cql_type = "tuple"
    python_type = tuple

    def __init__(self, *field_types: BaseField, **kwargs):
        if not field_types:
            raise TypeError("Tuple deve ter pelo menos um tipo de campo.")

        for field_type in field_types:
            if not isinstance(field_type, BaseField):
                raise TypeError(
                    "Todos os tipos de campo em Tuple devem ser instâncias de BaseField."
                )

        self.field_types = field_types
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        """Retorna a definição CQL completa, e.g., 'tuple<text, int, boolean>'."""
        inner_types = ", ".join(
            field_type.get_cql_definition() for field_type in self.field_types
        )
        return f"tuple<{inner_types}>"

    def to_python(self, value: Any) -> Any:
        """Converte um valor vindo do Cassandra para uma tupla Python."""
        if value is None:
            return None
        if isinstance(value, tuple):
            # Converter cada elemento usando o tipo correspondente
            if len(value) != len(self.field_types):
                raise ValueError(
                    f"Tuple esperava {len(self.field_types)} elementos, mas recebeu {len(value)}"
                )

            converted_values = []
            for i, (val, field_type) in enumerate(zip(value, self.field_types)):
                try:
                    converted_values.append(field_type.to_python(val))
                except Exception as e:
                    raise ValueError(
                        f"Erro ao converter elemento {i} da tupla: {e}"
                    ) from e

            return tuple(converted_values)
        raise TypeError(f"Não foi possível converter {value!r} para tuple")

    def to_cql(self, value: Any) -> Any:
        """Converte uma tupla Python para formato serializável pelo driver do Cassandra."""
        if value is None:
            return None
        if isinstance(value, tuple):
            if len(value) != len(self.field_types):
                raise ValueError(
                    f"Tuple esperava {len(self.field_types)} elementos, mas recebeu {len(value)}"
                )

            converted_values = []
            for val, field_type in zip(value, self.field_types):
                converted_values.append(field_type.to_cql(val))

            return tuple(converted_values)
        raise TypeError(f"Não foi possível converter {value!r} para tuple")

    def get_pydantic_type(self) -> Type[Any]:
        """Retorna o tipo Pydantic equivalente para este campo."""
        from typing import Tuple as TypingTuple

        inner_types = [
            field_type.get_pydantic_type() for field_type in self.field_types
        ]
        return TypingTuple[tuple(inner_types)]


class UserDefinedType(BaseField):
    """
    Representa um campo User-Defined Type (UDT) no Cassandra.
    Uso: fields.UserDefinedType(Address)
    """

    cql_type = "frozen"
    python_type = object  # Será substituído pelo tipo específico

    def __init__(self, udt_class: Type, **kwargs):
        from ..types.usertype import UserType

        if not isinstance(udt_class, type) or not issubclass(udt_class, UserType):
            raise TypeError(
                "UserDefinedType deve receber uma classe que herda de UserType."
            )

        self.udt_class = udt_class
        self.python_type = udt_class  # Usar a classe UDT como tipo Python
        super().__init__(**kwargs)

    def get_cql_definition(self) -> str:
        """Retorna a definição CQL completa, e.g., 'frozen<address>'."""
        type_name = getattr(
            self.udt_class, "__type_name__", self.udt_class.__name__.lower()
        )
        return f"frozen<{type_name}>"

    def to_python(self, value: Any) -> Any:
        """Converte um valor vindo do Cassandra para uma instância do UDT."""
        if value is None:
            return None
        if isinstance(value, self.udt_class):
            return value
        if isinstance(value, dict):
            return self.udt_class(**value)
        # Aceita namedtuple (tem _fields)
        if hasattr(value, "_fields"):
            return self.udt_class(**{f: getattr(value, f) for f in value._fields})
        # Aceita objetos com __dict__
        if hasattr(value, "__dict__"):
            return self.udt_class(**vars(value))
        raise TypeError(
            f"Não foi possível converter {value!r} para {self.udt_class.__name__}"
        )

    def to_cql(self, value: Any) -> Any:
        """Converte uma instância do UDT para formato serializável pelo driver do Cassandra."""
        if value is None:
            return None
        if isinstance(value, self.udt_class):
            return value.model_dump()
        if isinstance(value, dict):
            return value
        raise TypeError(
            f"Não foi possível converter {value!r} para {self.udt_class.__name__}"
        )

    def get_pydantic_type(self) -> Type[Any]:
        """Retorna o tipo Pydantic equivalente para este campo."""
        return self.udt_class


# Adicione mais tipos conforme necessário, como Set, Map, etc.
