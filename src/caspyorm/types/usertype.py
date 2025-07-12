# caspyorm/usertype.py

import logging
from typing import Any, ClassVar, Dict, Type

from .._internal.model_construction import ModelMetaclass
from ..core.fields import BaseField
from ..utils.exceptions import ValidationError

logger = logging.getLogger(__name__)


class UserType(metaclass=ModelMetaclass):
    """
    Classe base para User-Defined Types (UDT) do Cassandra.

    Uso:
        class Address(UserType):
            street: Text = Text()
            city: Text = Text()
            zip_code: Text = Text()
    """

    # --- Atributos que a metaclasse irá preencher ---
    __type_name__: ClassVar[str]
    __caspy_schema__: ClassVar[Dict[str, Any]]
    model_fields: ClassVar[Dict[str, Any]]

    def __init__(self, **kwargs: Any):
        self.__dict__["_data"] = {}
        for key, field_obj in self.model_fields.items():
            # Obter valor dos kwargs ou None
            value = kwargs.get(key)

            # Aplicar default se valor for None
            if value is None and field_obj.default is not None:
                value = (
                    field_obj.default()
                    if callable(field_obj.default)
                    else field_obj.default
                )

            # Inicializar coleções vazias se valor ainda for None
            if value is None and hasattr(field_obj, "python_type"):
                value = self._initialize_empty_collection(field_obj.python_type)

            # Validar campo required após inicialização
            if value is None and field_obj.required:
                raise ValidationError(
                    f"Campo '{key}' é obrigatório e não foi fornecido."
                )

            # Converter valor usando to_python se necessário
            if value is not None:
                try:
                    value = field_obj.to_python(value)
                except (TypeError, ValueError) as e:
                    raise ValidationError(f"Valor inválido para campo '{key}': {e}")

            self.__dict__[key] = value

    def _initialize_empty_collection(self, python_type: type) -> Any:
        """Inicializa uma coleção vazia baseada no tipo Python."""
        if python_type is list:
            return []
        elif python_type is set:
            return set()
        elif python_type is dict:
            return {}
        return None

    def __setattr__(self, key: str, value: Any):
        if key in self.model_fields:
            self.__dict__[key] = value
        else:
            super().__setattr__(key, value)

    def model_dump(self, by_alias: bool = False) -> Dict[str, Any]:
        """Converte a instância para um dicionário."""
        result = {}
        for field_name, field_obj in self.model_fields.items():
            value = getattr(self, field_name)
            if value is not None:
                result[field_name] = value
        return result

    def __repr__(self) -> str:
        attrs = ", ".join(f"{k}={getattr(self, k)!r}" for k in self.model_fields)
        return f"{self.__class__.__name__}({attrs})"

    @classmethod
    def create_udt(cls, name: str, fields: Dict[str, Any]) -> Type:
        """
        Cria dinamicamente um novo UDT.

        Args:
            name: Nome do tipo UDT
            fields: Dicionário com nome do campo -> instância de BaseField

        Returns:
            Nova classe UDT dinamicamente criada
        """
        # Validar que todos os campos são instâncias de BaseField
        for field_name, field_obj in fields.items():
            if not isinstance(field_obj, BaseField):
                raise TypeError(
                    f"Campo '{field_name}' deve ser uma instância de BaseField, recebido: {type(field_obj)}"
                )

        # Criar atributos da classe
        attrs = {
            "__type_name__": name,
            "__caspy_schema__": None,  # Será preenchido pela metaclasse
            "model_fields": fields,
        }

        # Criar a classe usando a metaclasse ModelMetaclass
        new_udt_class = ModelMetaclass(name, (cls,), attrs)

        return new_udt_class
