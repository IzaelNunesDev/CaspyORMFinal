# caspyorm/_internal/serialization.py
import json
import logging
import uuid
from datetime import datetime

# Importação do Model para tipagem
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

if TYPE_CHECKING:
    from ..core.model import Model

logger = logging.getLogger(__name__)

# Lidar com a importação opcional do Pydantic
PYDANTIC_V2 = False
BaseModel = None
create_model = None
Field = None
ConfigDict = None
FieldInfo = None

try:
    import pydantic
    from pydantic import BaseModel, Field, create_model

    # Detectar versão do Pydantic
    PYDANTIC_V2 = pydantic.VERSION.startswith("2")

    if PYDANTIC_V2:
        # Pydantic v2 imports
        from pydantic import ConfigDict
        from pydantic.fields import FieldInfo
    else:
        # Pydantic v1 imports
        ConfigDict = None
        FieldInfo = None

except ImportError:
    pass


class CaspyJSONEncoder(json.JSONEncoder):
    """Encoder JSON customizado para tipos da CaspyORM."""

    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Adicione outros tipos aqui se necessário
        return super().default(obj)


def model_to_dict(instance: "Model", by_alias: bool = False) -> Dict[str, Any]:
    """Serializa uma instância de modelo para um dicionário."""
    # `by_alias` será usado no futuro
    data = {}
    for key in instance.model_fields.keys():
        data[key] = getattr(instance, key, None)
    return data


def model_to_json(
    instance: "Model", by_alias: bool = False, indent: Optional[int] = None
) -> str:
    """Serializa uma instância de modelo para uma string JSON."""
    return json.dumps(
        instance.model_dump(by_alias=by_alias), cls=CaspyJSONEncoder, indent=indent
    )


def generate_pydantic_model(
    model_cls: Type["Model"],
    name: Optional[str] = None,
    exclude: Optional[List[str]] = None,
) -> Type:
    """
    Gera dinamicamente um modelo Pydantic a partir de um modelo CaspyORM.
    Suporta tanto Pydantic v1 quanto v2.
    """
    # Verificar se Pydantic está disponível
    try:
        import pydantic
    except ImportError:
        raise ImportError(
            "A funcionalidade de integração com Pydantic requer que o pacote 'pydantic' seja instalado."
        )

    exclude = exclude or []

    pydantic_fields: Dict[str, Any] = {}
    caspy_fields = model_cls.model_fields  # Usar a propriedade da classe

    for field_name, field_obj in caspy_fields.items():
        if field_name in exclude:
            continue

        # Usa o método get_pydantic_type() que implementamos nos fields
        try:
            python_type = field_obj.get_pydantic_type()
        except (ImportError, TypeError) as e:
            logger.warning(
                f"Não foi possível obter o tipo Pydantic para o campo '{field_name}'. Erro: {e}"
            )
            continue

        # Configurar campo baseado na versão do Pydantic
        if PYDANTIC_V2 and Field is not None:
            # Pydantic v2: usar Field() para configurações
            if field_obj.required:
                pydantic_fields[field_name] = (python_type, Field())
            elif field_obj.default is not None:
                pydantic_fields[field_name] = (
                    python_type,
                    Field(default=field_obj.default),
                )
            else:
                # Campo opcional sem default
                from typing import Optional as OptionalType

                pydantic_fields[field_name] = (
                    OptionalType[python_type],
                    Field(default=None),
                )
        else:
            # Pydantic v1: usar sintaxe antiga
            if field_obj.required:
                pydantic_fields[field_name] = (python_type, ...)
            elif field_obj.default is not None:
                pydantic_fields[field_name] = (python_type, field_obj.default)
            else:
                # Campo opcional sem default
                from typing import Optional as OptionalType

                pydantic_fields[field_name] = (OptionalType[python_type], None)

    model_name = name or f"{model_cls.__name__}Pydantic"

    # Criar modelo com configurações específicas da versão
    if create_model is None:
        raise RuntimeError("Pydantic não está disponível")

    if PYDANTIC_V2:
        # Pydantic v2: usar ConfigDict
        pydantic_model = create_model(model_name, __base__=BaseModel, **pydantic_fields)
    else:
        # Pydantic v1: sintaxe padrão
        pydantic_model = create_model(model_name, **pydantic_fields)

    if pydantic_model is None:
        raise RuntimeError("Falha ao criar modelo Pydantic")

    return pydantic_model
