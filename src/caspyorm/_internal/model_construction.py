# caspyorm/_internal/model_construction.py # caspyorm/_internal/model_construction.py

from typing import Any, Dict

from ..core.fields import BaseField


class ModelMetaclass(type):
    """
    Metaclasse que transforma a declaração de uma classe em um modelo CaspyORM funcional.
    """

    def __new__(mcs, name: str, bases: tuple, attrs: Dict[str, Any]):
        # Não processa a classe Model base ou UserType base
        if name in ["Model", "UserType"]:
            return super().__new__(mcs, name, bases, attrs)

        # Extrai os campos declarados na classe
        model_fields: Dict[str, BaseField] = {}

        # Se model_fields já foi definido (criação dinâmica), use-o
        if "model_fields" in attrs:
            model_fields = attrs["model_fields"]
        else:
            # Processamento normal para definição estática
            for key, value in attrs.items():
                if isinstance(value, BaseField):
                    model_fields[key] = value

            # Remove os campos dos atributos da classe para que não sejam atributos de classe
            for key in model_fields:
                del attrs[key]

            # Processa as anotações de tipo (e.g., nome: fields.Text())
            annotations = attrs.get(
                "__annotations__", {}
            ).copy()  # Copia para evitar modificar durante a iteração
            for key, field_type in annotations.items():
                if key not in model_fields and isinstance(field_type, BaseField):
                    model_fields[key] = field_type
                    # Remove a anotação para limpar o namespace da classe final
                    if "__annotations__" in attrs and key in attrs["__annotations__"]:
                        del attrs["__annotations__"][key]

        if not model_fields:
            raise TypeError(f'O modelo "{name}" não definiu nenhum campo.')

        # Verifica se é um UDT (herda de UserType)
        is_udt = any(base.__name__ == "UserType" for base in bases)

        if is_udt:
            # Para UDTs, não precisa de schema de tabela
            schema = {
                "type_name": attrs.get("__type_name__", name.lower()),
                "fields": model_fields,
                "is_udt": True,
            }
            attrs["__caspy_schema__"] = schema
        else:
            # Define o nome da tabela (pode ser sobrescrito com __table_name__)
            table_name = attrs.get("__table_name__", name.lower() + "s")
            attrs["__table_name__"] = table_name

            # Constrói o schema interno
            schema = mcs.build_schema(table_name, model_fields)
            attrs["__caspy_schema__"] = schema

        # Armazena os campos para fácil acesso
        attrs["model_fields"] = model_fields

        # Cria a classe final
        new_class = super().__new__(mcs, name, bases, attrs)
        return new_class

    @staticmethod
    def build_schema(table_name: str, fields: Dict[str, BaseField]) -> Dict[str, Any]:
        """
        Cria a representação interna do schema do modelo.
        Esta estrutura será usada para gerar CQL, validar, etc.
        """
        schema = {
            "table_name": table_name,
            "fields": {},
            "primary_keys": [],
            "partition_keys": [],
            "clustering_keys": [],
            "indexes": [],
        }

        for name, field in fields.items():
            schema["fields"][name] = {
                "type": field.get_cql_definition(),
                "required": field.required,
                "default": field.default,
            }
            if field.partition_key:
                schema["partition_keys"].append(name)
            if field.clustering_key:
                schema["clustering_keys"].append(name)
            if field.index:
                schema["indexes"].append(name)

        # Define as chaves primárias
        schema["primary_keys"] = schema["partition_keys"] + schema["clustering_keys"]
        if not schema["primary_keys"]:
            raise ValueError(
                "O modelo deve ter pelo menos uma 'partition_key' ou 'primary_key'."
            )

        return schema
