# caspyorm/model.py (REVISADO)

import asyncio
from typing import Any, ClassVar, Dict, Optional, List, Type
from typing_extensions import Self
import json
import logging

from ._internal.model_construction import ModelMetaclass
from ._internal.schema_sync import sync_table
from ._internal.serialization import generate_pydantic_model, model_to_dict, model_to_json
from .query import QuerySet, get_one, filter_query, save_instance
from caspyorm.exceptions import ValidationError

logger = logging.getLogger(__name__)

class Model(metaclass=ModelMetaclass):
    # ... (o resto da classe permanece igual, mas agora os imports apontam para a lógica real)
    # --- Atributos que a metaclasse irá preencher ---
    __table_name__: ClassVar[str]
    __caspy_schema__: ClassVar[Dict[str, Any]]
    model_fields: ClassVar[Dict[str, Any]]

    # --- Métodos de API Pública ---
    def __init__(self, **kwargs: Any):
        self.__dict__["_data"] = {}
        for key, field_obj in self.model_fields.items():
            # Obter valor dos kwargs ou None
            value = kwargs.get(key)
            
            # Aplicar default se valor for None
            if value is None and field_obj.default is not None:
                value = field_obj.default() if callable(field_obj.default) else field_obj.default
            
            # Inicializar coleções vazias se valor ainda for None
            if value is None and hasattr(field_obj, 'python_type'):
                value = self._initialize_empty_collection(field_obj.python_type)
            
            # Validar campo required após inicialização
            if value is None and field_obj.required:
                raise ValidationError(f"Campo '{key}' é obrigatório e não foi fornecido.")
            
            # Converter valor usando to_python se necessário
            if value is not None:
                try:
                    value = field_obj.to_python(value)
                except (TypeError, ValueError) as e:
                    raise ValidationError(f"Valor inválido para campo '{key}': {e}")
            
            self.__dict__[key] = value
    
    def _initialize_empty_collection(self, python_type: type) -> Any:
        """
        Inicializa uma coleção vazia baseada no tipo Python.
        
        Args:
            python_type: Tipo da coleção (list, set, dict)
            
        Returns:
            Coleção vazia do tipo especificado ou None se não for uma coleção
        """
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
        return model_to_dict(self, by_alias=by_alias)

    def model_dump_json(self, by_alias: bool = False, indent: Optional[int] = None) -> str:
        return model_to_json(self, by_alias=by_alias, indent=indent)

    def save(self) -> Self:
        # VALIDAÇÃO ADICIONADA: Garante que as chaves primárias não são nulas ao salvar.
        for pk_name in self.__caspy_schema__['primary_keys']:
            if getattr(self, pk_name, None) is None:
                raise ValidationError(f"Primary key '{pk_name}' cannot be None before saving.")
        save_instance(self)
        return self

    async def save_async(self) -> Self:
        """Salva (insere ou atualiza) a instância no Cassandra (assíncrono)."""
        # VALIDAÇÃO ADICIONADA: Garante que as chaves primárias não são nulas ao salvar.
        for pk_name in self.__caspy_schema__['primary_keys']:
            if getattr(self, pk_name, None) is None:
                raise ValidationError(f"Primary key '{pk_name}' cannot be None before saving.")
        
        from .query import save_instance_async
        await save_instance_async(self)
        return self

    async def update(self, **kwargs: Any) -> Self:
        """
        Atualiza parcialmente esta instância no banco de dados.
        Diferente de save(), que faz um upsert completo, update() gera
        uma query UPDATE específica apenas para os campos fornecidos.
        """
        if not kwargs:
            logger.warning("update() chamado sem campos para atualizar")
            return self
        
        # Validar e converter os valores
        validated_data = {}
        for key, value in kwargs.items():
            if key not in self.model_fields:
                raise ValidationError(f"Campo '{key}' não existe no modelo {self.__class__.__name__}")
            
            field_obj = self.model_fields[key]
            if value is not None:
                try:
                    validated_value = field_obj.to_python(value)
                    validated_data[key] = validated_value
                    # Atualizar o atributo da instância
                    setattr(self, key, validated_value)
                except (TypeError, ValueError) as e:
                    raise ValidationError(f"Valor inválido para campo '{key}': {e}")
        
        if not validated_data:
            logger.warning("Nenhum campo válido fornecido para update()")
            return self
        
        # Gerar e executar query UPDATE
        from ._internal.query_builder import build_update_cql
        cql, params = build_update_cql(
            self.__caspy_schema__,
            update_data=validated_data,
            pk_filters={pk: getattr(self, pk) for pk in self.__caspy_schema__['primary_keys']}
        )
        
        try:
            from .connection import get_session
            session = get_session()
            prepared = await session.prepare_async(cql)
            await session.execute_async(prepared, params)
            logger.info(f"Instância atualizada: {self.__class__.__name__} com campos: {list(validated_data.keys())}")
        except Exception as e:
            logger.error(f"Erro ao atualizar instância: {e}")
            raise
        
        return self

    async def update_async(self, **kwargs: Any) -> Self:
        """
        Atualiza parcialmente esta instância no banco de dados (assíncrono).
        Diferente de save_async(), que faz um upsert completo, update_async() gera
        uma query UPDATE específica apenas para os campos fornecidos.
        """
        if not kwargs:
            logger.warning("update_async() chamado sem campos para atualizar")
            return self
        
        # Validar e converter os valores
        validated_data = {}
        for key, value in kwargs.items():
            if key not in self.model_fields:
                raise ValidationError(f"Campo '{key}' não existe no modelo {self.__class__.__name__}")
            
            field_obj = self.model_fields[key]
            if value is not None:
                try:
                    validated_value = field_obj.to_python(value)
                    validated_data[key] = validated_value
                    # Atualizar o atributo da instância
                    setattr(self, key, validated_value)
                except (TypeError, ValueError) as e:
                    raise ValidationError(f"Valor inválido para campo '{key}': {e}")
        
        if not validated_data:
            logger.warning("Nenhum campo válido fornecido para update_async()")
            return self
        
        # Gerar e executar query UPDATE
        from ._internal.query_builder import build_update_cql
        cql, params = build_update_cql(
            self.__caspy_schema__,
            update_data=validated_data,
            pk_filters={pk: getattr(self, pk) for pk in self.__caspy_schema__['primary_keys']}
        )
        
        try:
            from .connection import get_async_session
            session = get_async_session()
            prepared = await session.prepare_async(cql)
            future = session.execute_async(prepared, params)
            await future
            logger.info(f"Instância atualizada (ASSÍNCRONO): {self.__class__.__name__} com campos: {list(validated_data.keys())}")
        except Exception as e:
            logger.error(f"Erro ao atualizar instância (async): {e}")
            raise
        
        return self

    @classmethod
    def create(cls, **kwargs: Any) -> Self:
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    async def create_async(cls, **kwargs: Any) -> Self:
        """Cria uma nova instância e a salva no banco de dados (assíncrono)."""
        instance = cls(**kwargs)
        await instance.save_async()
        return instance

    @classmethod
    def bulk_create(cls, instances: List["Model"]) -> List["Model"]:
        """
        Insere uma lista de instâncias de modelo em lote usando um UNLOGGED BATCH
        para máxima performance. As instâncias são modificadas no local.
        Nota: Validações de Primary Key devem ser feitas antes de chamar este método.
        """
        if not instances:
            return []
        
        # Delega a lógica para um método do QuerySet
        return QuerySet(cls).bulk_create(instances)

    @classmethod
    async def bulk_create_async(cls, instances: List["Model"]) -> List["Model"]:
        """
        Insere uma lista de instâncias de modelo em lote usando um UNLOGGED BATCH
        para máxima performance (assíncrono). As instâncias são modificadas no local.
        Nota: Validações de Primary Key devem ser feitas antes de chamar este método.
        """
        if not instances:
            return []
        
        # Implementar lógica assíncrona real
        from .connection import get_async_session
        from ._internal.query_builder import build_insert_cql
        
        session = get_async_session()
        
        # Criar batch de queries
        batch_queries = []
        for instance in instances:
            cql = build_insert_cql(instance.__caspy_schema__)
            params = list(instance.model_dump().values())
            batch_queries.append((cql, params))
        
        # Executar batch assíncrono
        try:
            for cql, params in batch_queries:
                prepared = await session.prepare_async(cql)
                future = session.execute_async(prepared, params)
                await future
            
            logger.info(f"Bulk create assíncrono concluído: {len(instances)} instâncias")
            return instances
        except Exception as e:
            logger.error(f"Erro no bulk create assíncrono: {e}")
            raise

    @classmethod
    def get(cls, **kwargs: Any) -> Optional["Model"]:
        """Busca um único registro."""
        # A lógica foi movida para query.py, que usa o QuerySet
        return get_one(cls, **kwargs)

    @classmethod
    async def get_async(cls, **kwargs: Any) -> Optional["Model"]:
        """Busca um único registro (assíncrono)."""
        from .query import get_one_async
        return await get_one_async(cls, **kwargs)

    @classmethod
    def filter(cls, **kwargs: Any) -> QuerySet:
        """Inicia uma query com filtros e retorna um QuerySet."""
        return QuerySet(cls).filter(**kwargs)

    @classmethod
    def all(cls) -> QuerySet:
        """Retorna um QuerySet para todos os registros da tabela."""
        return QuerySet(cls)

    # --- Pydantic & FastAPI Integração ---
    @classmethod
    def as_pydantic(cls, name: Optional[str] = None, exclude: Optional[List[str]] = None) -> Type[Any]:
        """Gera um modelo Pydantic (classe) a partir deste modelo CaspyORM."""
        return generate_pydantic_model(cls, name=name, exclude=exclude or [])

    def to_pydantic_model(self, exclude: Optional[List[str]] = None) -> Any:
        """Converte esta instância do modelo CaspyORM para uma instância Pydantic."""
        PydanticModel = self.as_pydantic(exclude=exclude or [])
        return PydanticModel(**self.model_dump())

    # --- Métodos de Schema ---
    @classmethod
    def sync_table(cls, auto_apply: bool = False, verbose: bool = True):
        sync_table(cls, auto_apply=auto_apply, verbose=verbose)

    @classmethod
    async def sync_table_async(cls, auto_apply: bool = False, verbose: bool = True):
        """Sincroniza o schema da tabela (assíncrono)."""
        # TODO: Implementar sync_table_async
        sync_table(cls, auto_apply=auto_apply, verbose=verbose)

    def __repr__(self) -> str:
        attrs = ", ".join(f"{k}={getattr(self, k)!r}" for k in self.model_fields)
        return f"{self.__class__.__name__}({attrs})"

    def delete(self) -> None:
        """Deleta esta instância específica do banco de dados."""
        pk_fields = self.__caspy_schema__['primary_keys']
        if not pk_fields:
            raise RuntimeError("Não é possível deletar um modelo sem chave primária.")
        
        # VALIDAÇÃO ADICIONADA: Garante que as chaves primárias não são nulas ao deletar.
        pk_filters = {}
        for field in pk_fields:
            value = getattr(self, field, None)
            if value is None:
                raise ValidationError(f"Primary key '{field}' is required to delete, but was None.")
            pk_filters[field] = value
        
        from .query import QuerySet
        QuerySet(self.__class__).filter(**pk_filters).delete()
        logger.info(f"Instância deletada: {self}")

    async def delete_async(self) -> None:
        """Deleta esta instância específica do banco de dados (assíncrono)."""
        pk_fields = self.__caspy_schema__['primary_keys']
        if not pk_fields:
            raise RuntimeError("Não é possível deletar um modelo sem chave primária.")
        
        # VALIDAÇÃO ADICIONADA: Garante que as chaves primárias não são nulas ao deletar.
        pk_filters = {}
        for field in pk_fields:
            value = getattr(self, field, None)
            if value is None:
                raise ValidationError(f"Primary key '{field}' is required to delete, but was None.")
            pk_filters[field] = value
        
        from .query import QuerySet
        await QuerySet(self.__class__).filter(**pk_filters).delete_async()
        logger.info(f"Instância deletada (ASSÍNCRONO): {self}")

    async def update_collection(self, field_name: str, add: Any = None, remove: Any = None) -> Self:
        """
        Atualiza atomicamente um campo de coleção (List, Set) no banco de dados.

        Args:
            field_name (str): O nome do campo da coleção a ser atualizado.
            add (list | set): Itens para adicionar à coleção.
            remove (list | set): Itens para remover da coleção.

        Returns:
            Self: A instância atualizada.

        Raises:
            ValidationError: Se o campo não existe ou não é uma coleção.
        """
        if field_name not in self.model_fields:
            raise ValidationError(f"Campo '{field_name}' não existe no modelo {self.__class__.__name__}")
        
        from ._internal.query_builder import build_collection_update_cql
        cql, params = build_collection_update_cql(
            self.__caspy_schema__,
            field_name,
            add=add,
            remove=remove,
            pk_filters={pk: getattr(self, pk) for pk in self.__caspy_schema__['primary_keys']}
        )
        try:
            from .connection import get_session
            session = get_session()
            prepared = await session.prepare_async(cql)
            await session.execute_async(prepared, params)
            logger.info(f"Coleção '{field_name}' atualizada para a instância: {self}")
        except Exception as e:
            logger.error(f"Erro ao atualizar coleção: {e}")
            raise
        return self

    async def update_collection_async(self, field_name: str, add: Any = None, remove: Any = None) -> Self:
        """
        Atualiza atomicamente um campo de coleção (List, Set) no banco de dados (assíncrono).

        Args:
            field_name (str): O nome do campo da coleção a ser atualizado.
            add (list | set): Itens para adicionar à coleção.
            remove (list | set): Itens para remover da coleção.

        Returns:
            Self: A instância atualizada.

        Raises:
            ValidationError: Se o campo não existe ou não é uma coleção.
        """
        if field_name not in self.model_fields:
            raise ValidationError(f"Campo '{field_name}' não existe no modelo {self.__class__.__name__}")
        
        from ._internal.query_builder import build_collection_update_cql
        cql, params = build_collection_update_cql(
            self.__caspy_schema__,
            field_name,
            add=add,
            remove=remove,
            pk_filters={pk: getattr(self, pk) for pk in self.__caspy_schema__['primary_keys']}
        )
        try:
            from .connection import get_async_session
            session = get_async_session()
            prepared = await session.prepare_async(cql)
            future = session.execute_async(prepared, params)
            await future
            logger.info(f"Coleção '{field_name}' atualizada (ASSÍNCRONO) para a instância: {self}")
        except Exception as e:
            logger.error(f"Erro ao atualizar coleção (async): {e}")
            raise
        return self

    @classmethod
    def create_model(cls, name: str, fields: Dict[str, Any], table_name: Optional[str] = None) -> Type:
        """
        Cria dinamicamente um novo modelo CaspyORM.
        
        Args:
            name: Nome da classe do modelo
            fields: Dicionário com nome do campo -> instância de BaseField
            table_name: Nome da tabela (opcional, usa name.lower() + 's' por padrão)
            
        Returns:
            Nova classe de modelo dinamicamente criada
            
        Example:
            from caspyorm.fields import Text, Int, UUID
            
            UserModel = Model.create_model(
                name="User",
                fields={
                    "id": UUID(primary_key=True),
                    "name": Text(required=True),
                    "age": Int(),
                    "email": Text(index=True)
                },
                table_name="users"
            )
        """
        from .fields import BaseField
        
        # Validar que todos os campos são instâncias de BaseField
        for field_name, field_obj in fields.items():
            if not isinstance(field_obj, BaseField):
                raise TypeError(f"Campo '{field_name}' deve ser uma instância de BaseField, recebido: {type(field_obj)}")
        
        # Criar atributos da classe
        attrs = {
            '__table_name__': table_name or f"{name.lower()}s",
            '__caspy_schema__': None,  # Será preenchido pela metaclasse
            'model_fields': fields,
        }
        
        # Criar a classe usando a metaclasse ModelMetaclass
        from ._internal.model_construction import ModelMetaclass
        new_model_class = ModelMetaclass(name, (cls,), attrs)
        
        return new_model_class