# caspyorm/model.py (REVISADO)

import asyncio
import logging
from typing import Any, ClassVar, Dict, List, Optional, Type

from typing_extensions import Self

from .._internal.model_construction import ModelMetaclass
from .._internal.schema_sync import sync_table
from .._internal.serialization import (
    generate_pydantic_model,
    model_to_dict,
    model_to_json,
)
from ..utils.exceptions import ValidationError
from .query import QuerySet, filter_query, get_one, save_instance

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

    def model_dump_json(
        self, by_alias: bool = False, indent: Optional[int] = None
    ) -> str:
        return model_to_json(self, by_alias=by_alias, indent=indent)

    # --- HOOKS DE EVENTOS ---
    def before_save(self):
        """Hook chamado antes de salvar a instância."""
        pass

    def after_save(self):
        """Hook chamado após salvar a instância."""
        pass

    def before_update(self, update_data: dict):
        """Hook chamado antes de atualizar a instância."""
        pass

    def after_update(self, update_data: dict):
        """Hook chamado após atualizar a instância."""
        pass

    def before_delete(self):
        """Hook chamado antes de deletar a instância."""
        pass

    def after_delete(self):
        """Hook chamado após deletar a instância."""
        pass

    def save(self, ttl: int = None) -> Self:
        """
        Salva (insere ou atualiza) a instância no Cassandra.
        Se ttl for fornecido, define o tempo de expiração em segundos.
        """
        for pk_name in self.__caspy_schema__["primary_keys"]:
            if getattr(self, pk_name, None) is None:
                raise ValidationError(
                    f"Primary key '{pk_name}' cannot be None before saving."
                )
        try:
            self.before_save()
        except Exception as e:
            logger.error(f"Exceção em before_save: {e}")
            raise
        save_instance(self, ttl=ttl)
        try:
            self.after_save()
        except Exception as e:
            logger.error(f"Exceção em after_save: {e}")
            raise
        return self

    async def save_async(self, ttl: int = None) -> Self:
        """
        Salva (insere ou atualiza) a instância no Cassandra (assíncrono).
        Se ttl for fornecido, define o tempo de expiração em segundos.
        """
        for pk_name in self.__caspy_schema__["primary_keys"]:
            if getattr(self, pk_name, None) is None:
                raise ValidationError(
                    f"Primary key '{pk_name}' cannot be None before saving."
                )
        try:
            self.before_save()
        except Exception as e:
            logger.error(f"Exceção em before_save: {e}")
            raise
        from .query import save_instance_async

        await save_instance_async(self, ttl=ttl)
        try:
            self.after_save()
        except Exception as e:
            logger.error(f"Exceção em after_save: {e}")
            raise
        return self

    async def update(self, ttl: int = None, **kwargs: Any) -> Self:
        """
        Atualiza parcialmente esta instância no banco de dados.
        Se ttl for fornecido, define o tempo de expiração em segundos.
        """
        if not kwargs:
            logger.warning("update() chamado sem campos para atualizar")
            return self
        validated_data = {}
        for key, value in kwargs.items():
            if key not in self.model_fields:
                raise ValidationError(
                    f"Campo '{key}' não existe no modelo {self.__class__.__name__}"
                )
            field_obj = self.model_fields[key]
            if value is not None:
                try:
                    validated_value = field_obj.to_python(value)
                    validated_data[key] = validated_value
                    setattr(self, key, validated_value)
                except (TypeError, ValueError) as e:
                    raise ValidationError(f"Valor inválido para campo '{key}': {e}")
        if not validated_data:
            logger.warning("Nenhum campo válido fornecido para update()")
            return self
        try:
            self.before_update(validated_data)
        except Exception as e:
            logger.error(f"Exceção em before_update: {e}")
            raise
        from .._internal.query_builder import build_update_cql

        cql, params = build_update_cql(
            self.__caspy_schema__,
            update_data=validated_data,
            pk_filters={
                pk: getattr(self, pk) for pk in self.__caspy_schema__["primary_keys"]
            },
            ttl=ttl,
        )
        from ..types.batch import get_active_batch

        active_batch = get_active_batch()
        if active_batch:
            active_batch.add(cql, params)
            logger.debug(f"Adicionado update ao batch: {self.__class__.__name__}")
        else:
            try:
                from .connection import get_session

                session = get_session()
                prepared = session.prepare(cql)
                session.execute(prepared, params)
                logger.info(
                    f"Instância atualizada: {self.__class__.__name__} com campos: {list(validated_data.keys())}"
                )
            except Exception as e:
                logger.error(f"Erro ao atualizar instância: {e}")
                raise
        try:
            self.after_update(validated_data)
        except Exception as e:
            logger.error(f"Exceção em after_update: {e}")
            raise
        return self

    async def update_async(self, ttl: int = None, **kwargs: Any) -> Self:
        """
        Atualiza parcialmente esta instância no banco de dados (assíncrono).
        Se ttl for fornecido, define o tempo de expiração em segundos.
        """
        if not kwargs:
            logger.warning("update_async() chamado sem campos para atualizar")
            return self
        validated_data = {}
        for key, value in kwargs.items():
            if key not in self.model_fields:
                raise ValidationError(
                    f"Campo '{key}' não existe no modelo {self.__class__.__name__}"
                )
            field_obj = self.model_fields[key]
            if value is not None:
                try:
                    validated_value = field_obj.to_python(value)
                    validated_data[key] = validated_value
                    setattr(self, key, validated_value)
                except (TypeError, ValueError) as e:
                    raise ValidationError(f"Valor inválido para campo '{key}': {e}")
        if not validated_data:
            logger.warning("Nenhum campo válido fornecido para update_async()")
            return self
        try:
            self.before_update(validated_data)
        except Exception as e:
            logger.error(f"Exceção em before_update: {e}")
            raise
        from .._internal.query_builder import build_update_cql

        cql, params = build_update_cql(
            self.__caspy_schema__,
            update_data=validated_data,
            pk_filters={
                pk: getattr(self, pk) for pk in self.__caspy_schema__["primary_keys"]
            },
            ttl=ttl,
        )
        from ..types.batch import get_active_batch

        active_batch = get_active_batch()
        if active_batch:
            active_batch.add(cql, params)
            logger.debug(
                f"Adicionado update ao batch (async): {self.__class__.__name__}"
            )
        else:
            try:
                from .connection import get_async_session

                session = get_async_session()
                prepared = session.prepare(cql)
                future = session.execute_async(prepared, params)
                await asyncio.to_thread(future.result)
                logger.info(
                    f"Instância atualizada (ASSÍNCRONO): {self.__class__.__name__} com campos: {list(validated_data.keys())}"
                )
            except Exception as e:
                logger.error(f"Erro ao atualizar instância (async): {e}")
                raise
        try:
            self.after_update(validated_data)
        except Exception as e:
            logger.error(f"Exceção em after_update: {e}")
            raise
        return self

    @classmethod
    def create(cls, **kwargs: Any) -> Self:
        """Cria uma nova instância e a salva no banco de dados."""
        instance = cls(**kwargs)
        return instance.save()

    @classmethod
    async def create_async(cls, **kwargs: Any) -> Self:
        """Cria uma nova instância e a salva no banco de dados (assíncrono)."""
        instance = cls(**kwargs)
        return await instance.save_async()

    @classmethod
    def bulk_create(cls, instances: List["Model"], ttl: int = None) -> List["Model"]:
        """
        Cria múltiplas instâncias em uma única operação batch.
        Se ttl for fornecido, define o tempo de expiração em segundos.
        """
        if not instances:
            return []
        model_class = instances[0].__class__
        if not all(isinstance(instance, model_class) for instance in instances):
            raise ValidationError("Todas as instâncias devem ser do mesmo tipo")
        from ..types.batch import BatchQuery

        with BatchQuery() as batch:
            for instance in instances:
                instance.save(ttl=ttl)
        return instances

    @classmethod
    async def bulk_create_async(
        cls, instances: List["Model"], ttl: int = None
    ) -> List["Model"]:
        """
        Cria múltiplas instâncias em uma única operação batch (assíncrono).
        Se ttl for fornecido, define o tempo de expiração em segundos.
        """
        if not instances:
            return []
        model_class = instances[0].__class__
        if not all(isinstance(instance, model_class) for instance in instances):
            raise ValidationError("Todas as instâncias devem ser do mesmo tipo")
        from ..types.batch import BatchQuery

        with BatchQuery() as batch:
            for instance in instances:
                await instance.save_async(ttl=ttl)
        return instances

    @classmethod
    def get(cls, **kwargs: Any) -> Optional["Model"]:
        """Obtém uma única instância que corresponde aos filtros."""
        return get_one(cls, **kwargs)

    @classmethod
    async def get_async(cls, **kwargs: Any) -> Optional["Model"]:
        """Obtém uma única instância que corresponde aos filtros (assíncrono)."""
        from .query import get_one_async

        return await get_one_async(cls, **kwargs)

    @classmethod
    def filter(cls, **kwargs: Any) -> QuerySet:
        """Filtra instâncias baseado nos critérios fornecidos."""
        return filter_query(cls, **kwargs)

    @classmethod
    def all(cls) -> QuerySet:
        """Retorna todas as instâncias do modelo."""
        return QuerySet(cls)

    @classmethod
    def as_pydantic(
        cls, name: Optional[str] = None, exclude: Optional[List[str]] = None
    ) -> Type[Any]:
        """Gera um modelo Pydantic equivalente."""
        return generate_pydantic_model(cls, name=name, exclude=exclude)

    def to_pydantic_model(self, exclude: Optional[List[str]] = None) -> Any:
        """Converte esta instância para um modelo Pydantic."""
        pydantic_model = self.as_pydantic(exclude=exclude)
        return pydantic_model(**self.model_dump())

    @classmethod
    def sync_table(cls, auto_apply: bool = False, verbose: bool = True):
        """Sincroniza o schema da tabela com o modelo."""
        sync_table(cls, auto_apply=auto_apply, verbose=verbose)

    @classmethod
    async def sync_table_async(cls, auto_apply: bool = False, verbose: bool = True):
        """Sincroniza o schema da tabela com o modelo (assíncrono)."""
        from .._internal.schema_sync import sync_table_async

        await sync_table_async(cls, auto_apply=auto_apply, verbose=verbose)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.model_dump()}>"

    def delete(self) -> None:
        """Remove esta instância do banco de dados."""
        for pk_name in self.__caspy_schema__["primary_keys"]:
            if getattr(self, pk_name, None) is None:
                raise ValidationError(
                    f"Primary key '{pk_name}' cannot be None before deleting."
                )
        try:
            self.before_delete()
        except Exception as e:
            logger.error(f"Exceção em before_delete: {e}")
            raise
        from .._internal.query_builder import build_delete_cql

        cql, params = build_delete_cql(
            self.__caspy_schema__,
            filters={
                pk: getattr(self, pk) for pk in self.__caspy_schema__["primary_keys"]
            },
        )
        from ..types.batch import get_active_batch

        active_batch = get_active_batch()
        if active_batch:
            active_batch.add(cql, params)
            logger.debug(f"Adicionado delete ao batch: {self.__class__.__name__}")
        else:
            try:
                from .connection import get_session

                session = get_session()
                prepared = session.prepare(cql)
                session.execute(prepared, params)
                logger.info(f"Instância deletada: {self.__class__.__name__}")
            except Exception as e:
                logger.error(f"Erro ao deletar instância: {e}")
                raise
        try:
            self.after_delete()
        except Exception as e:
            logger.error(f"Exceção em after_delete: {e}")
            raise

    async def delete_async(self) -> None:
        """Remove esta instância do banco de dados (assíncrono)."""
        for pk_name in self.__caspy_schema__["primary_keys"]:
            if getattr(self, pk_name, None) is None:
                raise ValidationError(
                    f"Primary key '{pk_name}' cannot be None before deleting."
                )
        try:
            self.before_delete()
        except Exception as e:
            logger.error(f"Exceção em before_delete: {e}")
            raise
        from .._internal.query_builder import build_delete_cql

        cql, params = build_delete_cql(
            self.__caspy_schema__,
            filters={
                pk: getattr(self, pk) for pk in self.__caspy_schema__["primary_keys"]
            },
        )
        from ..types.batch import get_active_batch

        active_batch = get_active_batch()
        if active_batch:
            active_batch.add(cql, params)
            logger.debug(
                f"Adicionado delete ao batch (async): {self.__class__.__name__}"
            )
        else:
            try:
                from .connection import get_async_session

                session = get_async_session()
                prepared = session.prepare(cql)
                future = session.execute_async(prepared, params)
                await asyncio.to_thread(future.result)
                logger.info(
                    f"Instância deletada (ASSÍNCRONO): {self.__class__.__name__}"
                )
            except Exception as e:
                logger.error(f"Erro ao deletar instância (async): {e}")
                raise
        try:
            self.after_delete()
        except Exception as e:
            logger.error(f"Exceção em after_delete: {e}")
            raise

    async def update_collection(
        self, field_name: str, add: Any = None, remove: Any = None
    ) -> Self:
        """
        Atualiza uma coleção (list, set, map) adicionando ou removendo elementos.
        """
        if field_name not in self.model_fields:
            raise ValidationError(
                f"Campo '{field_name}' não existe no modelo {self.__class__.__name__}"
            )

        field_obj = self.model_fields[field_name]
        if not hasattr(field_obj, "collection_type"):
            raise ValidationError(f"Campo '{field_name}' não é uma coleção")

        # Construir query de atualização da coleção
        from .._internal.query_builder import build_collection_update_cql

        cql, params = build_collection_update_cql(
            self.__caspy_schema__,
            field_name=field_name,
            add=add,
            remove=remove,
            pk_filters={
                pk: getattr(self, pk) for pk in self.__caspy_schema__["primary_keys"]
            },
        )

        # Suporte a batch
        from ..types.batch import get_active_batch

        active_batch = get_active_batch()
        if active_batch:
            active_batch.add(cql, params)
            logger.debug(
                f"Adicionado update_collection ao batch: {self.__class__.__name__}"
            )
        else:
            try:
                from .connection import get_session

                session = get_session()
                session.execute(cql, params)
                logger.info(
                    f"Coleção '{field_name}' atualizada: {self.__class__.__name__}"
                )
            except Exception as e:
                logger.error(f"Erro ao atualizar coleção: {e}")
                raise

        # Após sucesso, invalidar o valor local
        if field_name in self.__dict__:
            del self.__dict__[field_name]
        return self

    async def update_collection_async(
        self, field_name: str, add: Any = None, remove: Any = None
    ) -> Self:
        """
        Atualiza uma coleção (list, set, map) adicionando ou removendo elementos (assíncrono).
        """
        if field_name not in self.model_fields:
            raise ValidationError(
                f"Campo '{field_name}' não existe no modelo {self.__class__.__name__}"
            )

        field_obj = self.model_fields[field_name]
        if not hasattr(field_obj, "collection_type"):
            raise ValidationError(f"Campo '{field_name}' não é uma coleção")

        # Construir query de atualização da coleção
        from .._internal.query_builder import build_collection_update_cql

        cql, params = build_collection_update_cql(
            self.__caspy_schema__,
            field_name=field_name,
            add=add,
            remove=remove,
            pk_filters={
                pk: getattr(self, pk) for pk in self.__caspy_schema__["primary_keys"]
            },
        )

        # Suporte a batch
        from ..types.batch import get_active_batch

        active_batch = get_active_batch()
        if active_batch:
            active_batch.add(cql, params)
            logger.debug(
                f"Adicionado update_collection ao batch (async): {self.__class__.__name__}"
            )
        else:
            try:
                from .connection import get_async_session

                session = get_async_session()
                prepared = session.prepare(cql)
                future = session.execute_async(prepared, params)
                await asyncio.to_thread(future.result)
                logger.info(
                    f"Coleção '{field_name}' atualizada (ASSÍNCRONO): {self.__class__.__name__}"
                )
            except Exception as e:
                logger.error(f"Erro ao atualizar coleção (async): {e}")
                raise

        # Após sucesso, invalidar o valor local
        if field_name in self.__dict__:
            del self.__dict__[field_name]
        return self

    @classmethod
    def create_model(
        cls, name: str, fields: Dict[str, Any], table_name: Optional[str] = None
    ) -> Type:
        """
        Cria dinamicamente um novo modelo.

        Args:
            name: Nome da classe do modelo
            fields: Dicionário de campos {nome: tipo}
            table_name: Nome da tabela (opcional, usa o nome da classe se não fornecido)

        Returns:
            Nova classe de modelo
        """
        from .._internal.model_construction import ModelMetaclass

        return ModelMetaclass(
            name,
            (cls,),
            {
                "__table_name__": table_name or f"{name.lower()}s",
                "__caspy_schema__": None,  # Será preenchido pela metaclasse
                "model_fields": fields,
            },
        )
