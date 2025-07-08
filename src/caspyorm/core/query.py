# caspyorm/query.py (REVISADO E AMPLIADO)

import asyncio
from typing import Any, Dict, List, Optional, Type
from typing_extensions import Self
from .connection import get_session, get_async_session, execute, execute_cql_async
from .._internal import query_builder
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel
import logging
import warnings
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .model import Model

logger = logging.getLogger(__name__)

def _map_row_to_instance(model_cls, row_dict):
    """Mapeia um dicionário (linha do DB) para uma instância do modelo."""
    return model_cls(**row_dict)

class QuerySet:
    """
    Representa uma query preguiçosa (lazy) que pode ser encadeada.
    Suporta operações síncronas e assíncronas.
    """
    def __init__(self, model_cls: Type["Model"]):
        self.model_cls = model_cls
        self._filters: Dict[str, Any] = {}
        self._limit: Optional[int] = None
        self._ordering: List[str] = []  # NOVO: lista de campos para ordenação
        self._result_cache: Optional[List["Model"]] = None
        self._allow_filtering = False

    def __iter__(self):
        """Executa a query quando o queryset é iterado (síncrono)."""
        if self._result_cache is None:
            self._execute_query_sync()
        return iter(self._result_cache or [])

    async def __aiter__(self):
        """Executa a query quando o queryset é iterado (assíncrono)."""
        if self._result_cache is None:
            await self._execute_query_async()
        for item in self._result_cache or []:
            yield item

    def __repr__(self) -> str:
        # Mostra os resultados se a query já foi executada, senão mostra a query planejada.
        if self._result_cache is not None:
            return repr(self._result_cache)
        return f"<QuerySet model={self.model_cls.__name__} filters={self._filters} ordering={self._ordering}>"

    def _clone(self) -> Self:
        """Cria um clone do QuerySet atual para permitir o encadeamento."""
        new_qs = self.__class__(self.model_cls)
        new_qs._filters = self._filters.copy()
        new_qs._limit = self._limit
        new_qs._ordering = self._ordering[:]
        new_qs._allow_filtering = self._allow_filtering  # Copiar o estado de allow_filtering
        return new_qs

    def _execute_query_sync(self):
        """Executa a query no banco de dados e armazena os resultados no cache (síncrono)."""
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=None,  # Seleciona todas as colunas
            filters=self._filters,
            limit=self._limit,
            ordering=self._ordering,
            allow_filtering=self._allow_filtering
        )
        session = get_session()
        # Sempre preparar a query para garantir suporte a parâmetros posicionais
        prepared = session.prepare(cql)
        result_set = session.execute(prepared, params)
        self._result_cache = [_map_row_to_instance(self.model_cls, row._asdict()) for row in result_set]
        logger.debug(f"Executando query (SÍNCRONO): {cql} com parâmetros: {params}")

    async def _execute_query_async(self):
        """Executa a query no banco de dados e armazena os resultados no cache (assíncrono)."""
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=None,
            filters=self._filters,
            limit=self._limit,
            ordering=self._ordering,
            allow_filtering=True
        )
        from . import connection
        session = get_async_session()
        # PreparedStatement transparente com cache
        if cql in connection.connection._prepared_statement_cache:
            prepared = connection.connection._prepared_statement_cache[cql]
        else:
            prepared = session.prepare(cql)
            connection.connection._prepared_statement_cache[cql] = prepared
        result_set = await asyncio.wrap_future(session.execute_async(prepared, params))
        self._result_cache = [_map_row_to_instance(self.model_cls, row._asdict()) for row in result_set]
        logger.debug(f"Executando query (ASSÍNCRONO): {cql} com parâmetros: {params}")

    def allow_filtering(self) -> Self:
        """
        Permite o uso de ALLOW FILTERING na query.
        Use com cautela, pois pode impactar o desempenho em grandes tabelas.
        """
        clone = self._clone()
        clone._allow_filtering = True
        return clone

    # --- Métodos de API Pública do QuerySet (Síncronos) ---
    
    def filter(self, **kwargs: Any) -> Self:
        """Adiciona condições de filtro à query."""
        clone = self._clone()
        
        # --- AVISO PARA CAMPOS NÃO INDEXADOS ---
        schema = self.model_cls.__caspy_schema__
        indexed_fields = set(schema['primary_keys']) | set(schema.get('indexes', []))
        
        for key in kwargs:
            field_name = key.split('__')[0]  # Remove sufixos como __exact, __contains, etc.
            if field_name not in indexed_fields:
                warnings.warn(
                    f"O campo '{field_name}' não é uma chave primária nem está indexado. "
                    f"A consulta pode ser ineficiente ou falhar sem 'ALLOW FILTERING'.",
                    UserWarning
                )
        # ----------------------------------------
                
        clone._filters.update(kwargs)
        return clone

    def limit(self, count: int) -> Self:
        """Limita o número de resultados retornados."""
        clone = self._clone()
        clone._limit = count
        return clone

    def order_by(self, *fields: str) -> Self:
        """Define a ordenação da query."""
        clone = self._clone()
        clone._ordering = list(fields)
        return clone

    def all(self) -> List["Model"]:
        """Executa a query e retorna todos os resultados como uma lista (síncrono)."""
        if self._result_cache is None:
            self._execute_query_sync()
        return self._result_cache or []

    async def all_async(self) -> List["Model"]:
        """Executa a query e retorna todos os resultados como uma lista (assíncrono)."""
        if self._result_cache is None:
            await self._execute_query_async()
        return self._result_cache or []
    
    def first(self) -> Optional["Model"]:
        """Executa a query e retorna o primeiro resultado, ou None se não houver resultados (síncrono)."""
        # Otimização: aplica LIMIT 1 na query se ainda não foi executada
        if self._result_cache is None and self._limit is None:
            return self.limit(1).first()

        results = self.all()
        return results[0] if results else None

    async def first_async(self) -> Optional["Model"]:
        """Executa a query e retorna o primeiro resultado, ou None se não houver resultados (assíncrono)."""
        if self._result_cache is None and self._limit is None:
            return await self.limit(1).first_async()
        results = await self.all_async()
        return results[0] if results else None

    def count(self) -> int:
        """
        Executa uma query `SELECT COUNT(*)` otimizada e retorna o número de resultados (síncrono).
        """
        if self._result_cache is not None:
            return len(self._result_cache)
        cql, params = query_builder.build_count_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        session = get_session()
        prepared = session.prepare(cql)
        result_set = session.execute(prepared, params)
        row = result_set.one()
        return row.count if row else 0

    async def count_async(self) -> int:
        """
        Executa uma query `SELECT COUNT(*)` otimizada e retorna o número de resultados (assíncrono).
        """
        if self._result_cache is not None:
            return len(self._result_cache)
        cql, params = query_builder.build_count_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        session = get_async_session()
        prepared = session.prepare(cql)
        result_set = await asyncio.wrap_future(session.execute_async(prepared, params))
        row = result_set.one()
        return row.count if row else 0

    def exists(self) -> bool:
        """
        Verifica se existe pelo menos um resultado que corresponde aos filtros (síncrono).
        Otimizado para usar LIMIT 1.
        """
        if self._result_cache is not None:
            return len(self._result_cache) > 0
        
        # Otimização: usar LIMIT 1 para verificar existência
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=['1'],  # Seleciona apenas uma coluna constante
            filters=self._filters,
            limit=1,
            ordering=self._ordering,
            allow_filtering=self._allow_filtering
        )
        session = get_session()
        prepared = session.prepare(cql)
        result_set = session.execute(prepared, params)
        return result_set.one() is not None

    async def exists_async(self) -> bool:
        """
        Verifica se existe pelo menos um resultado que corresponde aos filtros (assíncrono).
        Otimizado para usar LIMIT 1.
        """
        if self._result_cache is not None:
            return len(self._result_cache) > 0
        
        # Otimização: usar LIMIT 1 para verificar existência
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=['1'],  # Seleciona apenas uma coluna constante
            filters=self._filters,
            limit=1,
            ordering=self._ordering,
            allow_filtering=self._allow_filtering
        )
        session = get_async_session()
        prepared = session.prepare(cql)
        result_set = await asyncio.wrap_future(session.execute_async(prepared, params))
        return result_set.one() is not None

    def delete(self) -> int:
        """
        Deleta todos os registros que correspondem aos filtros (síncrono).
        IMPORTANTE: No Cassandra, você DEVE especificar a chave de partição completa.
        """
        if not self._filters:
            raise ValueError("A deleção em massa sem filtros não é permitida por segurança.")
        
        cql, params = query_builder.build_delete_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        session = get_session()
        prepared = session.prepare(cql)
        result = session.execute(prepared, params)
        logger.info(f"Deletados registros: {self.model_cls.__name__} com filtros: {self._filters}")
        return 1  # Cassandra não retorna número de linhas afetadas

    async def delete_async(self) -> int:
        """
        Deleta todos os registros que correspondem aos filtros (assíncrono).
        IMPORTANTE: No Cassandra, você DEVE especificar a chave de partição completa.
        """
        if not self._filters:
            raise ValueError("A deleção em massa sem filtros não é permitida por segurança.")
        
        cql, params = query_builder.build_delete_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        session = get_async_session()
        prepared = session.prepare(cql)
        result = await asyncio.wrap_future(session.execute_async(prepared, params))
        logger.info(f"Deletados registros (ASSÍNCRONO): {self.model_cls.__name__} com filtros: {self._filters}")
        return 1  # Cassandra não retorna número de linhas afetadas

    def page(self, page_size: int = 100, paging_state: Any = None):
        """
        Implementa paginação usando o paging_state do Cassandra (síncrono).
        Retorna um objeto com resultados e próximo paging_state.
        """
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=None,
            filters=self._filters,
            limit=page_size,
            ordering=self._ordering,
            allow_filtering=self._allow_filtering
        )
        session = get_session()
        prepared = session.prepare(cql)
        bound = prepared.bind(params)
        if paging_state:
            bound.paging_state = paging_state
        
        result_set = session.execute(bound)
        results = [_map_row_to_instance(self.model_cls, row._asdict()) for row in result_set]
        
        return {
            'results': results,
            'paging_state': result_set.paging_state,
            'has_more_pages': result_set.has_more_pages
        }

    async def page_async(self, page_size: int = 100, paging_state: Any = None):
        """
        Implementa paginação usando o paging_state do Cassandra (assíncrono).
        Retorna um objeto com resultados e próximo paging_state.
        """
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=None,
            filters=self._filters,
            limit=page_size,
            ordering=self._ordering,
            allow_filtering=self._allow_filtering
        )
        session = get_async_session()
        prepared = session.prepare(cql)
        bound = prepared.bind(params)
        if paging_state:
            bound.paging_state = paging_state
        
        result_set = await asyncio.wrap_future(session.execute_async(bound))
        results = [_map_row_to_instance(self.model_cls, row._asdict()) for row in result_set]
        
        return {
            'results': results,
            'paging_state': result_set.paging_state,
            'has_more_pages': result_set.has_more_pages
        }

    def bulk_create(self, instances: List["Model"]) -> List["Model"]:
        """
        Insere uma lista de instâncias de modelo em lote usando um UNLOGGED BATCH
        para máxima performance. As instâncias são modificadas no local.
        Nota: Validações de Primary Key devem ser feitas antes de chamar este método.
        """
        if not instances:
            return []
        
        # Validar que todas as instâncias são do mesmo tipo
        model_class = instances[0].__class__
        if not all(isinstance(instance, model_class) for instance in instances):
            raise ValueError("Todas as instâncias devem ser do mesmo tipo")
        
        # Usar batch se disponível
        from ..types.batch import BatchQuery
        with BatchQuery() as batch:
            for instance in instances:
                instance.save()
        
        return instances

# --- Funções de Conveniência ---

def save_instance(instance) -> None:
    """
    Salva uma instância de modelo no banco de dados (síncrono).
    Usa INSERT com IF NOT EXISTS para evitar duplicatas.
    """
    from .._internal.query_builder import build_insert_cql
    
    # Construir query INSERT
    cql = build_insert_cql(instance.__caspy_schema__)
    params = list(instance.model_dump().values())
    
    # Verificar se há batch ativo
    from ..types.batch import get_active_batch
    active_batch = get_active_batch()
    
    if active_batch:
        # Adicionar ao batch ativo
        active_batch.add(cql, params)
        logger.debug(f"Adicionado ao batch: {instance.__class__.__name__}")
    else:
        # Executar imediatamente
        session = get_session()
        prepared = session.prepare(cql)
        session.execute(prepared, params)
        logger.info(f"Instância salva: {instance.__class__.__name__}")

async def save_instance_async(instance) -> None:
    """
    Salva uma instância de modelo no banco de dados (assíncrono).
    Usa INSERT com IF NOT EXISTS para evitar duplicatas.
    """
    from .._internal.query_builder import build_insert_cql
    
    # Construir query INSERT
    cql = build_insert_cql(instance.__caspy_schema__)
    params = list(instance.model_dump().values())
    
    # Verificar se há batch ativo
    from ..types.batch import get_active_batch
    active_batch = get_active_batch()
    
    if active_batch:
        # Adicionar ao batch ativo
        active_batch.add(cql, params)
        logger.debug(f"Adicionado ao batch (async): {instance.__class__.__name__}")
    else:
        # Executar imediatamente
        session = get_async_session()
        prepared = session.prepare(cql)
        future = session.execute_async(prepared, params)
        await asyncio.to_thread(future.result)
        logger.info(f"Instância salva (ASSÍNCRONO): {instance.__class__.__name__}")

def get_one(model_cls: Type["Model"], **kwargs: Any) -> Optional["Model"]:
    """Busca um único registro."""
    return QuerySet(model_cls).filter(**kwargs).first()

async def get_one_async(model_cls: Type["Model"], **kwargs: Any) -> Optional["Model"]:
    """Busca um único registro (assíncrono)."""
    return await QuerySet(model_cls).filter(**kwargs).first_async()

def filter_query(model_cls: Type["Model"], **kwargs: Any) -> QuerySet:
    """Inicia uma query com filtros e retorna um QuerySet."""
    return QuerySet(model_cls).filter(**kwargs)