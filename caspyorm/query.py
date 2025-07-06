# caspyorm/query.py (REVISADO E AMPLIADO)

import asyncio
from typing import Any, Dict, List, Optional, Type
from typing_extensions import Self
from caspyorm.connection import get_session, get_async_session, execute
from caspyorm._internal import query_builder
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

    def __iter__(self):
        """Executa a query quando o queryset é iterado (síncrono)."""
        if self._result_cache is None:
            print("EXECUTADO")
            self._execute_query()
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
        new_qs._ordering = self._ordering[:]  # NOVO: copiar lista de ordenação
        return new_qs

    def _execute_query(self):
        """Executa a query no banco de dados e armazena os resultados no cache (síncrono)."""
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=None,  # Seleciona todas as colunas
            filters=self._filters,
            limit=self._limit,
            ordering=self._ordering,
            allow_filtering=True # Adicionado para permitir filtros em campos não-PK
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
            columns=None,  # Seleciona todas as colunas
            filters=self._filters,
            limit=self._limit,
            ordering=self._ordering,
            allow_filtering=True # Adicionado para permitir filtros em campos não-PK
        )
        session = get_async_session()
        # Preparar a query de forma síncrona, executar de forma assíncrona
        prepared = session.prepare(cql)
        future = session.execute_async(prepared, params)
        # ResponseFuture não é awaitable, precisamos usar asyncio.to_thread para obter o resultado
        result_set = await asyncio.to_thread(future.result)
        self._result_cache = [_map_row_to_instance(self.model_cls, row._asdict()) for row in result_set]
        logger.debug(f"Executando query (ASSÍNCRONO): {cql} com parâmetros: {params}")

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
            self._execute_query()
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
        # Otimização: aplica LIMIT 1 na query se ainda não foi executada
        if self._result_cache is None and self._limit is None:
            return await self.limit(1).first_async()

        results = await self.all_async()
        return results[0] if results else None

    def count(self) -> int:
        """
        Executa uma query `SELECT COUNT(*)` otimizada e retorna o número de resultados (síncrono).
        """
        # Se a query já foi executada, podemos simplesmente retornar o tamanho do cache.
        if self._result_cache is not None:
            return len(self._result_cache)

        # Se não, construímos e executamos a query COUNT(*) otimizada.
        cql, params = query_builder.build_count_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        
        session = get_session()
        prepared = session.prepare(cql)
        result_set = session.execute(prepared, params)
        
        # O resultado de COUNT(*) é uma única linha com uma coluna chamada 'count'.
        row = result_set.one()
        return row.count if row else 0

    async def count_async(self) -> int:
        """
        Executa uma query `SELECT COUNT(*)` otimizada e retorna o número de resultados (assíncrono).
        """
        # Se a query já foi executada, podemos simplesmente retornar o tamanho do cache.
        if self._result_cache is not None:
            return len(self._result_cache)

        # Se não, construímos e executamos a query COUNT(*) otimizada.
        cql, params = query_builder.build_count_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        
        session = get_async_session()
        prepared = session.prepare(cql)
        future = session.execute_async(prepared, params)
        result_set = await asyncio.to_thread(future.result)
        
        # O resultado de COUNT(*) é uma única linha com uma coluna chamada 'count'.
        row = result_set.one()
        return row.count if row else 0

    def exists(self) -> bool:
        """
        Verifica de forma otimizada se algum registro corresponde aos filtros,
        executando uma query `SELECT <pk> ... LIMIT 1` (síncrono).
        Retorna True se pelo menos um registro for encontrado, False caso contrário.
        """
        if self._result_cache is not None:
            return bool(self._result_cache)

        # Seleciona apenas o primeiro campo da chave primária para máxima eficiência.
        pk_to_select = [self.model_cls.__caspy_schema__['primary_keys'][0]]

        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=pk_to_select,
            filters=self._filters,
            limit=1
        )
        
        session = get_session()
        prepared = session.prepare(cql)
        result_set = session.execute(prepared, params)
        
        # Se .one() retornar uma linha, significa que existe. Se retornar None, não existe.
        return result_set.one() is not None

    async def exists_async(self) -> bool:
        """
        Verifica de forma otimizada se algum registro corresponde aos filtros,
        executando uma query `SELECT <pk> ... LIMIT 1` (assíncrono).
        Retorna True se pelo menos um registro for encontrado, False caso contrário.
        """
        if self._result_cache is not None:
            return bool(self._result_cache)

        # Seleciona apenas o primeiro campo da chave primária para máxima eficiência.
        pk_to_select = [self.model_cls.__caspy_schema__['primary_keys'][0]]

        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=pk_to_select,
            filters=self._filters,
            limit=1
        )
        
        session = get_async_session()
        prepared = session.prepare(cql)
        future = session.execute_async(prepared, params)
        result_set = await asyncio.to_thread(future.result)
        
        # Se .one() retornar uma linha, significa que existe. Se retornar None, não existe.
        return result_set.one() is not None

    def delete(self) -> int:
        """
        Executa uma operação de deleção para os registros que correspondem
        aos filtros atuais (síncrono).
        Retorna o número de linhas que *poderiam* ter sido deletadas.
        Nota: O Cassandra não retorna o número de linhas afetadas.
        """
        if self._result_cache is not None:
            # Se a query já foi executada, podemos deletar por chave primária
            count = 0
            for item in self._result_cache:
                item.delete()
                count += 1
            return count
        # Se a query não foi executada, fazemos uma deleção em massa com base nos filtros
        session = get_session()
        cql, params = query_builder.build_delete_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        logger.debug(f"Executando DELETE (SÍNCRONO): {cql} com parâmetros: {params}")
        prepared = session.prepare(cql)
        session.execute(prepared, params)
        return 0  # Cassandra não retorna número de linhas deletadas

    async def delete_async(self) -> int:
        """
        Executa uma operação de deleção para os registros que correspondem
        aos filtros atuais (assíncrono).
        Retorna o número de linhas que *poderiam* ter sido deletadas.
        Nota: O Cassandra não retorna o número de linhas afetadas.
        """
        if self._result_cache is not None:
            # Se a query já foi executada, podemos deletar por chave primária
            count = 0
            for item in self._result_cache:
                await item.delete_async()
                count += 1
            return count
        # Se a query não foi executada, fazemos uma deleção em massa com base nos filtros
        session = get_async_session()
        cql, params = query_builder.build_delete_cql(
            self.model_cls.__caspy_schema__,
            filters=self._filters
        )
        logger.debug(f"Executando DELETE (ASSÍNCRONO): {cql} com parâmetros: {params}")
        prepared = session.prepare(cql)
        future = session.execute_async(prepared, params)
        await future
        return 0  # Cassandra não retorna número de linhas deletadas

    def page(self, page_size: int = 100, paging_state: Any = None):
        """
        Retorna uma página de resultados e o paging_state para a próxima página (síncrono).
        Args:
            page_size: Tamanho da página (quantidade de registros)
            paging_state: Estado de paginação retornado pela página anterior (ou None para a primeira página)
        Returns:
            (resultados: List[Model], next_paging_state: Any)
        """
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=None,  # Seleciona todas as colunas
            filters=self._filters,
            limit=None,  # Não usar LIMIT para paginação real
            ordering=self._ordering
        )
        session = get_session()
        prepared = session.prepare(cql)
        statement = prepared.bind(params)
        statement.fetch_size = page_size
        if paging_state:
            statement.paging_state = paging_state
        result_set = session.execute(statement)
        resultados = [_map_row_to_instance(self.model_cls, row._asdict()) for row in result_set]
        next_paging_state = result_set.paging_state
        return resultados, next_paging_state

    async def page_async(self, page_size: int = 100, paging_state: Any = None):
        """
        Retorna uma página de resultados e o paging_state para a próxima página (assíncrono).
        Args:
            page_size: Tamanho da página (quantidade de registros)
            paging_state: Estado de paginação retornado pela página anterior (ou None para a primeira página)
        Returns:
            (resultados: List[Model], next_paging_state: Any)
        """
        cql, params = query_builder.build_select_cql(
            self.model_cls.__caspy_schema__,
            columns=None,  # Seleciona todas as colunas
            filters=self._filters,
            limit=None,  # Não usar LIMIT para paginação real
            ordering=self._ordering
        )
        session = get_async_session()
        prepared = session.prepare(cql)
        statement = prepared.bind(params)
        statement.fetch_size = page_size
        if paging_state:
            statement.paging_state = paging_state
        future = session.execute_async(statement)
        result_set = await asyncio.to_thread(future.result)
        resultados = [_map_row_to_instance(self.model_cls, row._asdict()) for row in result_set]
        next_paging_state = result_set.paging_state
        return resultados, next_paging_state

    def bulk_create(self, instances: List["Model"]) -> List["Model"]:
        """
        Lógica interna para inserir instâncias em lote.
        """
        if not instances:
            return []
        
        session = get_session()
        table_name = self.model_cls.__table_name__
        
        # Pega a query de inserção e os nomes das colunas uma única vez
        data_sample = instances[0].model_dump()
        columns = list(data_sample.keys())
        placeholders = ", ".join(['?'] * len(columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        prepared_statement = session.prepare(insert_query)
        
        # Usar UNLOGGED BATCH para performance
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        
        for instance in instances:
            # Validação crucial: garantir que as chaves primárias não são nulas
            for pk_name in self.model_cls.__caspy_schema__['primary_keys']:
                if getattr(instance, pk_name, None) is None:
                    # Alternativamente, poderíamos gerar o UUID aqui se for o caso
                    raise ValueError(f"Primary key '{pk_name}' não pode ser nula em bulk_create. Instância: {instance}")
            
            data = instance.model_dump()
            params = [data.get(col) for col in columns]  # Garante a ordem correta
            batch.add(prepared_statement, params)
            
            # Limite prático para o tamanho do batch para evitar timeouts
            if len(batch) >= 100:
                session.execute(batch)
                batch.clear()

        # Executa o batch final com os registros restantes
        if len(batch) > 0:
            session.execute(batch)
            
        logger.info(f"{len(instances)} instâncias inseridas em lote na tabela '{table_name}'.")
        return instances

# --- Funções do módulo que interagem com o QuerySet ---

def save_instance(instance) -> None:
    """Salva (insere ou atualiza) a instância no Cassandra."""
    if not get_session():
        raise RuntimeError("Não há conexão ativa com o Cassandra")
    
    table_name = instance.__class__.__table_name__
    data = instance.model_dump()
    
    # Construir query INSERT com placeholders parametrizados
    columns = list(data.keys())
    placeholders = ", ".join(['?'] * len(columns))
    
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
    """
    
    # Preparar e executar com parâmetros
    try:
        session = get_session()
        prepared = session.prepare(insert_query)
        session.execute(prepared, list(data.values()))
        logger.info(f"Instância salva na tabela '{table_name}'")
    except Exception as e:
        logger.error(f"Erro ao salvar instância: {e}")
        raise

async def save_instance_async(instance) -> None:
    """Salva (insere ou atualiza) a instância no Cassandra (assíncrono)."""
    if not get_async_session():
        raise RuntimeError("Não há conexão assíncrona ativa com o Cassandra")
    
    table_name = instance.__class__.__table_name__
    data = instance.model_dump()
    
    # Construir query INSERT com placeholders parametrizados
    columns = list(data.keys())
    placeholders = ", ".join(['?'] * len(columns))
    
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
    """
    
    # Preparar e executar com parâmetros de forma assíncrona
    try:
        session = get_async_session()
        prepared = session.prepare(insert_query)
        future = session.execute_async(prepared, list(data.values()))
        await asyncio.to_thread(future.result)
        logger.info(f"Instância salva na tabela '{table_name}' (ASSÍNCRONO)")
    except Exception as e:
        logger.error(f"Erro ao salvar instância (async): {e}")
        raise

def get_one(model_cls: Type["Model"], **kwargs: Any) -> Optional["Model"]:
    """Busca um único registro usando um QuerySet."""
    return QuerySet(model_cls).filter(**kwargs).first()

async def get_one_async(model_cls: Type["Model"], **kwargs: Any) -> Optional["Model"]:
    """Busca um único registro usando um QuerySet (assíncrono)."""
    return await QuerySet(model_cls).filter(**kwargs).first_async()

def filter_query(model_cls: Type["Model"], **kwargs: Any) -> QuerySet:
    """Retorna um QuerySet com os filtros aplicados."""
    return QuerySet(model_cls).filter(**kwargs)