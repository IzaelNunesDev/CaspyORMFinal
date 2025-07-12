import asyncio
from contextvars import ContextVar, Token
from typing import Optional

from cassandra.query import BatchStatement

from ..core.connection import get_async_session, get_session

# ContextVar para batch ativo (correção para asyncio)
_active_batch_context: ContextVar[Optional["BatchQuery"]] = ContextVar(
    "active_batch", default=None
)
# ContextVar para batch assíncrono
_active_async_batch_context: ContextVar[Optional["AsyncBatchQuery"]] = ContextVar(
    "active_async_batch", default=None
)


class BatchQuery:
    """
    Gerenciador de contexto para batch de operações Cassandra.
    Uso:
        with BatchQuery() as batch:
            ... # Model.save() etc
    """

    def __init__(self):
        self.statements = []  # Lista de (query, params)
        self.token: Optional[Token] = None

    def add(self, query, params):
        self.statements.append((query, params))

    def __enter__(self):
        self.token = _active_batch_context.set(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if not exc_type and self.statements:  # Apenas executa se não houve exceção
                session = get_session()
                batch = BatchStatement()
                for query, params in self.statements:
                    # Preparar a query se for string
                    if isinstance(query, str):
                        prepared = session.prepare(query)
                        batch.add(prepared, params)
                    else:
                        batch.add(query, params)
                session.execute(batch)
        finally:
            if self.token:
                _active_batch_context.reset(self.token)


# AsyncBatchQuery para uso em contextos assíncronos
class AsyncBatchQuery:
    """
    Gerenciador de contexto assíncrono para batch de operações Cassandra.
    Uso:
        async with AsyncBatchQuery() as batch:
            ... # await Model.save_async() etc
    """

    def __init__(self):
        self.statements = []  # Lista de (query, params)
        self.token: Optional[Token] = None

    def add(self, query, params):
        self.statements.append((query, params))

    async def __aenter__(self):
        self.token = _active_async_batch_context.set(self)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if not exc_type and self.statements:
                session = get_async_session()
                batch = BatchStatement()
                for query, params in self.statements:
                    batch.add(query, params)
                future = session.execute_async(batch)
                await asyncio.to_thread(future.result)
        finally:
            if self.token:
                _active_async_batch_context.reset(self.token)


# Função utilitária para acessar o batch ativo
def get_active_batch() -> Optional[BatchQuery]:
    return _active_batch_context.get()


# Função utilitária para acessar o batch assíncrono ativo
def get_active_async_batch() -> Optional[AsyncBatchQuery]:
    return _active_async_batch_context.get()
