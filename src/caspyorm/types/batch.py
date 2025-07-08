from cassandra.query import BatchStatement
from ..core.connection import get_session
from contextvars import ContextVar, Token
from typing import Optional

# ContextVar para batch ativo (correção para asyncio)
_active_batch_context: ContextVar[Optional["BatchQuery"]] = ContextVar("active_batch", default=None)

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
                    batch.add(query, params)
                session.execute(batch)
        finally:
            if self.token:
                _active_batch_context.reset(self.token)

# Função utilitária para acessar o batch ativo
def get_active_batch() -> Optional[BatchQuery]:
    return _active_batch_context.get() 