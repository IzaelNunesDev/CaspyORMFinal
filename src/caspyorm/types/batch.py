from cassandra.query import BatchStatement
from ..core.connection import get_session
import threading

# Thread-local para batch ativo
_batch_context = threading.local()

class BatchQuery:
    """
    Gerenciador de contexto para batch de operações Cassandra.
    Uso:
        with BatchQuery() as batch:
            ... # Model.save() etc
    """
    def __init__(self):
        self.statements = []  # Lista de (query, params)

    def add(self, query, params):
        self.statements.append((query, params))

    def __enter__(self):
        _batch_context.active = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.statements:
                session = get_session()
                batch = BatchStatement()
                for query, params in self.statements:
                    batch.add(query, params)
                session.execute(batch)
        finally:
            _batch_context.active = None

# Função utilitária para acessar o batch ativo
def get_active_batch():
    return getattr(_batch_context, 'active', None) 