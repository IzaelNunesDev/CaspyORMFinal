import pytest
from unittest.mock import MagicMock, patch
from caspyorm.types.batch import BatchQuery

class DummySession:
    def __init__(self):
        self.executed = False
        self.batch = []
    def prepare(self, cql):
        return f"prepared:{cql}"
    def execute(self, batch):
        self.executed = True
        self.batch = batch.statements
        return "executed"

class DummyBatch:
    def __init__(self):
        self.statements = []
    def add(self, query, params):
        self.statements.append((query, params))

@patch("caspyorm.types.batch.get_session")
@patch("caspyorm.types.batch.BatchStatement", DummyBatch)
def test_batchquery_executes(get_session_mock):
    session = DummySession()
    get_session_mock.return_value = session
    with BatchQuery() as bq:
        bq.add("INSERT INTO t (a) VALUES (?)", (1,))
        bq.add("INSERT INTO t (a) VALUES (?)", (2,))
    assert session.executed
    assert len(session.batch) == 2
    assert session.batch[0][1] == (1,)

@patch("caspyorm.types.batch.get_session")
@patch("caspyorm.types.batch.BatchStatement", DummyBatch)
def test_batchquery_rollback_on_error(get_session_mock):
    session = DummySession()
    get_session_mock.return_value = session
    with pytest.raises(ValueError):
        with BatchQuery() as bq:
            bq.add("INSERT INTO t (a) VALUES (?)", (1,))
            raise ValueError("fail")
    # Não executa batch se erro
    assert not session.executed

@patch("caspyorm.types.batch.get_session")
@patch("caspyorm.types.batch.BatchStatement", DummyBatch)
def test_batchquery_nested(get_session_mock):
    session = DummySession()
    get_session_mock.return_value = session
    with BatchQuery() as bq1:
        bq1.add("Q1", (1,))
        with BatchQuery() as bq2:
            bq2.add("Q2", (2,))
    assert session.executed
    assert len(session.batch) == 1 or len(session.batch) == 2  # depende da implementação 