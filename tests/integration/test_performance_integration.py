import pytest
import time
from caspyorm.core.fields import Integer, Text
from caspyorm.core.model import Model
from caspyorm.core.connection import get_session, connect
from caspyorm.utils.schema import create_table
from caspyorm.types.batch import BatchQuery

KEYSPACE = "nyc_data"

class PerfModel(Model):
    __table_name__ = "perf_integration_test"
    id = Integer(primary_key=True, required=True)
    name = Text()

@pytest.fixture(scope="module", autouse=True)
def setup_perf_table(db_connection):
    # Garante conexão ativa
    connect(contact_points=["172.18.0.2"], keyspace=KEYSPACE, port=9042)
    session = get_session()
    create_table(session, PerfModel)
    yield

def test_batch_insert_performance(db_connection):
    N = 1000
    batch_size = 100
    t0 = time.time()
    for start in range(0, N, batch_size):
        end = min(start + batch_size, N)
        with BatchQuery():
            for i in range(start, end):
                PerfModel(id=i, name=f"Nome {i}").save()
    t1 = time.time()
    print(f"Tempo para inserir {N} registros em batches de {batch_size}: {t1-t0:.2f}s")
    assert PerfModel.get(id=0) is not None

def test_read_performance(db_connection):
    t0 = time.time()
    objs = list(PerfModel.all().all())
    t1 = time.time()
    print(f"Tempo para ler todos os registros: {t1-t0:.2f}s, total={len(objs)}")
    assert len(objs) >= 1000

def test_delete_performance(db_connection):
    t0 = time.time()
    with BatchQuery():
        for i in range(1000):
            obj = PerfModel.get(id=i)
            if obj:
                obj.delete()
    t1 = time.time()
    print(f"Tempo para deletar 1000 registros em batch: {t1-t0:.2f}s")
    assert PerfModel.get(id=0) is None 