import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from src.caspyorm.core.query import QuerySet, _map_row_to_instance, save_instance, save_instance_async, get_one, get_one_async, filter_query
from src.caspyorm.core.model import Model
from src.caspyorm.core.fields import Text, Integer, UUID
import uuid
import asyncio
import concurrent.futures

# Define a dummy Model for testing
class DummyModel(Model):
    id: UUID = UUID(primary_key=True)
    name: Text = Text()
    age: Integer = Integer()

    class Meta:
        table_name = "dummy_table"
        keyspace = "test_keyspace"

# Mock the schema for query_builder
@pytest.fixture
def mock_schema():
    return {
        'table_name': 'dummy_table',
        'fields': {
            'id': {'type': 'uuid'},
            'name': {'type': 'text'},
            'age': {'type': 'int'},
        },
        'primary_keys': ['id'],
        'partition_keys': ['id'],
        'clustering_keys': [],
    }

@pytest.fixture
def mock_query_builder():
    with patch('src.caspyorm._internal.query_builder') as mock_qb:
        mock_qb.build_select_cql.return_value = ("SELECT * FROM dummy_table", [])
        mock_qb.build_count_cql.return_value = ("SELECT COUNT(*) FROM dummy_table", [])
        mock_qb.build_delete_cql.return_value = ("DELETE FROM dummy_table WHERE id = ?", ["mock_id"])
        yield mock_qb

@pytest.fixture
def mock_connection():
    with patch('src.caspyorm.core.connection.get_session') as mock_get_session:
        with patch('src.caspyorm.core.connection.get_async_session') as mock_get_async_session:
            with patch('src.caspyorm.core.connection.execute_cql_async') as mock_execute_cql_async:

                mock_session = MagicMock()
                mock_session.prepare.return_value = MagicMock()
                mock_session.execute.return_value = MagicMock(one=MagicMock(return_value=MagicMock(_asdict=lambda: {'id': '1', 'name': 'Test1', 'age': 10})))
                mock_get_session.return_value = mock_session

                mock_async_session = AsyncMock()
                mock_async_session.prepare.return_value = AsyncMock()
                mock_execute_cql_async.return_value = MagicMock(one=AsyncMock(return_value=MagicMock(_asdict=lambda: {'id': '1', 'name': 'Test1', 'age': 10})))
                mock_get_async_session.return_value = mock_async_session

                yield {
                    "get_session": mock_get_session,
                    "get_async_session": mock_get_async_session,
                    "execute_cql_async": mock_execute_cql_async,
                    "session": mock_session,
                    "async_session": mock_async_session,
                }

class TestQuerySet:

    def test_queryset_init(self):
        qs = QuerySet(DummyModel)
        assert qs.model_cls == DummyModel
        assert qs._filters == {}
        assert qs._limit is None
        assert qs._ordering == []
        assert qs._allow_filtering is False
        assert qs._result_cache is None

    def test_queryset_clone(self):
        qs = QuerySet(DummyModel)
        qs._filters = {"name": "test"}
        qs._limit = 5
        qs._ordering = ["name"]
        qs._allow_filtering = True

        cloned_qs = qs._clone()

        assert cloned_qs.model_cls == qs.model_cls
        assert cloned_qs._filters == qs._filters
        assert cloned_qs._limit == qs._limit
        assert cloned_qs._ordering == qs._ordering
        assert cloned_qs._allow_filtering == qs._allow_filtering
        assert cloned_qs._result_cache is None
        assert cloned_qs is not qs  # Ensure it's a new instance
        assert cloned_qs._filters is not qs._filters # Ensure filters are copied
        assert cloned_qs._ordering is not qs._ordering # Ensure ordering is copied

    @pytest.mark.asyncio
    async def test_execute_query_sync(self, mock_query_builder, mock_connection, mock_schema):
        qs = QuerySet(DummyModel)
        qs._execute_query_sync()

        mock_query_builder.build_select_cql.assert_called_once_with(
            DummyModel.__caspy_schema__,
            columns=None,
            filters={},
            limit=None,
            ordering=[],
            allow_filtering=False # Should be False by default
        )
        mock_connection["get_session"].assert_called_once()
        mock_connection["session"].prepare.assert_called_once_with("SELECT * FROM dummy_table")
        mock_connection["session"].execute.assert_called_once_with(mock_connection["session"].prepare.return_value, [])
        assert len(qs._result_cache) == 1
        assert isinstance(qs._result_cache[0], DummyModel)

    @pytest.mark.asyncio
    async def test_execute_query_async(self, mock_query_builder, mock_connection, mock_schema):
        qs = QuerySet(DummyModel)
        await qs._execute_query_async()

        mock_query_builder.build_select_cql.assert_called_once_with(
            DummyModel.__caspy_schema__,
            columns=None,
            filters={},
            limit=None,
            ordering=[],
            allow_filtering=False # Should be False by default
        )
        mock_connection["get_async_session"].assert_called_once()
        mock_connection["async_session"].prepare.assert_called_once_with("SELECT * FROM dummy_table")
        mock_connection["execute_cql_async"].assert_called_once_with(mock_connection["async_session"].prepare.return_value, [])
        assert len(qs._result_cache) == 1
        assert isinstance(qs._result_cache[0], DummyModel)

    def test_allow_filtering(self):
        qs = QuerySet(DummyModel)
        cloned_qs = qs.allow_filtering()
        assert cloned_qs._allow_filtering is True
        assert cloned_qs is not qs

    def test_filter(self):
        qs = QuerySet(DummyModel)
        cloned_qs = qs.filter(name="test", age__gt=10)
        assert cloned_qs._filters == {"name": "test", "age__gt": 10}
        assert cloned_qs is not qs

    def test_limit(self):
        qs = QuerySet(DummyModel)
        cloned_qs = qs.limit(10)
        assert cloned_qs._limit == 10
        assert cloned_qs is not qs

    def test_order_by(self):
        qs = QuerySet(DummyModel)
        cloned_qs = qs.order_by("name", "-age")
        assert cloned_qs._ordering == ["name", "-age"]
        assert cloned_qs is not qs

    @pytest.mark.asyncio
    async def test_all_sync(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        results = qs.all()
        assert len(results) == 1
        assert isinstance(results[0], DummyModel)
        mock_query_builder.build_select_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_all_async(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        results = await qs.all_async()
        assert len(results) == 1
        assert isinstance(results[0], DummyModel)
        mock_query_builder.build_select_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_first_sync(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        result = qs.first()
        assert isinstance(result, DummyModel)
        mock_query_builder.build_select_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_first_async(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        result = await qs.first_async()
        assert isinstance(result, DummyModel)
        mock_query_builder.build_select_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_count_sync(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        mock_connection["session"].execute.return_value = [MagicMock(count=5)]
        count = qs.count()
        assert count == 5
        mock_query_builder.build_count_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_count_async(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        mock_connection["execute_cql_async"].return_value = [MagicMock(count=5)]
        count = await qs.count_async()
        assert count == 5
        mock_query_builder.build_count_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_exists_sync(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        mock_connection["session"].execute.return_value = [MagicMock(id="1")]
        exists = qs.exists()
        assert exists is True
        mock_query_builder.build_select_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_exists_async(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        mock_connection["execute_cql_async"].return_value = [MagicMock(id="1")]
        exists = await qs.exists_async()
        assert exists is True
        mock_query_builder.build_select_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_sync(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        qs._filters = {"id": "123"}
        result = qs.delete()
        assert result == 0 # Cassandra doesn't return affected rows
        mock_query_builder.build_delete_cql.assert_called_once_with(
            DummyModel.__caspy_schema__,
            filters=qs._filters
        )
        mock_connection["session"].execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_async(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        qs._filters = {"id": "123"}
        result = await qs.delete_async()
        assert result == 0 # Cassandra doesn't return affected rows
        mock_query_builder.build_delete_cql.assert_called_once_with(
            DummyModel.__caspy_schema__,
            filters=qs._filters
        )
        mock_connection["execute_cql_async"].assert_called_once()

    @pytest.mark.asyncio
    async def test_page_sync(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        mock_result_set = MagicMock()
        mock_result_set.paging_state = "next_page_state"
        mock_result_set.__iter__.return_value = [MagicMock(_asdict=lambda: {'id': '1', 'name': 'Test1', 'age': 10})]
        mock_connection["session"].execute.return_value = mock_result_set
        
        results, next_state = qs.page(page_size=1)
        assert len(results) == 1
        assert next_state == "next_page_state"
        mock_query_builder.build_select_cql.assert_called_once()
        mock_connection["session"].prepare.assert_called_once()
        mock_connection["session"].prepare.return_value.bind.assert_called_once_with([])
        mock_connection["session"].prepare.return_value.bind.return_value.fetch_size = 1

    @pytest.mark.asyncio
    async def test_page_async(self, mock_query_builder, mock_connection):
        qs = QuerySet(DummyModel)
        mock_result_set = AsyncMock()
        mock_result_set.paging_state = "next_page_state_async"
        mock_result_set.__aiter__.return_value = [MagicMock(_asdict=lambda: {'id': '1', 'name': 'Test1', 'age': 10})]
        mock_connection["execute_cql_async"].return_value = mock_result_set
        
        results, next_state = await qs.page_async(page_size=1)
        assert len(results) == 1
        assert next_state == "next_page_state_async"
        mock_query_builder.build_select_cql.assert_called_once()
        mock_connection["async_session"].prepare.assert_called_once()
        mock_connection["async_session"].prepare.return_value.bind.assert_called_once_with([])
        mock_connection["async_session"].prepare.return_value.bind.return_value.fetch_size = 1

    @pytest.mark.asyncio
    async def test_bulk_create(self, mock_connection):
        # Mock the session.prepare to return a mock prepared statement
        mock_prepared_statement = MagicMock()
        mock_connection["session"].prepare.return_value = mock_prepared_statement

        # Mock the BatchStatement to allow checking its calls
        with patch('src.caspyorm.core.query.BatchStatement') as MockBatchStatement:
            mock_batch = MagicMock()
            mock_batch.clear.return_value = None
            mock_batch.__len__.side_effect = [0, 100, 0] # Simulate batch filling up and then clearing
            MockBatchStatement.return_value = mock_batch

            instances = [
                DummyModel(id=uuid.UUID("11111111-1111-1111-1111-111111111111"), name="User1", age=20),
                DummyModel(id=uuid.UUID("22222222-2222-2222-2222-222222222222"), name="User2", age=25),
            ]
            
            qs = QuerySet(DummyModel)
            created_instances = qs.bulk_create(instances)

            assert created_instances == instances
            mock_connection["session"].prepare.assert_called_once_with("INSERT INTO dummy_table (id, name, age) VALUES (?, ?, ?)")
            assert MockBatchStatement.called
            assert mock_batch.add.call_count == 2
            mock_connection["session"].execute.assert_called_once_with(mock_batch)

    def test_bulk_create_empty_list(self):
        qs = QuerySet(DummyModel)
        instances = qs.bulk_create([])
        assert instances == []

    def test_bulk_create_missing_pk(self, mock_connection):
        with patch('src.caspyorm.core.connection.get_session', return_value=mock_connection["session"]):
            qs = QuerySet(DummyModel)
            instances = [
                DummyModel(name="User1", age=20), # Missing ID
            ]
            with pytest.raises(ValueError, match="Primary key 'id' não pode ser nula em bulk_create."):
                qs.bulk_create(instances)


class TestModuleFunctions:

    @pytest.mark.asyncio
    async def test_save_instance(self, mock_connection):
        instance = DummyModel(id=uuid.UUID("11111111-1111-1111-1111-111111111111"), name="Test", age=10)
        mock_connection["get_session"].return_value = mock_connection["session"]
        
        save_instance(instance)
        
        mock_connection["session"].prepare.assert_called_once()
        mock_connection["session"].execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_instance_async(self, mock_connection):
        instance = DummyModel(id=uuid.UUID("11111111-1111-1111-1111-111111111111"), name="Test", age=10)
        mock_connection["get_async_session"].return_value = mock_connection["async_session"]
        
        await save_instance_async(instance)
        
        mock_connection["async_session"].prepare.assert_called_once()
        mock_connection["execute_cql_async"].assert_called_once()

    @pytest.mark.asyncio
    async def test_get_one(self, mock_query_builder, mock_connection):
        result = get_one(DummyModel, id=uuid.UUID("11111111-1111-1111-1111-111111111111"))
        assert isinstance(result, DummyModel)
        mock_query_builder.build_select_cql.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_one_async(self, mock_query_builder, mock_connection):
        result = await get_one_async(DummyModel, id=uuid.UUID("11111111-1111-1111-1111-111111111111"))
        assert isinstance(result, DummyModel)
        mock_query_builder.build_select_cql.assert_called_once()

    def test_filter_query(self):
        qs = filter_query(DummyModel, name="test")
        assert isinstance(qs, QuerySet)
        assert qs._filters == {"name": "test"}


class TestPreparedStatementCache:
    """Testa o cache transparente de prepared statements."""
    
    @pytest.mark.asyncio
    async def test_prepared_statement_cache_reuse(self):
        """Testa que prepared statements são reutilizados do cache."""
        # Mock da sessão e conexão
        mock_session = MagicMock()
        mock_prepared = MagicMock()
        # prepare_async retorna concurrent.futures.Future
        future_prepared = concurrent.futures.Future()
        future_prepared.set_result(mock_prepared)
        mock_session.prepare_async.return_value = future_prepared
        
        mock_result = MagicMock()
        mock_result._asdict.return_value = {'id': '11111111-1111-1111-1111-111111111111', 'name': 'test'}
        future_result = concurrent.futures.Future()
        future_result.set_result([mock_result])
        mock_session.execute_async.return_value = future_result
        
        # Mock da conexão global
        mock_connection = MagicMock()
        mock_connection._prepared_statement_cache = {}
        
        with patch('src.caspyorm.core.query.get_async_session', return_value=mock_session), \
             patch('src.caspyorm.core.connection.connection', mock_connection):
            
            # Criar QuerySet
            queryset = QuerySet(DummyModel)
            queryset._filters = {'name': 'test'}
            
            # Primeira execução - deve preparar o statement
            await queryset._execute_query_async()
            
            # Segunda execução - deve reutilizar do cache
            await queryset._execute_query_async()
            
            # Verificar que prepare_async foi chamado apenas uma vez
            assert mock_session.prepare_async.call_count == 1
            
            # Verificar que execute_async foi chamado duas vezes
            assert mock_session.execute_async.call_count == 2
            
            # Verificar que o cache foi populado
            assert len(mock_connection._prepared_statement_cache) == 1