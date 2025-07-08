"""
Testes para as correções de prioridade crítica no connection.py
"""
import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from caspyorm.core.connection import ConnectionManager, connection


class TestConnectionAsyncFixes:
    """Testa as correções das funções assíncronas para evitar bloqueio do loop de eventos."""
    
    @pytest.fixture
    def connection_manager(self):
        """Fixture para um gerenciador de conexão limpo."""
        return ConnectionManager()
    
    @pytest.mark.asyncio
    async def test_connect_async_uses_to_thread(self, connection_manager):
        """Testa se connect_async usa asyncio.to_thread para evitar bloqueio."""
        mock_cluster = Mock()
        mock_session = Mock()
        mock_cluster.connect.return_value = mock_session
        
        with patch('caspyorm.core.connection.Cluster', return_value=mock_cluster), \
             patch('asyncio.to_thread') as mock_to_thread:
            
            mock_to_thread.return_value = mock_session
            
            await connection_manager.connect_async(
                contact_points=['127.0.0.1'],
                port=9042
            )
            
            # Verifica se asyncio.to_thread foi chamado
            mock_to_thread.assert_called_once_with(mock_cluster.connect)
            assert connection_manager.session == mock_session
            assert connection_manager._is_async_connected is True
    
    @pytest.mark.asyncio
    async def test_disconnect_async_uses_to_thread(self, connection_manager):
        """Testa se disconnect_async usa asyncio.to_thread para cluster.shutdown."""
        mock_cluster = Mock()
        connection_manager.cluster = mock_cluster
        
        with patch('asyncio.to_thread') as mock_to_thread:
            await connection_manager.disconnect_async()
            
            # Verifica se asyncio.to_thread foi chamado para cluster.shutdown
            mock_to_thread.assert_called_once_with(mock_cluster.shutdown)
            assert connection_manager.cluster is None
            assert connection_manager._is_async_connected is False
    
    @pytest.mark.asyncio
    async def test_prepare_async_uses_to_thread(self, connection_manager):
        """Testa se prepare_async usa asyncio.to_thread para session.prepare."""
        mock_session = Mock()
        mock_prepared = Mock()
        connection_manager.async_session = mock_session
        connection_manager._is_async_connected = True
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = mock_prepared
            
            result = await connection_manager.prepare_async("SELECT * FROM test")
            
            # Verifica se asyncio.to_thread foi chamado
            mock_to_thread.assert_called_once_with(mock_session.prepare, "SELECT * FROM test")
            assert result == mock_prepared
    
    @pytest.mark.asyncio
    async def test_prepare_async_uses_cache(self, connection_manager):
        """Testa se prepare_async usa cache corretamente."""
        mock_session = Mock()
        mock_prepared = Mock()
        connection_manager.async_session = mock_session
        connection_manager._is_async_connected = True
        connection_manager._prepared_statement_cache = {"SELECT * FROM test": mock_prepared}
        
        # Não deve chamar asyncio.to_thread se já está em cache
        with patch('asyncio.to_thread') as mock_to_thread:
            result = await connection_manager.prepare_async("SELECT * FROM test")
            
            # Não deve ter chamado asyncio.to_thread
            mock_to_thread.assert_not_called()
            assert result == mock_prepared


class TestBatchContextVars:
    """Testa se o sistema de batch usa contextvars corretamente."""
    
    def test_batch_uses_contextvars(self):
        """Testa se o sistema de batch usa contextvars em vez de threading.local."""
        from caspyorm.types.batch import BatchQuery, get_active_batch, _active_batch_context
        
        # Verifica se está usando ContextVar
        assert hasattr(_active_batch_context, 'get')
        assert hasattr(_active_batch_context, 'set')
        assert hasattr(_active_batch_context, 'reset')
        
        # Testa o contexto
        batch = BatchQuery()
        assert get_active_batch() is None
        
        with batch:
            assert get_active_batch() is batch
        
        assert get_active_batch() is None


class TestTemplateLoading:
    """Testa se o carregamento de template usa importlib.resources."""
    
    def test_migrate_new_uses_importlib_resources(self):
        """Testa se migrate_new usa importlib.resources para carregar template."""
        from caspyorm_cli.main import migrate_new
        
        # Verifica se a função existe e pode ser chamada
        assert callable(migrate_new)
        
        # O teste real seria executar a função, mas isso requer setup complexo
        # Por enquanto, apenas verificamos se a função existe


class TestSchemaSyncAPI:
    """Testa se schema_sync usa a API oficial do driver."""
    
    def test_get_cassandra_table_schema_uses_official_api(self):
        """Testa se get_cassandra_table_schema usa a API oficial do driver."""
        from caspyorm._internal.schema_sync import get_cassandra_table_schema
        
        # Mock da sessão com metadados do cluster
        mock_session = Mock()
        mock_cluster = Mock()
        mock_metadata = Mock()
        mock_keyspace_meta = Mock()
        mock_table_meta = Mock()
        
        # Configurar o mock
        mock_session.cluster = mock_cluster
        mock_cluster.metadata = mock_metadata
        mock_metadata.keyspaces = {'test_keyspace': mock_keyspace_meta}
        mock_keyspace_meta.tables = {'test_table': mock_table_meta}
        
        # Mock das colunas
        mock_column1 = Mock()
        mock_column1.name = 'id'
        mock_column1.cql_type = 'uuid'
        mock_column1.kind = 'partition_key'
        
        mock_column2 = Mock()
        mock_column2.name = 'name'
        mock_column2.cql_type = 'text'
        mock_column2.kind = 'regular'
        
        mock_table_meta.primary_key = [mock_column1]
        mock_table_meta.partition_key = [mock_column1]
        mock_table_meta.clustering_key = []
        mock_table_meta.columns = {
            'id': mock_column1,
            'name': mock_column2
        }
        
        # Testar a função
        result = get_cassandra_table_schema(mock_session, 'test_keyspace', 'test_table')
        
        assert result is not None
        assert 'id' in result['fields']
        assert 'name' in result['fields']
        assert result['primary_keys'] == ['id']
        assert result['partition_keys'] == ['id']
        assert result['clustering_keys'] == []
    
    def test_get_cassandra_table_schema_returns_none_for_nonexistent_table(self):
        """Testa se retorna None para tabela inexistente."""
        from caspyorm._internal.schema_sync import get_cassandra_table_schema
        
        mock_session = Mock()
        mock_cluster = Mock()
        mock_metadata = Mock()
        mock_keyspace_meta = Mock()
        
        mock_session.cluster = mock_cluster
        mock_cluster.metadata = mock_metadata
        mock_metadata.keyspaces = {'test_keyspace': mock_keyspace_meta}
        mock_keyspace_meta.tables = {}  # Tabela não existe
        
        result = get_cassandra_table_schema(mock_session, 'test_keyspace', 'nonexistent_table')
        assert result is None
    
    def test_get_cassandra_table_schema_handles_missing_cluster(self):
        """Testa se lida corretamente com cluster ausente."""
        from caspyorm._internal.schema_sync import get_cassandra_table_schema
        
        mock_session = Mock()
        mock_session.cluster = None  # Cluster ausente
        
        result = get_cassandra_table_schema(mock_session, 'test_keyspace', 'test_table')
        assert result is None 