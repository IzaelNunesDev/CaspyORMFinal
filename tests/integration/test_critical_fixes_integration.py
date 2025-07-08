"""
Testes de integração para as correções de prioridade crítica.
"""
import pytest
import asyncio
import tempfile
import os
from unittest.mock import Mock, patch
from caspyorm import Model, connection
from caspyorm.core.fields import UUID, Text, Integer
from caspyorm.types.batch import BatchQuery


class TestUser(Model):
    """Modelo de teste para validação das correções."""
    __table_name__ = "test_users"
    __primary_key__ = ("id",)
    __partition_key__ = ("id",)
    
    id: UUID = UUID(partition_key=True)
    name: Text = Text()
    age: Integer = Integer()


class TestCriticalFixesIntegration:
    """Testa as correções críticas em cenários de integração."""
    
    @pytest.fixture
    def temp_keyspace(self):
        """Cria um keyspace temporário para testes."""
        return f"test_critical_fixes_{os.getpid()}"
    
    @pytest.mark.asyncio
    async def test_async_connection_does_not_block_event_loop(self, temp_keyspace):
        """Testa se a conexão assíncrona não bloqueia o loop de eventos."""
        # Mock do cluster para simular conexão
        mock_cluster = Mock()
        mock_session = Mock()
        mock_session.keyspace = temp_keyspace
        mock_cluster.connect.return_value = mock_session
        
        with patch('caspyorm.core.connection.Cluster', return_value=mock_cluster), \
             patch('asyncio.to_thread') as mock_to_thread:
            
            mock_to_thread.return_value = mock_session
            
            # Simula múltiplas tarefas concorrentes
            async def connect_task():
                await connection.connect_async(
                    contact_points=['127.0.0.1'],
                    port=9042,
                    keyspace=temp_keyspace
                )
            
            # Executa múltiplas conexões concorrentes
            tasks = [connect_task() for _ in range(3)]
            await asyncio.gather(*tasks)
            
            # Verifica se asyncio.to_thread foi chamado para cada conexão
            assert mock_to_thread.call_count == 3
    
    @pytest.mark.asyncio
    async def test_batch_contextvars_thread_safety(self, temp_keyspace):
        """Testa se o sistema de batch é thread-safe com contextvars."""
        from caspyorm.types.batch import get_active_batch
        
        # Simula múltiplas tarefas usando batches
        async def batch_task(task_id):
            with BatchQuery() as batch:
                # Simula operações de batch
                batch.add(f"INSERT INTO test (id, task) VALUES (?, ?)", (task_id, f"task_{task_id}"))
                # Verifica se o contexto está isolado
                active_batch = get_active_batch()
                assert active_batch is batch
                assert active_batch is not None
                assert len(active_batch.statements) == 1
                return task_id
        
        # Executa múltiplas tarefas concorrentes
        tasks = [batch_task(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        # Verifica se todas as tarefas completaram
        assert results == [0, 1, 2, 3, 4]
        
        # Verifica se o contexto foi limpo
        assert get_active_batch() is None
    
    @pytest.mark.asyncio
    async def test_template_loading_with_importlib_resources(self):
        """Testa se o carregamento de template funciona com importlib.resources."""
        import importlib.resources
        from caspyorm_cli.main import migrate_new
        
        # Cria um diretório temporário para migrações
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock do diretório de migrações
            original_migrations_dir = os.environ.get('MIGRATIONS_DIR', 'migrations')
            os.environ['MIGRATIONS_DIR'] = temp_dir
            
            try:
                # Verifica se o template pode ser carregado
                template_content = importlib.resources.files('caspyorm_cli.templates').joinpath('migration_template.py.j2').read_text(encoding='utf-8')
                
                # Verifica se o template tem o conteúdo esperado
                assert '{name}' in template_content
                assert '{created_at}' in template_content
                assert 'async def upgrade():' in template_content
                assert 'async def downgrade():' in template_content
                
            finally:
                # Restaura o diretório original
                if original_migrations_dir != 'migrations':
                    os.environ['MIGRATIONS_DIR'] = original_migrations_dir
                else:
                    os.environ.pop('MIGRATIONS_DIR', None)
    
    @pytest.mark.asyncio
    async def test_schema_sync_uses_official_api(self, temp_keyspace):
        """Testa se o schema_sync usa a API oficial do driver."""
        from caspyorm._internal.schema_sync import get_cassandra_table_schema
        
        # Mock da sessão com metadados completos
        mock_session = Mock()
        mock_cluster = Mock()
        mock_metadata = Mock()
        mock_keyspace_meta = Mock()
        mock_table_meta = Mock()
        
        # Configuração dos mocks
        mock_session.cluster = mock_cluster
        mock_cluster.metadata = mock_metadata
        mock_metadata.keyspaces = {temp_keyspace: mock_keyspace_meta}
        mock_keyspace_meta.tables = {'test_users': mock_table_meta}
        
        # Mock das colunas
        mock_id_column = Mock()
        mock_id_column.name = 'id'
        mock_id_column.cql_type = 'uuid'
        mock_id_column.kind = 'partition_key'
        
        mock_name_column = Mock()
        mock_name_column.name = 'name'
        mock_name_column.cql_type = 'text'
        mock_name_column.kind = 'regular'
        
        mock_age_column = Mock()
        mock_age_column.name = 'age'
        mock_age_column.cql_type = 'int'
        mock_age_column.kind = 'regular'
        
        mock_table_meta.primary_key = [mock_id_column]
        mock_table_meta.partition_key = [mock_id_column]
        mock_table_meta.clustering_key = []
        mock_table_meta.columns = {
            'id': mock_id_column,
            'name': mock_name_column,
            'age': mock_age_column
        }
        
        # Testa a função
        result = get_cassandra_table_schema(mock_session, temp_keyspace, 'test_users')
        
        # Verifica se o resultado está correto
        assert result is not None
        assert 'id' in result['fields']
        assert 'name' in result['fields']
        assert 'age' in result['fields']
        assert result['primary_keys'] == ['id']
        assert result['partition_keys'] == ['id']
        assert result['clustering_keys'] == []
        
        # Verifica os tipos dos campos
        assert result['fields']['id']['type'] == 'uuid'
        assert result['fields']['name']['type'] == 'text'
        assert result['fields']['age']['type'] == 'int'
    
    @pytest.mark.asyncio
    async def test_prepare_async_uses_to_thread(self, temp_keyspace):
        """Testa se prepare_async usa asyncio.to_thread corretamente."""
        mock_session = Mock()
        mock_prepared = Mock()
        connection.async_session = mock_session
        connection._is_async_connected = True
        
        with patch('asyncio.to_thread') as mock_to_thread:
            mock_to_thread.return_value = mock_prepared
            
            # Testa prepare_async
            result = await connection.prepare_async("SELECT * FROM test_users WHERE id = ?")
            
            # Verifica se asyncio.to_thread foi chamado
            mock_to_thread.assert_called_once_with(mock_session.prepare, "SELECT * FROM test_users WHERE id = ?")
            assert result == mock_prepared
            
            # Testa cache
            result2 = await connection.prepare_async("SELECT * FROM test_users WHERE id = ?")
            assert result2 == mock_prepared
            # Não deve chamar to_thread novamente
            assert mock_to_thread.call_count == 1
    
    @pytest.mark.asyncio
    async def test_disconnect_async_uses_to_thread(self, temp_keyspace):
        """Testa se disconnect_async usa asyncio.to_thread para cluster.shutdown."""
        mock_cluster = Mock()
        connection.cluster = mock_cluster
        connection.session = Mock()
        connection._is_async_connected = True
        
        with patch('asyncio.to_thread') as mock_to_thread:
            await connection.disconnect_async()
            
            # Verifica se asyncio.to_thread foi chamado para cluster.shutdown
            mock_to_thread.assert_called_once_with(mock_cluster.shutdown)
            assert connection.cluster is None
            assert connection.session is None
            assert connection._is_async_connected is False


class TestConcurrencySafety:
    """Testa a segurança de concorrência das correções."""
    
    @pytest.mark.asyncio
    async def test_multiple_batches_concurrent(self):
        """Testa múltiplos batches executando concorrentemente."""
        from caspyorm.types.batch import get_active_batch
        
        results = []
        
        async def concurrent_batch_task(task_id):
            with BatchQuery() as batch:
                batch.add(f"INSERT INTO test (id, value) VALUES (?, ?)", (task_id, f"value_{task_id}"))
                # Verifica se o contexto está isolado
                active_batch = get_active_batch()
                assert active_batch is batch
                assert active_batch is not None
                results.append((task_id, len(active_batch.statements)))
                await asyncio.sleep(0.01)  # Simula trabalho
                return task_id
        
        # Executa 10 tarefas concorrentes
        tasks = [concurrent_batch_task(i) for i in range(10)]
        await asyncio.gather(*tasks)
        
        # Verifica se todas as tarefas tiveram seu próprio contexto
        assert len(results) == 10
        for task_id, statement_count in results:
            assert statement_count == 1
        
        # Verifica se o contexto foi limpo
        assert get_active_batch() is None
    
    @pytest.mark.asyncio
    async def test_connection_pool_concurrent_access(self):
        """Testa acesso concorrente ao pool de conexões."""
        mock_cluster = Mock()
        mock_session = Mock()
        mock_cluster.connect.return_value = mock_session
        
        with patch('caspyorm.core.connection.Cluster', return_value=mock_cluster), \
             patch('asyncio.to_thread') as mock_to_thread:
            
            mock_to_thread.return_value = mock_session
            
            async def connection_task(task_id):
                await connection.connect_async(
                    contact_points=['127.0.0.1'],
                    port=9042
                )
                # Simula uso da conexão
                await asyncio.sleep(0.01)
                await connection.disconnect_async()
                return task_id
            
            # Executa múltiplas conexões concorrentes
            tasks = [connection_task(i) for i in range(5)]
            results = await asyncio.gather(*tasks)
            
            # Verifica se todas as tarefas completaram
            assert results == [0, 1, 2, 3, 4]
            
            # Verifica se asyncio.to_thread foi chamado para cada operação
            # 5 conexões + 5 desconexões = 10 chamadas
            assert mock_to_thread.call_count == 10 