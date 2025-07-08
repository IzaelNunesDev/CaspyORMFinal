import pytest
import asyncio
from src.caspyorm import Model
from src.caspyorm.core.fields import Text, Integer, UUID
from src.caspyorm.types.batch import BatchQuery
import uuid

class TestUser(Model):
    id: UUID = UUID(primary_key=True)
    name: Text = Text()
    age: Integer = Integer()

class TestBatchOperations:
    """Testa operações de batch no CaspyORM."""
    
    @pytest.fixture(scope="class")
    def setup_database(self):
        """Setup do banco de dados para os testes."""
        # Conectar ao Cassandra
        import asyncio
        from src.caspyorm import connection
        
        async def setup():
            await connection.connect_async(
                contact_points=["127.0.0.1"],
                keyspace="caspyorm_test"
            )
            # Sincronizar tabela
            await TestUser.sync_table_async(auto_apply=True, verbose=False)
        
        asyncio.run(setup())
        yield
        # Cleanup
        asyncio.run(connection.disconnect_async())
    
    def test_batch_insert_multiple_users(self, setup_database):
        """Testa inserção de múltiplos usuários em um único batch."""
        # Criar múltiplos usuários
        users = [
            TestUser(id=uuid.uuid4(), name="Alice", age=25),
            TestUser(id=uuid.uuid4(), name="Bob", age=30),
            TestUser(id=uuid.uuid4(), name="Charlie", age=35),
        ]
        
        # Executar inserções em batch
        with BatchQuery() as batch:
            for user in users:
                user.save()  # Deve ser adicionado ao batch, não executado imediatamente
        
        # Verificar se todos os usuários foram inseridos
        for user in users:
            saved_user = TestUser.get(id=user.id)
            assert saved_user is not None
            assert saved_user.name == user.name
            assert saved_user.age == user.age
    
    def test_batch_insert_with_mixed_operations(self, setup_database):
        """Testa batch com inserções e atualizações misturadas."""
        # Criar usuário inicial
        user = TestUser(id=uuid.uuid4(), name="Initial", age=20)
        user.save()  # Salvar fora do batch
        
        # No batch: inserir novos usuários e atualizar o existente
        new_users = [
            TestUser(id=uuid.uuid4(), name="New1", age=25),
            TestUser(id=uuid.uuid4(), name="New2", age=30),
        ]
        
        with BatchQuery() as batch:
            # Inserir novos usuários
            for new_user in new_users:
                new_user.save()
            
            # Atualizar usuário existente
            user.name = "Updated"
            user.age = 25
            user.save()
        
        # Verificar se tudo foi executado corretamente
        # Novos usuários devem existir
        for new_user in new_users:
            saved_user = TestUser.get(id=new_user.id)
            assert saved_user is not None
            assert saved_user.name == new_user.name
        
        # Usuário atualizado deve ter os novos valores
        updated_user = TestUser.get(id=user.id)
        assert updated_user.name == "Updated"
        assert updated_user.age == 25
    
    def test_batch_empty_context(self, setup_database):
        """Testa batch vazio (não deve causar erros)."""
        with BatchQuery() as batch:
            pass  # Nenhuma operação
        
        # Não deve haver erros
    
    def test_batch_single_operation(self, setup_database):
        """Testa batch com apenas uma operação."""
        user = TestUser(id=uuid.uuid4(), name="Single", age=40)
        
        with BatchQuery() as batch:
            user.save()
        
        # Verificar se foi inserido
        saved_user = TestUser.get(id=user.id)
        assert saved_user is not None
        assert saved_user.name == "Single"
    
    def test_batch_large_number_of_operations(self, setup_database):
        """Testa batch com um grande número de operações."""
        users = []
        for i in range(50):  # 50 usuários
            users.append(TestUser(
                id=uuid.uuid4(),
                name=f"User{i}",
                age=20 + i
            ))
        
        with BatchQuery() as batch:
            for user in users:
                user.save()
        
        # Verificar se todos foram inseridos
        for user in users:
            saved_user = TestUser.get(id=user.id)
            assert saved_user is not None
            assert saved_user.name == user.name
            assert saved_user.age == user.age 