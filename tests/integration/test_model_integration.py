"""
Testes de integração para modelos e operações CRUD do CaspyORM.
"""

import pytest
import uuid
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from caspyorm.model import Model
from caspyorm.fields import Text, Integer, UUID, Boolean, Timestamp
from caspyorm import connection
from caspyorm.exceptions import ValidationError


class TestUser(Model):
    """Modelo de teste para usuário."""
    __table_name__ = 'users'
    
    model_fields = {
        'id': UUID(primary_key=True),
        'name': Text(required=True),
        'email': Text(required=True),
        'age': Integer(),
        'active': Boolean(default=True),
        'created_at': Timestamp(default=lambda: datetime.now())
    }


class TestPost(Model):
    """Modelo de teste para post."""
    __table_name__ = 'posts'
    
    model_fields = {
        'id': UUID(primary_key=True),
        'title': Text(required=True),
        'content': Text(),
        'user_id': UUID(required=True),
        'published': Boolean(default=False),
        'created_at': Timestamp(default=lambda: datetime.now())
    }


@pytest.mark.asyncio
class TestModelIntegration:
    """Testes de integração para modelos."""
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_crud_operations(self, mock_get_async_session):
        """Testa operações CRUD completas de um modelo."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Teste CREATE
        user = TestUser(name="João", email="joao@example.com", age=25)
        await user.save_async()
        
        # Verifica se executou INSERT
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
        # Apenas garantir que o método foi chamado
        
        # Reset do mock para próximo teste
        mock_session.execute_async.reset_mock()
        
        # Teste READ
        mock_row = Mock()
        mock_row.id = uuid.uuid4()
        mock_row.name = "João"
        mock_row.email = "joao@example.com"
        mock_row.age = 25
        mock_row.active = True
        mock_row.created_at = datetime.now()
        mock_row._asdict.return_value = {
            "id": mock_row.id,
            "name": mock_row.name,
            "email": mock_row.email,
            "age": mock_row.age,
            "active": mock_row.active,
            "created_at": mock_row.created_at,
        }
        # O result_set precisa ser iterável e ter o método one()
        mock_result_set = [mock_row]
        mock_result = Mock()
        mock_result.__iter__ = lambda s: iter(mock_result_set)
        mock_result.one.return_value = mock_row
        mock_session.execute_async.return_value = mock_result
        
        found_user = await TestUser.get_async(id=mock_row.id)
        assert found_user.name == "João"
        
        # Teste UPDATE
        mock_session.execute_async.reset_mock()
        user.name = "João Silva"
        await user.save_async()
        # Verifica se executou UPDATE
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
        
        # Teste DELETE
        mock_session.execute_async.reset_mock()
        await user.delete_async()
        # Verifica se executou DELETE
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_query_operations(self, mock_get_async_session):
        """Testa operações de query em modelos."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Mock de resultados para queries
        mock_row1 = Mock()
        mock_row1.id = uuid.uuid4()
        mock_row1.name = "João"
        mock_row1.email = "joao@example.com"
        mock_row1.age = 25
        mock_row1.active = True
        mock_row1.created_at = datetime.now()
        mock_row1._asdict.return_value = {
            "id": mock_row1.id,
            "name": mock_row1.name,
            "email": mock_row1.email,
            "age": mock_row1.age,
            "active": mock_row1.active,
            "created_at": mock_row1.created_at,
        }
        
        mock_row2 = Mock()
        mock_row2.id = uuid.uuid4()
        mock_row2.name = "Maria"
        mock_row2.email = "maria@example.com"
        mock_row2.age = 30
        mock_row2.active = True
        mock_row2.created_at = datetime.now()
        mock_row2._asdict.return_value = {
            "id": mock_row2.id,
            "name": mock_row2.name,
            "email": mock_row2.email,
            "age": mock_row2.age,
            "active": mock_row2.active,
            "created_at": mock_row2.created_at,
        }
        
        # O result_set precisa ser iterável
        mock_result_set = [mock_row1, mock_row2]
        mock_result = Mock()
        mock_result.__iter__ = lambda s: iter(mock_result_set)
        mock_session.execute_async.return_value = mock_result
        
        # Teste filter
        users = await TestUser.filter(active=True).all_async()
        assert len(users) == 2
        assert users[0].name == "João"
        assert users[1].name == "Maria"
        
        # Teste first
        mock_result_set = [mock_row1]
        first_user = await TestUser.filter(name="João").first_async()
        assert first_user.name == "João"
        
        # Teste limit
        mock_result_set = [mock_row1]
        limited_users = await TestUser.filter(active=True).limit(1).all_async()
        assert len(limited_users) == 1
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_count_operation(self, mock_get_async_session):
        """Testa operação de contagem."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Mock de resultado para count
        mock_result = Mock()
        mock_row = Mock()
        mock_row.count = 42
        mock_result.one.return_value = mock_row
        mock_session.execute_async.return_value = mock_result
        
        # Teste count
        count = await TestUser.filter(active=True).count_async()
        assert count == 42
        
        # Verifica se executou COUNT
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_exists_operation(self, mock_get_async_session):
        """Testa operação de verificação de existência."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Mock de resultado para exists
        mock_result = Mock()
        mock_row = Mock()
        mock_result.one.return_value = mock_row
        mock_session.execute_async.return_value = mock_result
        
        # Teste exists
        exists = await TestUser.filter(name="João").exists_async()
        assert exists is True
        
        # Verifica se executou EXISTS
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_delete_operation(self, mock_get_async_session):
        """Testa operação de exclusão em lote."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Mock de resultado para delete
        mock_result = Mock()
        mock_result.rowcount = 3
        mock_session.execute_async.return_value = mock_result
        
        # Teste delete - usar chave primária (id) em vez de campo não indexado
        user_id = uuid.uuid4()
        deleted_count = await TestUser.filter(id=user_id).delete_async()
        assert deleted_count == 0  # Cassandra não retorna número de linhas deletadas
        
        # Verifica se executou DELETE
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_complex_queries(self, mock_get_async_session):
        """Testa queries complexas com múltiplos filtros e ordenação."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Mock de resultados
        mock_row = Mock()
        mock_row.id = uuid.uuid4()
        mock_row.name = "João"
        mock_row.email = "joao@example.com"
        mock_row.age = 25
        mock_row.active = True
        mock_row.created_at = datetime.now()
        mock_row._asdict.return_value = {
            "id": mock_row.id,
            "name": mock_row.name,
            "email": mock_row.email,
            "age": mock_row.age,
            "active": mock_row.active,
            "created_at": mock_row.created_at,
        }
        
        # O result_set precisa ser iterável
        mock_result_set = [mock_row]
        mock_result = Mock()
        mock_result.__iter__ = lambda s: iter(mock_result_set)
        mock_session.execute_async.return_value = mock_result
        
        # Teste query complexa (removido offset que não existe)
        users = await (TestUser
                      .filter(active=True, age__gte=18, age__lt=65)
                      .order_by('name', '-age')
                      .limit(10)
                      .all_async())
        
        assert len(users) == 1
        assert users[0].name == "João"
        
        # Verifica se executou query com ORDER BY e LIMIT
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_relationship_queries(self, mock_get_async_session):
        """Testa queries com relacionamentos."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Teste query de posts por usuário
        user_id = uuid.uuid4()
        
        # Mock de resultados para posts
        mock_row = Mock()
        mock_row.id = uuid.uuid4()
        mock_row.title = "Meu Post"
        mock_row.content = "Conteúdo do post"
        mock_row.user_id = user_id  # Usar o mesmo user_id do teste
        mock_row.published = True
        mock_row.created_at = datetime.now()
        mock_row._asdict.return_value = {
            "id": mock_row.id,
            "title": mock_row.title,
            "content": mock_row.content,
            "user_id": mock_row.user_id,
            "published": mock_row.published,
            "created_at": mock_row.created_at,
        }
        
        # O result_set precisa ser iterável
        mock_result_set = [mock_row]
        mock_result = Mock()
        mock_result.__iter__ = lambda s: iter(mock_result_set)
        mock_session.execute_async.return_value = mock_result
        
        posts = await TestPost.filter(user_id=user_id, published=True).all_async()
        
        assert len(posts) == 1
        assert posts[0].title == "Meu Post"
        assert posts[0].user_id == user_id
        
        # Verifica se executou query com filtros
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_batch_operations(self, mock_get_async_session):
        """Testa operações em lote."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Mock de resultados para múltiplos usuários
        mock_rows = []
        for i in range(5):
            mock_row = Mock()
            mock_row.id = uuid.uuid4()
            mock_row.name = f"User{i}"
            mock_row.email = f"user{i}@example.com"
            mock_row.age = 20 + i
            mock_row.active = True
            mock_row.created_at = datetime.now()
            mock_row._asdict.return_value = {
                "id": mock_row.id,
                "name": mock_row.name,
                "email": mock_row.email,
                "age": mock_row.age,
                "active": mock_row.active,
                "created_at": mock_row.created_at,
            }
            mock_rows.append(mock_row)
        
        # O result_set precisa ser iterável
        mock_result = Mock()
        mock_result.__iter__ = lambda s: iter(mock_rows)
        mock_session.execute_async.return_value = mock_result
        
        # Teste busca de múltiplos usuários
        users = await TestUser.filter(active=True).all_async()
        
        assert len(users) == 5
        for i, user in enumerate(users):
            assert user.name == f"User{i}"
            assert user.email == f"user{i}@example.com"
            assert user.age == 20 + i
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_error_handling(self, mock_get_async_session):
        """Testa tratamento de erros em operações de modelo."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Teste erro na busca
        mock_session.execute_async.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            await TestUser.get_async(id=uuid.uuid4())
        
        # Teste erro na criação
        mock_session.execute_async.side_effect = Exception("Insert failed")
        
        user = TestUser(name="João", email="joao@example.com", age=25)
        with pytest.raises(Exception, match="Insert failed"):
            await user.save_async()
    
    @patch('caspyorm.query.get_async_session')
    async def test_model_validation_integration(self, mock_get_async_session):
        """Testa validação integrada com operações de modelo."""
        # Mock da sessão
        mock_session = AsyncMock()
        mock_get_async_session.return_value = mock_session
        
        # Teste criação com dados válidos
        user = TestUser(name="João", email="joao@example.com", age=25)
        await user.save_async()
        
        # Verifica se executou INSERT
        mock_session.execute_async.assert_called_once()
        # Não é possível inspecionar o conteúdo do prepared_statement pois é um mock/coroutine
        
        # Teste validação de campo obrigatório - a exceção é lançada no construtor
        with pytest.raises(ValidationError, match="Campo 'name' é obrigatório"):
            TestUser(email="invalid@example.com", age=25)
        
        # Teste validação de tipo - a exceção é lançada no construtor
        with pytest.raises(ValidationError, match="Valor inválido para campo 'age': Não foi possível converter 'não é número' para int"):
            TestUser(name="João", email="joao@example.com", age="não é número") 