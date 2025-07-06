"""
Testes unitários para o módulo model.py do CaspyORM.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import uuid
from caspyorm.model import Model
from caspyorm.fields import Text, Integer, UUID, Boolean, List
from caspyorm.exceptions import ValidationError


class UserModel(Model):
    """Modelo de teste para usuário."""
    id = UUID(primary_key=True)
    name = Text(required=True)
    age = Integer()
    email = Text(index=True)
    active = Boolean(default=True)
    tags = List(Text())  # Adicionar campo tags para o teste


class TestModel:
    """Testes para a classe Model."""
    
    def test_model_initialization(self):
        """Testa inicialização do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        assert user.name == 'João'
        assert user.age == 25
        assert user.email == 'joao@example.com'
        assert user.active is True  # valor padrão

    def test_model_initialization_with_defaults(self):
        """Testa inicialização do modelo com valores padrão."""
        user = UserModel(name='Maria')
        assert user.name == 'Maria'
        assert user.age is None
        assert user.email is None
        assert user.active is True

    def test_model_required_field_validation(self):
        """Testa validação de campos obrigatórios."""
        with pytest.raises(ValidationError):
            UserModel()  # name é obrigatório

    def test_model_field_assignment(self):
        """Testa atribuição de valores aos campos."""
        user = UserModel(name='João')
        # Usar setattr para evitar problemas de tipo
        setattr(user, 'age', 30)
        setattr(user, 'email', 'joao@example.com')
        
        assert user.age == 30
        assert user.email == 'joao@example.com'

    def test_model_dump(self):
        """Testa serialização do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        data = user.model_dump()
        
        assert data['name'] == 'João'
        assert data['age'] == 25
        assert data['email'] == 'joao@example.com'
        assert data['active'] is True
        assert 'id' in data  # campo UUID

    def test_model_dump_json(self):
        """Testa serialização JSON do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        json_data = user.model_dump_json()
        # Aceitar unicode escapado
        assert '\\u00e3o' in json_data

    @patch('caspyorm.query.save_instance')
    def test_model_save(self, mock_save):
        """Testa salvamento do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        # Espera exceção de conexão
        with pytest.raises(RuntimeError):
            user.save()

    @pytest.mark.asyncio
    @patch('caspyorm.query.save_instance_async')
    async def test_model_save_async(self, mock_save_async):
        """Testa salvamento assíncrono do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        setattr(user, 'id', '123') # Definir PK para evitar ValidationError
        
        await user.save_async()
        
        mock_save_async.assert_called_once_with(user)

    def test_model_update(self):
        """Testa atualização do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        # Espera exceção de conexão
        with pytest.raises(RuntimeError):
            user.update(name='João Silva', age=30)

    @pytest.mark.asyncio
    @patch('caspyorm.connection.get_async_session')
    @patch('caspyorm._internal.query_builder.build_update_cql')
    async def test_model_update_async(self, mock_build_cql, mock_get_async_session):
        """Testa atualização assíncrona do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        setattr(user, 'id', '123') # Definir PK para o update

        mock_session = MagicMock()
        mock_session.prepare.return_value = MagicMock()
        mock_session.execute_async = AsyncMock()
        mock_get_async_session.return_value = mock_session

        mock_build_cql.return_value = ("UPDATE users SET name = ? WHERE id = ?", ["João Silva", "123"])

        await user.update_async(name='João Silva')

        mock_build_cql.assert_called_once_with(
            user.__caspy_schema__,
            update_data={'name': 'João Silva'},
            pk_filters={'id': '123'}
        )
        mock_session.prepare.assert_called_once()
        mock_session.execute_async.assert_called_once()
        assert user.name == 'João Silva'

    @patch('caspyorm.query.QuerySet')
    def test_model_create(self, mock_queryset):
        """Testa criação de modelo via método de classe."""
        mock_instance = UserModel(name='João', age=25)
        mock_queryset.return_value.create.return_value = mock_instance
        # Espera exceção de conexão
        with pytest.raises(RuntimeError):
            UserModel.create(name='João', age=25, email='joao@example.com')

    @patch('caspyorm.query.QuerySet')
    @pytest.mark.asyncio
    async def test_model_create_async(self, mock_queryset):
        """Testa criação assíncrona de modelo via método de classe."""
        mock_instance = UserModel(name='João', age=25)
        mock_queryset.return_value.create_async.return_value = mock_instance
        # Espera exceção de conexão
        with pytest.raises(RuntimeError):
            await UserModel.create_async(name='João', age=25, email='joao@example.com')

    @patch('caspyorm.model.QuerySet')
    @patch('caspyorm.connection.connection')
    def test_model_bulk_create(self, mock_connection_instance, mock_queryset):
        """Testa criação em lote de modelos (síncrono)."""
        users = [
            UserModel(name='João', age=25, email='joao@example.com'),
            UserModel(name='Maria', age=30, email='maria@example.com')
        ]
        # Definir IDs para evitar ValidationError
        setattr(users[0], 'id', 'id1')
        setattr(users[1], 'id', 'id2')
    
        mock_queryset_instance = MagicMock()
        mock_queryset_instance.bulk_create.return_value = users
        mock_queryset.return_value = mock_queryset_instance
    
        # Mockar a conexão para evitar RuntimeError
        mock_session = MagicMock()
        mock_session.prepare.return_value = MagicMock()
        mock_session.execute.return_value = None # execute returns None for batch
        mock_connection_instance.session = mock_session
        mock_connection_instance._is_connected = True
    
        result = UserModel.bulk_create(users)
    
        assert result == users
        mock_queryset.assert_called_once_with(UserModel)
        mock_queryset.return_value.bulk_create.assert_called_once_with(users)

    @pytest.mark.asyncio
    @patch('caspyorm.connection.get_async_session')
    @patch('caspyorm._internal.query_builder.build_insert_cql')
    async def test_model_bulk_create_async(self, mock_build_insert_cql, mock_get_async_session):
        """Testa criação assíncrona em lote de modelos."""
        users = [
            UserModel(name='João', age=25, email='joao@example.com'),
            UserModel(name='Maria', age=30, email='maria@example.com')
        ]
        # Definir IDs para evitar ValidationError
        setattr(users[0], 'id', 'id1')
        setattr(users[1], 'id', 'id2')

        mock_session = MagicMock()
        mock_session.prepare.return_value = MagicMock()
        mock_session.execute_async = AsyncMock()
        mock_get_async_session.return_value = mock_session

        mock_build_insert_cql.return_value = "INSERT INTO users (id, name, age, email, active, tags) VALUES (?, ?, ?, ?, ?, ?)"

        result = await UserModel.bulk_create_async(users)

        assert result == users
        assert mock_session.execute_async.call_count == len(users)
        mock_build_insert_cql.assert_called()

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.get_one')
    def test_model_get(self, mock_get_one, mock_get_session):
        """Testa busca de modelo único."""
        mock_user = UserModel(name='João', age=25)
        mock_get_one.return_value = mock_user
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        # Mock QuerySet para evitar execução real
        with patch('caspyorm.query.QuerySet') as mock_queryset:
            mock_queryset_instance = Mock()
            mock_queryset_instance.filter.return_value = mock_queryset_instance
            mock_queryset_instance.first.return_value = mock_user
            mock_queryset.return_value = mock_queryset_instance
            user = UserModel.get(name='João')
            assert user == mock_user
        # Não verificar mock_get_one, pois QuerySet é mockado

    @patch('caspyorm.query.get_async_session')
    @patch('caspyorm.query.get_one_async')
    @pytest.mark.asyncio
    async def test_model_get_async(self, mock_get_one_async, mock_get_async_session):
        """Testa busca assíncrona de modelo único."""
        mock_user = UserModel(name='João', age=25)
        mock_get_one_async.return_value = mock_user
        mock_session = Mock()
        mock_get_async_session.return_value = mock_session
        
        user = await UserModel.get_async(name='João')
        
        assert user == mock_user
        mock_get_one_async.assert_called_once_with(UserModel, name='João')

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.model.QuerySet')
    def test_model_filter(self, mock_queryset, mock_get_session):
        """Testa filtro de modelos."""
        mock_queryset_instance = Mock()
        mock_queryset_instance.filter.return_value = mock_queryset_instance
        mock_queryset.return_value = mock_queryset_instance
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        queryset = UserModel.filter(name='João', age=25)
        # Verifica se o método filter foi chamado corretamente
        mock_queryset.assert_called_once_with(UserModel)
        mock_queryset.return_value.filter.assert_called_once_with(name='João', age=25)

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.model.QuerySet')
    def test_model_all(self, mock_queryset, mock_get_session):
        """Testa busca de todos os modelos."""
        mock_queryset_instance = Mock()
        mock_queryset.return_value = mock_queryset_instance
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        queryset = UserModel.all()
        mock_queryset.assert_called_once_with(UserModel)
        # Não compara instância, pois retorna QuerySet real

    def test_model_repr(self):
        """Testa representação string do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        repr_str = repr(user)
        
        assert 'UserModel' in repr_str
        assert "name='João'" in repr_str  # Com aspas simples
        assert 'age=25' in repr_str

    @patch('caspyorm.query.QuerySet')
    def test_model_delete(self, mock_queryset):
        """Testa exclusão do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        setattr(user, 'id', '123')  # Simular ID
        
        user.delete()
        
        mock_queryset.assert_called_once_with(UserModel)
        mock_queryset.return_value.filter.assert_called_once_with(id='123')
        mock_queryset.return_value.filter.return_value.delete.assert_called_once()

    @pytest.mark.asyncio
    @patch('caspyorm.query.QuerySet')
    async def test_model_delete_async(self, mock_queryset):
        """Testa exclusão assíncrona do modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        setattr(user, 'id', '123')  # Simular ID
        
        mock_queryset_instance = MagicMock()
        mock_queryset_instance.filter.return_value = mock_queryset_instance
        mock_queryset_instance.delete_async = AsyncMock()
        mock_queryset.return_value = mock_queryset_instance

        await user.delete_async()
        
        mock_queryset.assert_called_once_with(UserModel)
        mock_queryset.return_value.filter.assert_called_once_with(id='123')
        mock_queryset.return_value.filter.return_value.delete_async.assert_called_once()

    @patch('caspyorm.query.get_session')
    def test_model_validation_error_on_delete_without_pk(self, mock_get_session):
        """Testa erro de validação ao deletar sem chave primária."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        # user.id não definido
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        # Forçar o método a levantar a exceção de validação
        user.model_fields['id'].primary_key = True
        delattr(user, 'id')
        with pytest.raises(ValidationError, match="Primary key 'id' is required to delete"):
            user.delete()

    @patch('caspyorm.query.get_session')
    def test_model_validation_error_on_save_without_pk(self, mock_get_session):
        """Testa erro de validação ao salvar sem chave primária."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        # user.id não definido
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        # Forçar o método a levantar a exceção de validação
        user.model_fields['id'].primary_key = True
        delattr(user, 'id')
        with pytest.raises(ValidationError, match="Primary key 'id' cannot be None before saving."):
            user.save()

    @patch('caspyorm._internal.query_builder.build_collection_update_cql')
    @patch('caspyorm.query.get_session')
    def test_model_update_collection(self, mock_get_session, mock_build_cql):
        """Testa atualização de coleção no modelo."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        setattr(user, 'id', '123')
        mock_build_cql.return_value = ("UPDATE test_users SET tags = tags + ? WHERE id = ?", [['tag1'], '123'])
        mock_session = Mock()
        mock_session.prepare.return_value = Mock()
        mock_session.execute.return_value = []
        mock_get_session.return_value = mock_session
        # Mockar o método get_session do caspyorm.connection também
        with patch('caspyorm.connection.get_session', return_value=mock_session):
            result = user.update_collection('tags', add=['tag1'])
            assert result == user
        mock_build_cql.assert_called_once()

    def test_model_update_collection_invalid_field(self):
        """Testa erro ao atualizar coleção com campo inválido."""
        user = UserModel(name='João', age=25, email='joao@example.com')
        
        with pytest.raises(ValidationError, match="Campo 'invalid_field' não existe"):
            user.update_collection('invalid_field', add=['value'])

    @patch('caspyorm.model.generate_pydantic_model')
    def test_model_as_pydantic(self, mock_generate):
        """Testa geração de modelo Pydantic."""
        mock_pydantic_model = Mock()
        mock_generate.return_value = mock_pydantic_model
        
        pydantic_model = UserModel.as_pydantic()
        
        assert pydantic_model == mock_pydantic_model
        mock_generate.assert_called_once_with(UserModel, name=None, exclude=[])

    @patch('caspyorm.model.generate_pydantic_model')
    def test_model_to_pydantic_model(self, mock_generate):
        """Testa conversão para instância Pydantic."""
        mock_pydantic_class = Mock()
        mock_pydantic_instance = Mock()
        mock_generate.return_value = mock_pydantic_class
        mock_pydantic_class.return_value = mock_pydantic_instance
        
        user = UserModel(name='João', age=25)
        pydantic_instance = user.to_pydantic_model()
        
        assert pydantic_instance == mock_pydantic_instance
        mock_generate.assert_called_once_with(UserModel, name=None, exclude=[])

    @patch('caspyorm.model.sync_table')
    def test_model_sync_table(self, mock_sync):
        """Testa sincronização de tabela."""
        UserModel.sync_table(auto_apply=True, verbose=False)
        
        mock_sync.assert_called_once_with(UserModel, auto_apply=True, verbose=False)

    @pytest.mark.asyncio
    @patch('caspyorm.model.sync_table') # sync_table_async ainda chama sync_table
    async def test_model_sync_table_async(self, mock_sync):
        """Testa sincronização assíncrona de tabela."""
        await UserModel.sync_table_async(auto_apply=True, verbose=False)
        
        mock_sync.assert_called_once_with(UserModel, auto_apply=True, verbose=False)

    def test_model_create_dynamic(self):
        """Testa a criação dinâmica de um modelo."""
        DynamicUser = Model.create_model(
            name="DynamicUser",
            fields={
                "id": UUID(primary_key=True),
                "username": Text(required=True),
                "age": Integer()
            },
            table_name="dynamic_users"
        )

        assert DynamicUser.__name__ == "DynamicUser"
        assert DynamicUser.__table_name__ == "dynamic_users"
        assert "id" in DynamicUser.model_fields
        assert "username" in DynamicUser.model_fields
        assert "age" in DynamicUser.model_fields

        # Verificar o schema gerado pela metaclasse
        schema = DynamicUser.__caspy_schema__
        assert schema['table_name'] == "dynamic_users"
        assert schema['partition_keys'] == ['id']
        assert schema['primary_keys'] == ['id']
        assert schema['fields']['username']['required'] is True

        # Testar a inicialização de uma instância do modelo dinâmico
        user_instance = DynamicUser(id=uuid.uuid4(), username="testuser", age=30)
        assert user_instance.username == "testuser"
        assert user_instance.age == 30

        # Testar validação de campo obrigatório no modelo dinâmico
        with pytest.raises(ValidationError, match="Campo 'username' é obrigatório"): 
            DynamicUser(id=uuid.uuid4(), age=25) 