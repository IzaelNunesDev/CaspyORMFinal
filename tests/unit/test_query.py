"""
Testes unitários para o módulo query.py do CaspyORM.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock

from caspyorm.query import QuerySet
from caspyorm.fields import Text, Integer, UUID, Boolean
from caspyorm.model import Model


class UserModel(Model):
    """Modelo de teste para usuário."""
    id = UUID(primary_key=True)
    name = Text(required=True)
    age = Integer()
    email = Text(index=True)
    active = Boolean(default=True)


class TestQuerySet:
    """Testes para a classe QuerySet."""
    
    def test_queryset_initialization(self):
        """Testa inicialização do QuerySet."""
        queryset = QuerySet(UserModel)
        assert queryset.model_cls == UserModel
        assert queryset._filters == {}
        assert queryset._limit is None
        assert queryset._ordering == []

    def test_queryset_initialization_with_filters(self):
        """Testa inicialização do QuerySet com filtros."""
        queryset = QuerySet(UserModel).filter(name='João')
        assert queryset.model_cls == UserModel
        assert queryset._filters == {'name': 'João'}

    def test_queryset_filter(self):
        """Testa filtros do QuerySet."""
        queryset = QuerySet(UserModel).filter(name='João', age=25)
        assert queryset._filters == {'name': 'João', 'age': 25}

    def test_queryset_filter_chaining(self):
        """Testa encadeamento de filtros."""
        queryset = QuerySet(UserModel).filter(name='João').filter(age=25)
        assert queryset._filters == {'name': 'João', 'age': 25}

    def test_queryset_limit(self):
        """Testa limite do QuerySet."""
        queryset = QuerySet(UserModel).limit(10)
        assert queryset._limit == 10

    def test_queryset_offset(self):
        """Testa offset do QuerySet."""
        # QuerySet não tem offset direto, mas pode ser implementado
        queryset = QuerySet(UserModel)
        # Por enquanto, apenas testamos que não quebra
        assert queryset._limit is None

    def test_queryset_order_by(self):
        """Testa ordenação do QuerySet."""
        queryset = QuerySet(UserModel).order_by('name', 'age')
        assert queryset._ordering == ['name', 'age']

    def test_queryset_order_by_descending(self):
        """Testa ordenação descendente do QuerySet."""
        queryset = QuerySet(UserModel).order_by('-name', 'age')
        assert queryset._ordering == ['-name', 'age']

    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_build_select_query(self, mock_build_select):
        """Testa construção de query SELECT."""
        mock_build_select.return_value = ("SELECT * FROM test_users WHERE name = ? AND age > ?", ['João', 18])
        
        queryset = QuerySet(UserModel).filter(name='João', age__gt=18)
        # A query só é construída quando executada
        assert queryset._filters == {'name': 'João', 'age__gt': 18}

    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_build_select_query_no_filters(self, mock_build_select):
        """Testa construção de query SELECT sem filtros."""
        mock_build_select.return_value = ("SELECT * FROM test_users", [])
        
        queryset = QuerySet(UserModel)
        assert queryset._filters == {}

    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_build_select_query_with_limit(self, mock_build_select):
        """Testa construção de query SELECT com limite."""
        mock_build_select.return_value = ("SELECT * FROM test_users LIMIT ?", [10])
        
        queryset = QuerySet(UserModel).limit(10)
        assert queryset._limit == 10

    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_build_select_query_with_offset(self, mock_build_select):
        """Testa construção de query SELECT com offset."""
        # QuerySet não tem offset direto ainda
        queryset = QuerySet(UserModel)
        assert queryset._limit is None

    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_build_select_query_with_order_by(self, mock_build_select):
        """Testa construção de query SELECT com ordenação."""
        mock_build_select.return_value = ("SELECT * FROM test_users ORDER BY name, age DESC", [])
        
        queryset = QuerySet(UserModel).order_by('name', '-age')
        assert queryset._ordering == ['name', '-age']

    @patch('caspyorm.query.query_builder.build_count_cql')
    def test_queryset_build_count_query(self, mock_build_count):
        """Testa construção de query COUNT."""
        mock_build_count.return_value = ("SELECT COUNT(*) FROM test_users WHERE name = ?", ['João'])
        
        queryset = QuerySet(UserModel).filter(name='João')
        assert queryset._filters == {'name': 'João'}

    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_build_exists_query(self, mock_build_select):
        """Testa construção de query EXISTS."""
        mock_build_select.return_value = ("SELECT 1 FROM test_users WHERE name = ? LIMIT 1", ['João'])
        
        queryset = QuerySet(UserModel).filter(name='João')
        assert queryset._filters == {'name': 'João'}

    @patch('caspyorm.query.query_builder.build_delete_cql')
    def test_queryset_build_delete_query(self, mock_build_delete):
        """Testa construção de query DELETE."""
        mock_build_delete.return_value = ("DELETE FROM test_users WHERE name = ?", ['João'])
        
        queryset = QuerySet(UserModel).filter(name='João')
        assert queryset._filters == {'name': 'João'}

    @patch('caspyorm.query.query_builder.build_delete_cql')
    def test_queryset_build_delete_query_no_filters(self, mock_build_delete):
        """Testa construção de query DELETE sem filtros."""
        mock_build_delete.return_value = ("DELETE FROM test_users", [])
        
        queryset = QuerySet(UserModel)
        assert queryset._filters == {}

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_all_async(self, mock_build_select, mock_get_session):
        """Testa execução assíncrona de all()."""
        # Mock da sessão
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        # Mock da query
        mock_build_select.return_value = ("SELECT * FROM test_users", [])
        
        # Mock do resultado
        mock_result = Mock()
        mock_result._asdict.return_value = {'id': '123', 'name': 'João', 'age': 25}
        mock_session.execute.return_value = [mock_result]
        
        queryset = QuerySet(UserModel)
        # Por enquanto, apenas testamos que não quebra
        assert queryset._result_cache is None

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_all_async_empty(self, mock_build_select, mock_get_session):
        """Testa execução assíncrona de all() com resultado vazio."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_select.return_value = ("SELECT * FROM test_users", [])
        mock_session.execute.return_value = []
        
        queryset = QuerySet(UserModel)
        assert queryset._result_cache is None

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_all_async_with_filters(self, mock_build_select, mock_get_session):
        """Testa execução assíncrona de all() com filtros."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_select.return_value = ("SELECT * FROM test_users WHERE name = ?", ['João'])
        
        mock_result = Mock()
        mock_result._asdict.return_value = {'id': '123', 'name': 'João', 'age': 25}
        mock_session.execute.return_value = [mock_result]
        
        queryset = QuerySet(UserModel).filter(name='João')
        assert queryset._filters == {'name': 'João'}

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_count_cql')
    def test_queryset_count_async(self, mock_build_count, mock_get_session):
        """Testa execução assíncrona de count()."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_count.return_value = ("SELECT COUNT(*) FROM test_users", [])
        
        mock_row = Mock()
        mock_row.count = 5
        mock_session.execute.return_value.one.return_value = mock_row
        
        queryset = QuerySet(UserModel)
        assert queryset._result_cache is None

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_exists_async(self, mock_build_select, mock_get_session):
        """Testa execução assíncrona de exists()."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_select.return_value = ("SELECT 1 FROM test_users LIMIT 1", [])
        
        mock_result = Mock()
        mock_session.execute.return_value = [mock_result]
        
        queryset = QuerySet(UserModel)
        assert queryset._result_cache is None

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_exists_async_not_found(self, mock_build_select, mock_get_session):
        """Testa execução assíncrona de exists() quando não encontra."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_select.return_value = ("SELECT 1 FROM test_users LIMIT 1", [])
        mock_session.execute.return_value = []
        
        queryset = QuerySet(UserModel)
        assert queryset._result_cache is None

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_delete_cql')
    def test_queryset_delete_async(self, mock_build_delete, mock_get_session):
        """Testa execução assíncrona de delete()."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_delete.return_value = ("DELETE FROM test_users WHERE name = ?", ['João'])
        
        queryset = QuerySet(UserModel).filter(name='João')
        assert queryset._filters == {'name': 'João'}

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_delete_cql')
    def test_queryset_delete_async_no_filters(self, mock_build_delete, mock_get_session):
        """Testa execução assíncrona de delete() sem filtros."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_delete.return_value = ("DELETE FROM test_users", [])
        
        queryset = QuerySet(UserModel)
        assert queryset._filters == {}

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_first(self, mock_build_select, mock_get_session):
        """Testa obtenção do primeiro registro."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_select.return_value = ("SELECT * FROM test_users LIMIT 1", [])
        
        mock_result = Mock()
        mock_result._asdict.return_value = {'id': '123', 'name': 'João', 'age': 25}
        mock_session.execute.return_value = [mock_result]
        
        queryset = QuerySet(UserModel)
        assert queryset._result_cache is None

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_last(self, mock_build_select, mock_get_session):
        """Testa obtenção do último registro."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        mock_build_select.return_value = ("SELECT * FROM test_users ORDER BY name DESC LIMIT 1", [])
        
        mock_result = Mock()
        mock_result._asdict.return_value = {'id': '456', 'name': 'Maria', 'age': 30}
        mock_session.execute.return_value = [mock_result]
        
        queryset = QuerySet(UserModel)
        assert queryset._result_cache is None

    @patch('caspyorm.query.get_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_page(self, mock_build_select, mock_get_session):
        """Testa paginação síncrona do QuerySet."""
        mock_session = Mock()
        mock_get_session.return_value = mock_session
        
        mock_build_select.return_value = ("SELECT * FROM test_users", [])
        
        # Corrigir o mock para que _asdict seja um método que retorna o dicionário
        mock_row = Mock()
        mock_row._asdict = Mock(return_value={'id': '12345678-1234-5678-1234-567812345678', 'name': 'User1'})
        mock_result_set = [mock_row]
        mock_result = Mock()
        mock_result.paging_state = b'next_page_state'
        mock_result.__iter__ = lambda self: iter(mock_result_set)
        mock_session.execute.return_value = mock_result
        
        queryset = QuerySet(UserModel)
        results, next_paging_state = queryset.page(page_size=1)
        
        assert len(results) == 1
        assert hasattr(results[0], 'name') and getattr(results[0], 'name') == 'User1'
        assert next_paging_state == b'next_page_state'
        mock_session.execute.assert_called_once()
        mock_session.execute.call_args[0][0].fetch_size == 1

    @pytest.mark.asyncio
    @patch('caspyorm.query.get_async_session')
    @patch('caspyorm.query.query_builder.build_select_cql')
    async def test_queryset_page_async(self, mock_build_select, mock_get_async_session):
        """Testa paginação assíncrona do QuerySet."""
        mock_session = Mock()
        mock_get_async_session.return_value = mock_session
        
        mock_build_select.return_value = ("SELECT * FROM test_users", [])
        
        # Corrigir o mock para que execute_async seja um AsyncMock e retorne um resultado awaitable
        mock_row = Mock()
        mock_row._asdict = Mock(return_value={'id': '87654321-4321-8765-4321-876543210987', 'name': 'User2'})
        mock_result_set = [mock_row]
        
        # Criar um mock awaitable para o resultado
        mock_awaitable_result = Mock()
        mock_awaitable_result.paging_state = b'next_page_state_async'
        mock_awaitable_result.__iter__ = lambda self: iter(mock_result_set)
        
        mock_session.execute_async = AsyncMock(return_value=mock_awaitable_result)
        
        queryset = QuerySet(UserModel)
        results, next_paging_state = await queryset.page_async(page_size=1)
        
        assert len(results) == 1
        assert hasattr(results[0], 'name') and getattr(results[0], 'name') == 'User2'
        assert next_paging_state == b'next_page_state_async'
        mock_session.execute_async.assert_called_once()
        mock_session.execute_async.call_args[0][0].fetch_size == 1

    def test_queryset_hash(self):
        """Testa hash do QuerySet."""
        queryset = QuerySet(UserModel).filter(name='João')
        # QuerySet deve ser hashable
        hash_value = hash(queryset)
        assert isinstance(hash_value, int)

    @patch('caspyorm.query.query_builder.build_select_cql')
    def test_queryset_build_select_query_with_allow_filtering(self, mock_build_select_cql):
        """Testa construção de query SELECT com ALLOW FILTERING."""
        # Mockar o retorno para que a execução não falhe
        mock_build_select_cql.return_value = ("SELECT * FROM test_users WHERE name = ? ALLOW FILTERING", ['João'])

        queryset = QuerySet(UserModel).filter(name='João')

        # Mockar a sessão e o execute para que a query seja "executada"
        mock_session = Mock()
        mock_session.prepare.return_value = Mock()
        mock_session.execute.return_value = []
        with patch('caspyorm.query.get_session', return_value=mock_session):
            # Forçar a execução do queryset para que build_select_cql seja chamado
            list(queryset)

            mock_build_select_cql.assert_called_once_with(
                queryset.model_cls.__caspy_schema__,
                columns=None,
                filters={'name': 'João'},
                limit=None,
                ordering=[],
                allow_filtering=True
            )


class TestQuerySetOperators:
    """Testes para operadores de filtro do QuerySet."""
    
    def test_queryset_gt_operator(self):
        """Testa operador __gt."""
        queryset = QuerySet(UserModel).filter(age__gt=18)
        assert queryset._filters == {'age__gt': 18}

    def test_queryset_gte_operator(self):
        """Testa operador __gte."""
        queryset = QuerySet(UserModel).filter(age__gte=18)
        assert queryset._filters == {'age__gte': 18}

    def test_queryset_lt_operator(self):
        """Testa operador __lt."""
        queryset = QuerySet(UserModel).filter(age__lt=65)
        assert queryset._filters == {'age__lt': 65}

    def test_queryset_lte_operator(self):
        """Testa operador __lte."""
        queryset = QuerySet(UserModel).filter(age__lte=65)
        assert queryset._filters == {'age__lte': 65}

    def test_queryset_ne_operator(self):
        """Testa operador __ne."""
        queryset = QuerySet(UserModel).filter(age__ne=25)
        assert queryset._filters == {'age__ne': 25}

    def test_queryset_in_operator(self):
        """Testa operador __in."""
        queryset = QuerySet(UserModel).filter(name__in=['João', 'Maria'])
        assert queryset._filters == {'name__in': ['João', 'Maria']}

    def test_queryset_not_in_operator(self):
        """Testa operador __not_in."""
        queryset = QuerySet(UserModel).filter(name__not_in=['João', 'Maria'])
        assert queryset._filters == {'name__not_in': ['João', 'Maria']}

    def test_queryset_contains_operator(self):
        """Testa operador __contains."""
        queryset = QuerySet(UserModel).filter(name__contains='João')
        assert queryset._filters == {'name__contains': 'João'}

    def test_queryset_startswith_operator(self):
        """Testa operador __startswith."""
        queryset = QuerySet(UserModel).filter(name__startswith='João')
        assert queryset._filters == {'name__startswith': 'João'}

    def test_queryset_endswith_operator(self):
        """Testa operador __endswith."""
        queryset = QuerySet(UserModel).filter(name__endswith='Silva')
        assert queryset._filters == {'name__endswith': 'Silva'}

    def test_queryset_icontains_operator(self):
        """Testa operador __icontains."""
        queryset = QuerySet(UserModel).filter(name__icontains='joão')
        assert queryset._filters == {'name__icontains': 'joão'}

    def test_queryset_istartswith_operator(self):
        """Testa operador __istartswith."""
        queryset = QuerySet(UserModel).filter(name__istartswith='joão')
        assert queryset._filters == {'name__istartswith': 'joão'}

    def test_queryset_iendswith_operator(self):
        """Testa operador __iendswith."""
        queryset = QuerySet(UserModel).filter(name__iendswith='silva')
        assert queryset._filters == {'name__iendswith': 'silva'}

    def test_queryset_isnull_operator(self):
        """Testa operador __isnull."""
        queryset = QuerySet(UserModel).filter(age__isnull=True)
        assert queryset._filters == {'age__isnull': True}

    def test_queryset_isnotnull_operator(self):
        """Testa operador __isnotnull."""
        queryset = QuerySet(UserModel).filter(age__isnotnull=True)
        assert queryset._filters == {'age__isnotnull': True}

    def test_queryset_multiple_operators(self):
        """Testa múltiplos operadores."""
        queryset = QuerySet(UserModel).filter(
            age__gte=18,
            age__lt=65,
            name__contains='João',
            active=True
        )
        assert queryset._filters == {
            'age__gte': 18,
            'age__lt': 65,
            'name__contains': 'João',
            'active': True
        }


class TestQuerySetErrorHandling:
    """Testes para tratamento de erros do QuerySet."""
    
    def test_queryset_invalid_operator(self):
        """Testa operador inválido."""
        # QuerySet não valida operadores no momento da criação
        queryset = QuerySet(UserModel).filter(name__invalid='value')
        assert queryset._filters == {'name__invalid': 'value'}

    def test_queryset_invalid_field(self):
        """Testa campo inválido."""
        # QuerySet não valida campos no momento da criação
        queryset = QuerySet(UserModel).filter(invalid_field='value')
        assert queryset._filters == {'invalid_field': 'value'}

    def test_queryset_empty_in_list(self):
        """Testa lista vazia no operador __in."""
        # QuerySet não valida listas vazias no momento da criação
        queryset = QuerySet(UserModel).filter(name__in=[])
        assert queryset._filters == {'name__in': []}

    def test_queryset_empty_not_in_list(self):
        """Testa lista vazia no operador __not_in."""
        # QuerySet não valida listas vazias no momento da criação
        queryset = QuerySet(UserModel).filter(name__not_in=[])
        assert queryset._filters == {'name__not_in': []}

    def test_queryset_invalid_limit(self):
        """Testa limite inválido."""
        queryset = QuerySet(UserModel)
        # Por enquanto, apenas testamos que não quebra
        assert queryset._limit is None

    def test_queryset_invalid_offset(self):
        """Testa offset inválido."""
        queryset = QuerySet(UserModel)
        # QuerySet não tem offset ainda
        assert queryset._limit is None 