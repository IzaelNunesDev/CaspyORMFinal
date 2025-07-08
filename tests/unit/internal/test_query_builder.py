import pytest
from unittest.mock import patch
from src.caspyorm._internal.query_builder import (
    build_insert_cql,
    build_select_cql,
    build_create_table_cql,
    build_add_column_cql,
    build_drop_column_cql,
    build_count_cql,
    build_delete_cql,
    build_update_cql,
    build_collection_update_cql,
)

# Mock the get_cql_type function as it's an external dependency
@patch('src.caspyorm._internal.query_builder.get_cql_type', side_effect=lambda x: x)
class TestQueryBuilder:

    # Schema de exemplo para os testes
    EXAMPLE_SCHEMA = {
        'table_name': 'users',
        'fields': {
            'id': {'type': 'uuid'},
            'name': {'type': 'text'},
            'age': {'type': 'int'},
            'emails': {'type': 'set<text>'},
        },
        'primary_keys': ['id'],
        'partition_keys': ['id'],
        'clustering_keys': [],
    }

    EXAMPLE_SCHEMA_COMPOSITE_PK = {
        'table_name': 'orders',
        'fields': {
            'user_id': {'type': 'uuid'},
            'order_id': {'type': 'uuid'},
            'amount': {'type': 'decimal'},
        },
        'primary_keys': ['user_id', 'order_id'],
        'partition_keys': ['user_id'],
        'clustering_keys': ['order_id'],
    }

    def test_build_insert_cql(self, mock_get_cql_type):
        cql = build_insert_cql(self.EXAMPLE_SCHEMA)
        assert cql == "INSERT INTO users (id, name, age, emails) VALUES (?, ?, ?, ?)"

    @pytest.mark.parametrize("filters, limit, ordering, allow_filtering, expected_cql, expected_params", [
        # Teste básico sem filtros, limite ou ordenação
        (None, None, None, False, "SELECT * FROM users", []),
        # Teste com filtros exatos
        ({'id': '123'}, None, None, False, "SELECT * FROM users WHERE id = ?", ['123']),
        ({'name': 'Alice', 'age': 30}, None, None, False, "SELECT * FROM users WHERE name = ? AND age = ?", ['Alice', 30]),
        # Teste com operadores
        ({'age__gt': 25}, None, None, False, "SELECT * FROM users WHERE age > ?", [25]),
        ({'age__lt': 40, 'name__exact': 'Bob'}, None, None, False, "SELECT * FROM users WHERE age < ? AND name = ?", [40, 'Bob']),
        # Teste com operador IN
        ({'id__in': ['1', '2', '3']}, None, None, False, "SELECT * FROM users WHERE id IN (?, ?, ?)", ['1', '2', '3']),
        # Teste com limite
        (None, 10, None, False, "SELECT * FROM users LIMIT ?", [10]),
        ({'age__gte': 18}, 5, None, False, "SELECT * FROM users WHERE age >= ? LIMIT ?", [18, 5]),
        # Teste com ordenação
        (None, None, ['name'], False, "SELECT * FROM users ORDER BY name ASC", []),
        (None, None, ['-age', 'name'], False, "SELECT * FROM users ORDER BY age DESC, name ASC", []),
        # Teste com ALLOW FILTERING
        ({'age__gt': 25}, None, None, True, "SELECT * FROM users WHERE age > ? ALLOW FILTERING", [25]),
        # Teste com colunas específicas
        (['name', 'age'], None, None, False, "SELECT name, age FROM users", []),
    ])
    def test_build_select_cql(self, mock_get_cql_type, filters, limit, ordering, allow_filtering, expected_cql, expected_params):
        if isinstance(filters, dict) or filters is None:
            cql, params = build_select_cql(self.EXAMPLE_SCHEMA, filters=filters, limit=limit, ordering=ordering, allow_filtering=allow_filtering)
        else: # Assume filters is a list of columns
            cql, params = build_select_cql(self.EXAMPLE_SCHEMA, columns=filters, limit=limit, ordering=ordering, allow_filtering=allow_filtering)
        assert cql == expected_cql
        assert params == expected_params

    def test_build_select_cql_unsupported_operator(self, mock_get_cql_type):
        with pytest.raises(ValueError, match="Operador de filtro não suportado: 'unsupported'"):
            build_select_cql(self.EXAMPLE_SCHEMA, filters={'name__unsupported': 'value'})

    def test_build_select_cql_in_operator_invalid_value(self, mock_get_cql_type):
        with pytest.raises(TypeError, match="O valor para o filtro '__in' deve ser uma lista, tupla ou set, recebido: <class 'str'>"):
            build_select_cql(self.EXAMPLE_SCHEMA, filters={'id__in': '1,2,3'})

    def test_build_create_table_cql_simple_pk(self, mock_get_cql_type):
        cql = build_create_table_cql(self.EXAMPLE_SCHEMA)
        assert cql == "CREATE TABLE IF NOT EXISTS users (id uuid, name text, age int, emails set<text>, PRIMARY KEY (id))"

    def test_build_create_table_cql_composite_pk(self, mock_get_cql_type):
        cql = build_create_table_cql(self.EXAMPLE_SCHEMA_COMPOSITE_PK)
        assert cql == "CREATE TABLE IF NOT EXISTS orders (user_id uuid, order_id uuid, amount decimal, PRIMARY KEY (user_id, order_id))"

    def test_build_add_column_cql(self, mock_get_cql_type):
        cql = build_add_column_cql("users", "address", "text")
        assert cql == "ALTER TABLE users ADD address text;"

    def test_build_drop_column_cql(self, mock_get_cql_type):
        cql = build_drop_column_cql("users", "address")
        assert cql == "ALTER TABLE users DROP address;"

    @pytest.mark.parametrize("filters, expected_cql, expected_params", [
        (None, "SELECT COUNT(*) FROM users", []), # COUNT always uses ALLOW FILTERING
        ({'age__gt': 25}, "SELECT COUNT(*) FROM users WHERE age > ? ALLOW FILTERING", [25]),
        ({'name': 'Alice', 'age': 30}, "SELECT COUNT(*) FROM users WHERE name = ? AND age = ? ALLOW FILTERING", ['Alice', 30]),
        ({'id__in': ['1', '2']}, "SELECT COUNT(*) FROM users WHERE id IN (?, ?) ALLOW FILTERING", ['1', '2']),
    ])
    def test_build_count_cql(self, mock_get_cql_type, filters, expected_cql, expected_params):
        cql, params = build_count_cql(self.EXAMPLE_SCHEMA, filters=filters)
        assert cql == expected_cql
        assert params == expected_params

    def test_build_delete_cql(self, mock_get_cql_type):
        cql, params = build_delete_cql(self.EXAMPLE_SCHEMA, {'id': '123'})
        assert cql == "DELETE FROM users WHERE id = ?"
        assert params == ['123']

    def test_build_delete_cql_no_filters(self, mock_get_cql_type):
        with pytest.raises(ValueError, match="A deleção em massa sem um filtro 'WHERE' não é permitida por segurança."):
            build_delete_cql(self.EXAMPLE_SCHEMA, {})

    def test_build_delete_cql_missing_partition_key(self, mock_get_cql_type):
        # Assuming 'id' is the partition key, deleting by 'name' should fail
        with pytest.raises(ValueError, match="Para deletar, você deve especificar todos os campos da chave de partição."):
            build_delete_cql(self.EXAMPLE_SCHEMA, {'name': 'Alice'})

    def test_build_delete_cql_composite_pk(self, mock_get_cql_type):
        cql, params = build_delete_cql(self.EXAMPLE_SCHEMA_COMPOSITE_PK, {'user_id': 'user1', 'order_id': 'order1'})
        assert cql == "DELETE FROM orders WHERE user_id = ? AND order_id = ?"
        assert params == ['user1', 'order1']

    @pytest.mark.parametrize("update_data, pk_filters, expected_cql, expected_params", [
        ({'name': 'Bob'}, {'id': '123'}, "UPDATE users SET name = ? WHERE id = ?", ['Bob', '123']),
        ({'age': 31, 'name': 'Charlie'}, {'id': '456'}, "UPDATE users SET age = ?, name = ? WHERE id = ?", [31, 'Charlie', '456']),
    ])
    def test_build_update_cql(self, mock_get_cql_type, update_data, pk_filters, expected_cql, expected_params):
        cql, params = build_update_cql(self.EXAMPLE_SCHEMA, update_data, pk_filters)
        assert cql == expected_cql
        assert params == expected_params

    def test_build_update_cql_no_update_data(self, mock_get_cql_type):
        with pytest.raises(ValueError, match="Nenhum campo fornecido para atualização"):
            build_update_cql(self.EXAMPLE_SCHEMA, {}, {'id': '123'})

    def test_build_update_cql_no_pk_filters(self, mock_get_cql_type):
        with pytest.raises(ValueError, match="Filtros de chave primária são obrigatórios para UPDATE"):
            build_update_cql(self.EXAMPLE_SCHEMA, {'name': 'Bob'}, {})

    @pytest.mark.parametrize("field_name, add, remove, pk_filters, expected_cql, expected_params", [
        ('emails', ['a@b.com'], None, {'id': '123'}, "UPDATE users SET emails = emails + ? WHERE id = ?", [['a@b.com'], '123']),
        ('emails', None, ['c@d.com'], {'id': '123'}, "UPDATE users SET emails = emails - ? WHERE id = ?", [['c@d.com'], '123']),
        ('emails', ['x@y.com'], ['z@w.com'], {'id': '123'}, "UPDATE users SET emails = emails + ?, emails = emails - ? WHERE id = ?", [['x@y.com'], ['z@w.com'], '123']),
    ])
    def test_build_collection_update_cql(self, mock_get_cql_type, field_name, add, remove, pk_filters, expected_cql, expected_params):
        cql, params = build_collection_update_cql(self.EXAMPLE_SCHEMA, field_name, add, remove, pk_filters)
        assert cql == expected_cql
        assert params == expected_params

    def test_build_collection_update_cql_no_add_or_remove(self, mock_get_cql_type):
        with pytest.raises(ValueError, match="Deve ser fornecido 'add' ou 'remove' para update_collection."):
            build_collection_update_cql(self.EXAMPLE_SCHEMA, 'emails', None, None, {'id': '123'})
