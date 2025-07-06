"""
Testes unitários para o módulo _internal/model_construction.py do CaspyORM.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from caspyorm._internal.model_construction import ModelMetaclass
from caspyorm.fields import Text, Integer, UUID, Boolean, BaseField


class TestModelMetaclass:
    """Testes para a classe ModelMetaclass."""
    
    def test_model_metaclass_creation(self):
        """Testa criação de metaclasse."""
        metaclass = ModelMetaclass
        assert metaclass is not None
    
    def test_model_metaclass_new_basic(self):
        """Testa criação básica de classe com metaclasse."""
        # Definir campos
        fields = {
            'id': UUID(primary_key=True),
            'name': Text(required=True),
            'age': Integer()
        }
        
        # Criar classe usando metaclasse
        attrs = {
            'model_fields': fields,
            '__module__': 'test_module'
        }
        
        TestClass = ModelMetaclass.__new__(
            ModelMetaclass, 
            'TestClass', 
            (object,), 
            attrs
        )
        
        assert TestClass.__name__ == 'TestClass'
        assert hasattr(TestClass, 'model_fields')
        assert TestClass.model_fields == fields
    
    def test_model_metaclass_new_with_table_name(self):
        """Testa criação com nome de tabela personalizado."""
        fields = {
            'id': UUID(primary_key=True),
            'name': Text(required=True)
        }
        
        attrs = {
            'model_fields': fields,
            '__table_name__': 'custom_table',
            '__module__': 'test_module'
        }
        
        TestClass = ModelMetaclass.__new__(
            ModelMetaclass, 
            'TestClass', 
            (object,), 
            attrs
        )
        
        assert TestClass.__table_name__ == 'custom_table'
    
    def test_model_metaclass_new_without_fields(self):
        """Testa criação sem campos (deve falhar)."""
        attrs = {
            'model_fields': {},
            '__module__': 'test_module'
        }
        
        with pytest.raises(TypeError, match="não definiu nenhum campo"):
            ModelMetaclass.__new__(
                ModelMetaclass, 
                'TestClass', 
                (object,), 
                attrs
            )
    
    def test_model_metaclass_new_without_primary_key(self):
        """Testa criação sem chave primária (deve falhar)."""
        fields = {
            'name': Text(required=True),
            'age': Integer()
        }
        
        attrs = {
            'model_fields': fields,
            '__module__': 'test_module'
        }
        
        with pytest.raises(ValueError, match="deve ter pelo menos uma"):
            ModelMetaclass.__new__(
                ModelMetaclass, 
                'TestClass', 
                (object,), 
                attrs
            )
    
    def test_model_metaclass_build_schema(self):
        """Testa construção de schema."""
        fields = {
            'id': UUID(primary_key=True, partition_key=True),
            'name': Text(required=True),
            'age': Integer(),
            'email': Text(index=True)
        }
        
        schema = ModelMetaclass.build_schema('test_table', fields)
        
        assert schema['table_name'] == 'test_table'
        assert 'id' in schema['fields']
        assert 'name' in schema['fields']
        assert 'age' in schema['fields']
        assert 'email' in schema['fields']
        assert 'id' in schema['primary_keys']
        assert 'id' in schema['partition_keys']
        assert 'email' in schema['indexes']
    
    def test_model_metaclass_build_schema_with_clustering_key(self):
        """Testa construção de schema com clustering key."""
        fields = {
            'id': UUID(primary_key=True, partition_key=True),
            'timestamp': Integer(clustering_key=True),
            'name': Text(required=True)
        }
        
        schema = ModelMetaclass.build_schema('test_table', fields)
        
        assert 'id' in schema['partition_keys']
        assert 'timestamp' in schema['clustering_keys']
        assert 'id' in schema['primary_keys']
        assert 'timestamp' in schema['primary_keys']
    
    def test_model_metaclass_build_schema_field_properties(self):
        """Testa propriedades dos campos no schema."""
        fields = {
            'id': UUID(primary_key=True, partition_key=True),
            'name': Text(required=True),
            'age': Integer(default=0)
        }
        
        schema = ModelMetaclass.build_schema('test_table', fields)
        
        # Verificar propriedades do campo name
        name_field = schema['fields']['name']
        assert name_field['required'] is True
        assert name_field['default'] is None
        
        # Verificar propriedades do campo age
        age_field = schema['fields']['age']
        assert age_field['required'] is False
        assert age_field['default'] == 0
    
    def test_model_metaclass_build_schema_cql_definition(self):
        """Testa definição CQL dos campos no schema."""
        fields = {
            'id': UUID(primary_key=True, partition_key=True),
            'name': Text(required=True)
        }
        
        schema = ModelMetaclass.build_schema('test_table', fields)
        
        # Verificar se os campos têm definição CQL
        assert 'type' in schema['fields']['id']
        assert 'type' in schema['fields']['name']
    
    def test_model_metaclass_new_with_annotations(self):
        """Testa criação com anotações de tipo."""
        # Simular anotações de tipo
        annotations = {
            'id': UUID(primary_key=True),
            'name': Text(required=True)
        }
        
        attrs = {
            '__annotations__': annotations,
            '__module__': 'test_module'
        }
        
        # Adicionar campos diretamente nos atributos
        for key, value in annotations.items():
            attrs[key] = value
        
        TestClass = ModelMetaclass.__new__(
            ModelMetaclass, 
            'TestClass', 
            (object,), 
            attrs
        )
        
        assert hasattr(TestClass, 'model_fields')
        assert 'id' in TestClass.model_fields
        assert 'name' in TestClass.model_fields


class TestModelMetaclassIntegration:
    """Testes de integração da ModelMetaclass."""
    
    def test_model_metaclass_complete_workflow(self):
        """Testa fluxo completo de criação de modelo."""
        # Definir campos complexos
        fields = {
            'id': UUID(primary_key=True, partition_key=True),
            'user_id': UUID(clustering_key=True),
            'name': Text(required=True),
            'age': Integer(default=0),
            'email': Text(index=True),
            'active': Boolean(default=True)
        }
        
        attrs = {
            'model_fields': fields,
            '__table_name__': 'users',
            '__module__': 'test_module'
        }
        
        # Criar classe
        UserClass = ModelMetaclass.__new__(
            ModelMetaclass, 
            'User', 
            (object,), 
            attrs
        )
        
        # Verificar estrutura completa
        assert UserClass.__name__ == 'User'
        assert UserClass.__table_name__ == 'users'
        assert hasattr(UserClass, 'model_fields')
        assert hasattr(UserClass, '__caspy_schema__')
        
        # Verificar schema
        schema = UserClass.__caspy_schema__
        assert schema['table_name'] == 'users'
        assert len(schema['fields']) == 6
        assert len(schema['primary_keys']) == 2  # id + user_id
        assert len(schema['partition_keys']) == 1  # id
        assert len(schema['clustering_keys']) == 1  # user_id
        assert len(schema['indexes']) == 1  # email
    
    def test_model_metaclass_error_handling(self):
        """Testa tratamento de erros na metaclasse."""
        # Teste com campos inválidos
        with pytest.raises(ValueError):
            ModelMetaclass.build_schema('test', {})
        
        # Teste com campos sem chave primária
        fields = {'name': Text(required=True)}
        with pytest.raises(ValueError):
            ModelMetaclass.build_schema('test', fields)


class TestModelMetaclassEdgeCases:
    """Testes para casos extremos da ModelMetaclass."""
    
    def test_model_metaclass_empty_fields_dict(self):
        """Testa com dicionário de campos vazio."""
        attrs = {
            'model_fields': {},
            '__module__': 'test_module'
        }
        
        with pytest.raises(TypeError):
            ModelMetaclass.__new__(
                ModelMetaclass, 
                'TestClass', 
                (object,), 
                attrs
            )
    
    def test_model_metaclass_none_fields(self):
        """Testa com campos None."""
        attrs = {
            'model_fields': None,
            '__module__': 'test_module'
        }
        
        with pytest.raises(TypeError):
            ModelMetaclass.__new__(
                ModelMetaclass, 
                'TestClass', 
                (object,), 
                attrs
            )
    
    def test_model_metaclass_invalid_field_types(self):
        """Testa com tipos de campo inválidos."""
        fields = {
            'id': 'not_a_field',  # String ao invés de BaseField
            'name': Text(required=True)
        }
        
        attrs = {
            'model_fields': fields,
            '__module__': 'test_module'
        }
        
        # Deve falhar ao tentar acessar propriedades de BaseField
        with pytest.raises(AttributeError):
            ModelMetaclass.__new__(
                ModelMetaclass, 
                'TestClass', 
                (object,), 
                attrs
            )
    
    def test_model_metaclass_duplicate_field_names(self):
        """Testa com nomes de campo duplicados."""
        fields = {
            'id': UUID(primary_key=True, partition_key=True),
            'id': Text(required=True)  # Duplicado
        }
        
        attrs = {
            'model_fields': fields,
            '__module__': 'test_module'
        }
        
        # Deve levantar exceção por falta de chave primária
        with pytest.raises(ValueError, match="pelo menos uma 'partition_key' ou 'primary_key'"):
            ModelMetaclass.__new__(
                ModelMetaclass,
                'TestClass',
                (object,),
                attrs
            ) 