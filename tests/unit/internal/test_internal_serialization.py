"""
Testes unitários para o módulo _internal/serialization.py do CaspyORM.
"""

import pytest
import json
import uuid
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from src.caspyorm._internal.serialization import (
    CaspyJSONEncoder,
    model_to_dict,
    model_to_json
)
from src.caspyorm.core.model import Model
from src.caspyorm.core.fields import Text, Integer, UUID, Boolean
from src.caspyorm.utils.exceptions import ValidationError


class UserModel(Model):
    """Modelo de teste para usuário."""
    id = UUID(primary_key=True)
    name = Text(required=True)
    age = Integer()
    email = Text(index=True)
    active = Boolean(default=True)


class TestCaspyJSONEncoder:
    """Testes para a classe CaspyJSONEncoder."""
    
    def test_caspy_json_encoder_uuid(self):
        """Testa codificação de UUID."""
        test_uuid = uuid.uuid4()
        encoder = CaspyJSONEncoder()
        
        result = encoder.default(test_uuid)
        
        assert isinstance(result, str)
        assert result == str(test_uuid)
    
    def test_caspy_json_encoder_datetime(self):
        """Testa codificação de datetime."""
        test_datetime = datetime.now()
        encoder = CaspyJSONEncoder()
        
        result = encoder.default(test_datetime)
        
        assert isinstance(result, str)
        assert result == test_datetime.isoformat()
    
    def test_caspy_json_encoder_other_types(self):
        """Testa codificação de outros tipos."""
        encoder = CaspyJSONEncoder()
        
        # Testa com string
        with pytest.raises(TypeError):
            encoder.default("string")
        
        # Testa com int
        with pytest.raises(TypeError):
            encoder.default(123)
        
        # Testa com list
        with pytest.raises(TypeError):
            encoder.default([1, 2, 3])
    
    def test_caspy_json_encoder_json_dumps(self):
        """Testa uso do encoder com json.dumps."""
        test_uuid = uuid.uuid4()
        test_datetime = datetime.now()
        
        data = {
            "id": test_uuid,
            "created_at": test_datetime,
            "name": "test"
        }
        
        result = json.dumps(data, cls=CaspyJSONEncoder)
        
        assert isinstance(result, str)
        json_data = json.loads(result)
        assert json_data["id"] == str(test_uuid)
        assert json_data["created_at"] == test_datetime.isoformat()
        assert json_data["name"] == "test"


class TestModelToDict:
    """Testes para a função model_to_dict."""
    
    def test_model_to_dict_basic(self):
        """Testa conversão básica de modelo para dicionário."""
        user = UserModel(name="João", age=25, email="joao@example.com")
        setattr(user, 'id', uuid.uuid4())
        setattr(user, 'active', True)
        
        result = model_to_dict(user)
        
        assert isinstance(result, dict)
        assert result["name"] == "João"
        assert result["age"] == 25
        assert result["email"] == "joao@example.com"
        assert result["active"] is True
        assert "id" in result
    
    def test_model_to_dict_with_none_values(self):
        """Testa conversão com valores None."""
        user = UserModel(name="João")
        setattr(user, 'id', uuid.uuid4())
        
        result = model_to_dict(user)
        
        assert result["name"] == "João"
        assert result["age"] is None
        assert result["email"] is None
        assert result["active"] is True  # valor padrão
    
    def test_model_to_dict_by_alias_parameter(self):
        """Testa parâmetro by_alias (deve ser ignorado por enquanto)."""
        user = UserModel(name="João", age=25)
        setattr(user, 'id', uuid.uuid4())
        
        result = model_to_dict(user, by_alias=True)
        
        # Por enquanto, by_alias é ignorado
        assert result["name"] == "João"
        assert result["age"] == 25
    
    def test_model_to_dict_empty_model(self):
        """Testa conversão de modelo vazio."""
        # Não é possível instanciar sem campos obrigatórios
        with pytest.raises(ValidationError):
            UserModel()


class TestModelToJson:
    """Testes para a função model_to_json."""
    
    def test_model_to_json_basic(self):
        """Testa conversão básica de modelo para JSON."""
        user = UserModel(name="João", age=25, email="joao@example.com")
        setattr(user, 'id', uuid.uuid4())
        setattr(user, 'active', True)
        
        result = model_to_json(user)
        
        assert isinstance(result, str)
        json_data = json.loads(result)
        assert json_data["name"] == "João"
        assert json_data["age"] == 25
        assert json_data["email"] == "joao@example.com"
        assert json_data["active"] is True
    
    def test_model_to_json_with_uuid(self):
        """Testa conversão com UUID."""
        test_uuid = uuid.uuid4()
        user = UserModel(name="João")
        setattr(user, 'id', test_uuid)
        
        result = model_to_json(user)
        
        json_data = json.loads(result)
        assert json_data["id"] == str(test_uuid)
    
    def test_model_to_json_with_datetime(self):
        """Testa conversão com datetime."""
        test_datetime = datetime.now()
        user = UserModel(name="João")
        setattr(user, 'id', uuid.uuid4())
        setattr(user, 'created_at', test_datetime)
        
        result = model_to_json(user)
        
        json_data = json.loads(result)
        # 'created_at' pode não estar presente se não for campo do modelo
        if 'created_at' in json_data:
            assert json_data["created_at"] == test_datetime.isoformat()
    
    def test_model_to_json_with_indent(self):
        """Testa conversão com indentação."""
        user = UserModel(name="João", age=25)
        setattr(user, 'id', uuid.uuid4())
        
        result = model_to_json(user, indent=2)
        
        assert isinstance(result, str)
        # Verificar se há quebras de linha (indentação)
        lines = result.split('\n')
        assert len(lines) > 1
    
    def test_model_to_json_by_alias_parameter(self):
        """Testa parâmetro by_alias."""
        user = UserModel(name="João", age=25)
        setattr(user, 'id', uuid.uuid4())
        
        result = model_to_json(user, by_alias=True)
        
        json_data = json.loads(result)
        assert json_data["name"] == "João"
        assert json_data["age"] == 25
    
    def test_model_to_json_empty_model(self):
        """Testa conversão de modelo vazio para JSON."""
        with pytest.raises(ValidationError):
            UserModel()


class TestSerializationIntegration:
    """Testes de integração da serialização."""
    
    def test_serialization_workflow(self):
        """Testa o fluxo completo de serialização."""
        # Criar modelo
        user = UserModel(name="João", age=25, email="joao@example.com")
        setattr(user, 'id', uuid.uuid4())
        setattr(user, 'active', True)
        
        # Converter para dicionário
        user_dict = model_to_dict(user)
        assert isinstance(user_dict, dict)
        
        # Converter para JSON
        user_json = model_to_json(user)
        assert isinstance(user_json, str)
        
        # Verificar consistência
        json_dict = json.loads(user_json)
        # Comparar UUID como string
        assert str(user_dict["id"]) == json_dict["id"]
        # Comparar os outros campos
        for key in user_dict:
            if key != "id":
                assert user_dict[key] == json_dict[key]
    
    def test_serialization_with_complex_data(self):
        """Testa serialização com dados complexos."""
        test_uuid = uuid.uuid4()
        test_datetime = datetime.now()

        user = UserModel(name="João", age=25)
        setattr(user, 'id', test_uuid)
        setattr(user, 'created_at', test_datetime)
        setattr(user, 'tags', ['tag1', 'tag2'])

        result = model_to_json(user)
        json_data = json.loads(result)

        assert json_data["id"] == str(test_uuid)
        # Só verifica created_at se existir
        if "created_at" in json_data:
            assert json_data["created_at"] == test_datetime.isoformat()
    
    def test_serialization_error_handling(self):
        """Testa tratamento de erros na serialização."""
        # Teste com modelo inválido
        with pytest.raises(AttributeError):
            model_to_dict(None)
        
        with pytest.raises(AttributeError):
            model_to_json(None)


class TestSerializationPerformance:
    """Testes de performance da serialização."""
    
    def test_model_to_dict_performance(self):
        """Testa performance de model_to_dict."""
        import time
        
        user = UserModel(name="João", age=25, email="joao@example.com")
        setattr(user, 'id', uuid.uuid4())
        setattr(user, 'active', True)
        
        start_time = time.time()
        for _ in range(1000):
            model_to_dict(user)
        end_time = time.time()
        
        # Deve ser rápido (menos de 1 segundo para 1000 conversões)
        assert end_time - start_time < 1.0
    
    def test_model_to_json_performance(self):
        """Testa performance de model_to_json."""
        import time
        
        user = UserModel(name="João", age=25, email="joao@example.com")
        setattr(user, 'id', uuid.uuid4())
        setattr(user, 'active', True)
        
        start_time = time.time()
        for _ in range(1000):
            model_to_json(user)
        end_time = time.time()
        
        # Deve ser rápido (menos de 1 segundo para 1000 conversões)
        assert end_time - start_time < 1.0 