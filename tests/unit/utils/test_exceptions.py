"""
Testes unitários para o módulo exceptions.py do CaspyORM.
"""

import pytest
from src.caspyorm.utils.exceptions import (
    CaspyORMException,
    ObjectNotFound,
    MultipleObjectsReturned,
    ConnectionError,
    ValidationError,
    QueryError
)


class TestCaspyORMException:
    """Testes para a classe base CaspyORMException."""
    
    def test_caspyorm_exception_inheritance(self):
        """Testa se CaspyORMException herda de Exception."""
        assert issubclass(CaspyORMException, Exception)
    
    def test_caspyorm_exception_creation(self):
        """Testa criação de CaspyORMException."""
        exception = CaspyORMException("Erro de teste")
        assert str(exception) == "Erro de teste"
    
    def test_caspyorm_exception_without_message(self):
        """Testa criação de CaspyORMException sem mensagem."""
        exception = CaspyORMException()
        assert str(exception) == ""


class TestObjectNotFound:
    """Testes para a exceção ObjectNotFound."""
    
    def test_object_not_found_inheritance(self):
        """Testa se ObjectNotFound herda de CaspyORMException."""
        assert issubclass(ObjectNotFound, CaspyORMException)
    
    def test_object_not_found_creation(self):
        """Testa criação de ObjectNotFound."""
        exception = ObjectNotFound("Objeto não encontrado")
        assert str(exception) == "Objeto não encontrado"
    
    def test_object_not_found_default_message(self):
        """Testa ObjectNotFound com mensagem padrão."""
        exception = ObjectNotFound()
        assert str(exception) == ""


class TestMultipleObjectsReturned:
    """Testes para a exceção MultipleObjectsReturned."""
    
    def test_multiple_objects_returned_inheritance(self):
        """Testa se MultipleObjectsReturned herda de CaspyORMException."""
        assert issubclass(MultipleObjectsReturned, CaspyORMException)
    
    def test_multiple_objects_returned_creation(self):
        """Testa criação de MultipleObjectsReturned."""
        exception = MultipleObjectsReturned("Múltiplos objetos retornados")
        assert str(exception) == "Múltiplos objetos retornados"
    
    def test_multiple_objects_returned_default_message(self):
        """Testa MultipleObjectsReturned com mensagem padrão."""
        exception = MultipleObjectsReturned()
        assert str(exception) == ""


class TestConnectionError:
    """Testes para a exceção ConnectionError."""
    
    def test_connection_error_inheritance(self):
        """Testa se ConnectionError herda de CaspyORMException."""
        assert issubclass(ConnectionError, CaspyORMException)
    
    def test_connection_error_creation(self):
        """Testa criação de ConnectionError."""
        exception = ConnectionError("Erro de conexão")
        assert str(exception) == "Erro de conexão"
    
    def test_connection_error_default_message(self):
        """Testa ConnectionError com mensagem padrão."""
        exception = ConnectionError()
        assert str(exception) == ""


class TestValidationError:
    """Testes para a exceção ValidationError."""
    
    def test_validation_error_inheritance(self):
        """Testa se ValidationError herda de CaspyORMException."""
        assert issubclass(ValidationError, CaspyORMException)
    
    def test_validation_error_creation(self):
        """Testa criação de ValidationError."""
        exception = ValidationError("Erro de validação")
        assert str(exception) == "Erro de validação"
    
    def test_validation_error_default_message(self):
        """Testa ValidationError com mensagem padrão."""
        exception = ValidationError()
        assert str(exception) == ""


class TestQueryError:
    """Testes para a exceção QueryError."""
    
    def test_query_error_inheritance(self):
        """Testa se QueryError herda de CaspyORMException."""
        assert issubclass(QueryError, CaspyORMException)
    
    def test_query_error_creation(self):
        """Testa criação de QueryError."""
        exception = QueryError("Erro de query")
        assert str(exception) == "Erro de query"
    
    def test_query_error_default_message(self):
        """Testa QueryError com mensagem padrão."""
        exception = QueryError()
        assert str(exception) == ""


class TestExceptionHierarchy:
    """Testes para a hierarquia de exceções."""
    
    def test_exception_hierarchy(self):
        """Testa se todas as exceções herdam da classe base."""
        exceptions = [
            ObjectNotFound,
            MultipleObjectsReturned,
            ConnectionError,
            ValidationError,
            QueryError
        ]
        
        for exception_class in exceptions:
            assert issubclass(exception_class, CaspyORMException)
            assert issubclass(exception_class, Exception)
    
    def test_exception_instances(self):
        """Testa criação de instâncias de todas as exceções."""
        exceptions = [
            ObjectNotFound("test"),
            MultipleObjectsReturned("test"),
            ConnectionError("test"),
            ValidationError("test"),
            QueryError("test")
        ]
        
        for exception in exceptions:
            assert isinstance(exception, CaspyORMException)
            assert isinstance(exception, Exception)


class TestExceptionUsage:
    """Testes para uso prático das exceções."""
    
    def test_raise_object_not_found(self):
        """Testa levantamento de ObjectNotFound."""
        with pytest.raises(ObjectNotFound, match="Objeto não encontrado"):
            raise ObjectNotFound("Objeto não encontrado")
    
    def test_raise_multiple_objects_returned(self):
        """Testa levantamento de MultipleObjectsReturned."""
        with pytest.raises(MultipleObjectsReturned, match="Múltiplos objetos"):
            raise MultipleObjectsReturned("Múltiplos objetos")
    
    def test_raise_connection_error(self):
        """Testa levantamento de ConnectionError."""
        with pytest.raises(ConnectionError, match="Erro de conexão"):
            raise ConnectionError("Erro de conexão")
    
    def test_raise_validation_error(self):
        """Testa levantamento de ValidationError."""
        with pytest.raises(ValidationError, match="Erro de validação"):
            raise ValidationError("Erro de validação")
    
    def test_raise_query_error(self):
        """Testa levantamento de QueryError."""
        with pytest.raises(QueryError, match="Erro de query"):
            raise QueryError("Erro de query")
    
    def test_exception_chaining(self):
        """Testa encadeamento de exceções."""
        try:
            raise ConnectionError("Erro de conexão")
        except ConnectionError as e:
            with pytest.raises(ValidationError):
                raise ValidationError("Erro de validação") from e
    
    def test_exception_context(self):
        """Testa contexto das exceções."""
        exception = ValidationError("Campo obrigatório")
        assert "Campo obrigatório" in str(exception)
        
        # Testa se pode ser usada em estruturas de controle
        try:
            raise exception
        except ValidationError:
            pass  # Exceção capturada corretamente 