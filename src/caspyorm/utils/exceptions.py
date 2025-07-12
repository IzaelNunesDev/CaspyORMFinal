# caspyorm/exceptions.py


class CaspyORMException(Exception):
    """Classe base para todas as exceções da CaspyORM."""

    pass


class ObjectNotFound(CaspyORMException):
    """Levantada quando um objeto esperado não é encontrado no banco de dados."""

    pass


class MultipleObjectsReturned(CaspyORMException):
    """Levantada quando mais de um objeto é retornado por uma query que esperava apenas um."""

    pass


class ConnectionError(CaspyORMException):
    """Levantada quando há um problema com a conexão ao banco de dados."""

    pass


class ValidationError(CaspyORMException):
    """Levantada quando há um erro de validação nos dados."""

    pass


class QueryError(CaspyORMException):
    """Levantada quando há um erro na construção ou execução de uma query."""

    pass


class TimeoutError(CaspyORMException):
    """Levantada quando uma operação excede o tempo limite."""

    pass


class LWTError(CaspyORMException):
    """Levantada quando uma transação leve (IF) falha."""

    def __init__(self, message="LWT condition was not met.", existing=None):
        super().__init__(message)
        self.existing = existing
