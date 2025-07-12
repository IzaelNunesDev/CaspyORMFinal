"""
CaspyORM - A modern, type-safe ORM for Apache Cassandra.

CaspyORM provides a Pythonic interface to Apache Cassandra with support for:
- Type-safe model definitions
- User-Defined Types (UDTs)
- Batch operations
- Async/await support
- FastAPI integration
- CLI tools
"""

__version__ = "0.1.0"

# Core functionality
from .core import (
    UUID,
    BaseField,
    Boolean,
    ConnectionManager,
    Float,
    Integer,
    List,
    Map,
    Model,
    QuerySet,
    Set,
    Text,
    Timestamp,
    Tuple,
    UserDefinedType,
    connect,
    disconnect,
)

# Connection instance
from .core.connection import connection

# Custom types
from .types import BatchQuery, UserType

# Utilities
from .utils.exceptions import ValidationError
from .utils.logging import get_logger, setup_logging

# Optional integrations
try:
    from .contrib import (
        as_response_model,
        as_response_models,
        get_async_session,
        get_session,
    )
except ImportError:
    pass

__all__ = [
    # Core
    "Model",
    "BaseField",
    "Text",
    "Integer",
    "Float",
    "Boolean",
    "UUID",
    "Timestamp",
    "List",
    "Set",
    "Map",
    "Tuple",
    "UserDefinedType",
    "QuerySet",
    "ConnectionManager",
    "connect",
    "disconnect",
    "connection",
    # Types
    "UserType",
    "BatchQuery",
    # Utils
    "ValidationError",
    "ConnectionError",
    "QueryError",
    "setup_logging",
    "get_logger",
    # Version
    "__version__",
]
