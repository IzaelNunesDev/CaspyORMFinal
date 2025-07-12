"""
Core functionality for CaspyORM.

This module contains the main classes and functionality for working with
Cassandra databases through CaspyORM.
"""

from .connection import ConnectionManager, connect, disconnect
from .fields import (
    UUID,
    BaseField,
    Boolean,
    Float,
    Integer,
    List,
    Map,
    Set,
    Text,
    Timestamp,
    Tuple,
    UserDefinedType,
)
from .model import Model
from .query import QuerySet

__all__ = [
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
]
