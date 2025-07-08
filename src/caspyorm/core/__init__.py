"""
Core functionality for CaspyORM.

This module contains the main classes and functionality for working with
Cassandra databases through CaspyORM.
"""

from .model import Model
from .fields import (
    BaseField, Text, Integer, Float, Boolean, UUID, Timestamp,
    List, Set, Map, Tuple, UserDefinedType
)
from .query import QuerySet
from .connection import ConnectionManager, connect, disconnect

__all__ = [
    'Model',
    'BaseField', 'Text', 'Integer', 'Float', 'Boolean', 'UUID', 'Timestamp',
    'List', 'Set', 'Map', 'Tuple', 'UserDefinedType',
    'QuerySet',
    'ConnectionManager', 'connect', 'disconnect'
] 