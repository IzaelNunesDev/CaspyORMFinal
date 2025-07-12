"""
Custom types for CaspyORM.

This module contains custom types like User-Defined Types (UDTs)
and batch operations.
"""

from .batch import BatchQuery
from .usertype import UserType

__all__ = ["UserType", "BatchQuery"]
