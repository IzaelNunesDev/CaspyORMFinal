"""
CaspyORM CLI - Command line interface for CaspyORM.

This package provides a powerful CLI for interacting with CaspyORM models,
managing migrations, and exploring your Cassandra database.
"""

__version__ = "0.1.0"

from .main import app

__all__ = ['app', '__version__'] 