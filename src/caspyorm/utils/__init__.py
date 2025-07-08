"""
Utility modules for CaspyORM.

This module contains utility classes and functions including
exceptions and logging configuration.
"""

from .logging import setup_logging, get_logger

__all__ = [
    'setup_logging', 'get_logger'
] 