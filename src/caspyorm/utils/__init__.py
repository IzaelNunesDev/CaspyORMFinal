"""
Utility modules for CaspyORM.

This module contains utility classes and functions including
exceptions and logging configuration.
"""

from .logging import get_logger, setup_logging

__all__ = ["setup_logging", "get_logger"]
