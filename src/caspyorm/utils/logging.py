"""
Módulo de logging centralizado para CaspyORM.

Este módulo configura o sistema de logging da biblioteca, fornecendo
loggers configurados adequadamente para diferentes componentes.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    format_string: Optional[str] = None,
    stream: Optional[logging.StreamHandler] = None,
) -> None:
    """
    Configura o sistema de logging da CaspyORM.

    Args:
        level: Nível de logging (default: INFO)
        format_string: String de formatação personalizada
        stream: Handler de stream personalizado
    """
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    if stream is None:
        stream = logging.StreamHandler(sys.stdout)

    # Configurar o logger raiz da CaspyORM
    logger = logging.getLogger("caspyorm")
    logger.setLevel(level)

    # Evitar duplicação de handlers
    if not logger.handlers:
        formatter = logging.Formatter(format_string)
        stream.setFormatter(formatter)
        logger.addHandler(stream)

    # Configurar propagação para evitar logs duplicados
    logger.propagate = False


def get_logger(name: str) -> logging.Logger:
    """
    Obtém um logger configurado para o módulo especificado.

    Args:
        name: Nome do módulo (ex: "caspyorm.connection")

    Returns:
        Logger configurado
    """
    return logging.getLogger(name)


# Configuração padrão do logging
setup_logging()
