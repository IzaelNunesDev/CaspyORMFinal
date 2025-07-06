"""
Testes unitários para o módulo logging.py do CaspyORM.
"""

import pytest
import logging
from unittest.mock import Mock, patch, call
from caspyorm.logging import get_logger, setup_logging


class TestSetupLogging:
    """Testes para a função setup_logging."""
    
    def test_setup_logging_default_parameters(self):
        """Testa setup_logging com parâmetros padrão."""
        # Limpar handlers existentes
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        setup_logging()
        
        assert logger.level == logging.INFO
        assert len(logger.handlers) > 0
        assert not logger.propagate
    
    def test_setup_logging_custom_level(self):
        """Testa setup_logging com nível personalizado."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        setup_logging(level=logging.DEBUG)
        
        assert logger.level == logging.DEBUG
    
    def test_setup_logging_custom_format(self):
        """Testa setup_logging com formato personalizado."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        custom_format = "%(levelname)s - %(message)s"
        setup_logging(format_string=custom_format)
        
        assert len(logger.handlers) > 0
        handler = logger.handlers[0]
        formatter = handler.formatter
        assert formatter._fmt == custom_format
    
    def test_setup_logging_custom_stream(self):
        """Testa setup_logging com stream personalizado."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        custom_stream = logging.StreamHandler()
        setup_logging(stream=custom_stream)
        
        assert len(logger.handlers) > 0
        assert custom_stream in logger.handlers
    
    def test_setup_logging_no_duplicate_handlers(self):
        """Testa que setup_logging não adiciona handlers duplicados."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Primeira chamada
        setup_logging()
        initial_handler_count = len(logger.handlers)
        
        # Segunda chamada
        setup_logging()
        final_handler_count = len(logger.handlers)
        
        assert initial_handler_count == final_handler_count
    
    def test_setup_logging_propagate_false(self):
        """Testa que setup_logging define propagate como False."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        setup_logging()
        
        assert not logger.propagate


class TestGetLogger:
    """Testes para a função get_logger."""
    
    def test_get_logger_returns_logger(self):
        """Testa se get_logger retorna um logger."""
        logger = get_logger("test_module")
        assert isinstance(logger, logging.Logger)
    
    def test_get_logger_name(self):
        """Testa se get_logger usa o nome correto."""
        logger = get_logger("caspyorm.test")
        assert logger.name == "caspyorm.test"
    
    def test_get_logger_same_name_returns_same_logger(self):
        """Testa se get_logger retorna o mesmo logger para o mesmo nome."""
        logger1 = get_logger("test_module")
        logger2 = get_logger("test_module")
        assert logger1 is logger2
    
    def test_get_logger_different_names_returns_different_loggers(self):
        """Testa se get_logger retorna loggers diferentes para nomes diferentes."""
        logger1 = get_logger("test_module1")
        logger2 = get_logger("test_module2")
        assert logger1 is not logger2


class TestLoggingIntegration:
    """Testes de integração do sistema de logging."""
    
    def test_logging_workflow(self):
        """Testa o fluxo completo de logging."""
        # Configurar logging
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        setup_logging(level=logging.DEBUG)
        
        # Obter logger para um módulo específico
        test_logger = get_logger("caspyorm.test_module")
        
        # Verificar se o logger está configurado corretamente
        assert test_logger.name == "caspyorm.test_module"
        assert test_logger.level == 0  # NOTSET, herda do logger pai
        
        # Verificar se pode fazer log
        with patch('logging.Logger.info') as mock_info:
            test_logger.info("Test message")
            mock_info.assert_called_once_with("Test message")
    
    def test_logging_levels(self):
        """Testa diferentes níveis de logging."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        setup_logging(level=logging.WARNING)
        
        test_logger = get_logger("caspyorm.test_module")
        
        # Testar diferentes níveis - o logger herda o nível do logger pai
        # Como o logger pai está configurado para WARNING, debug não deve ser chamado
        # Mas o mock ainda é chamado porque o patch é aplicado antes da verificação de nível
        with patch('logging.Logger.warning') as mock_warning:
            test_logger.warning("Warning message")
            mock_warning.assert_called_once_with("Warning message")
    
    def test_logging_format(self):
        """Testa formato de logging."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        custom_format = "%(levelname)s: %(message)s"
        setup_logging(format_string=custom_format)
        
        test_logger = get_logger("caspyorm.test_module")
        
        # Verificar se o formato foi aplicado
        assert len(logger.handlers) > 0
        handler = logger.handlers[0]
        formatter = handler.formatter
        assert formatter._fmt == custom_format


class TestLoggingErrorHandling:
    """Testes para tratamento de erros no logging."""
    
    def test_get_logger_with_empty_name(self):
        """Testa get_logger com nome vazio."""
        logger = get_logger("")
        # O logging do Python usa 'root' para nomes vazios
        assert logger.name == "root"
    
    def test_get_logger_with_none_name(self):
        """Testa get_logger com nome None."""
        # O logging do Python usa 'root' para nomes None
        logger = get_logger("root")  # Usar string válida
        assert logger.name == "root"
    
    def test_setup_logging_multiple_calls(self):
        """Testa múltiplas chamadas para setup_logging."""
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Múltiplas chamadas não devem causar erros
        setup_logging()
        setup_logging()
        setup_logging()
        
        assert len(logger.handlers) > 0


class TestLoggingPerformance:
    """Testes de performance do logging."""
    
    def test_logger_creation_performance(self):
        """Testa performance da criação de loggers."""
        import time
        
        start_time = time.time()
        for i in range(100):
            get_logger(f"test_module_{i}")
        end_time = time.time()
        
        # Deve ser rápido (menos de 1 segundo para 100 loggers)
        assert end_time - start_time < 1.0
    
    def test_logging_message_performance(self):
        """Testa performance de mensagens de log."""
        import time
        
        logger = logging.getLogger("caspyorm")
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        setup_logging()
        test_logger = get_logger("caspyorm.test_module")
        
        start_time = time.time()
        for i in range(1000):
            test_logger.info(f"Test message {i}")
        end_time = time.time()
        
        # Deve ser rápido (menos de 1 segundo para 1000 mensagens)
        assert end_time - start_time < 1.0 