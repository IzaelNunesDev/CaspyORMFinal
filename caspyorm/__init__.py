# caspyorm/__init__.py
# Exposição da API pública

from .model import Model
from . import fields
from . import logging

# Configurar logging padrão
logging.setup_logging()

# ... existing code ... 