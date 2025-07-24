import logging
from ..config.settings import Settings

def setup_logger(name: str) -> logging.Logger:
    """Configura un logger con le impostazioni standard"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:  # Evita duplicazione handlers
        handler = logging.StreamHandler()
        formatter = logging.Formatter(Settings.LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, Settings.LOG_LEVEL))
    
    return logger
