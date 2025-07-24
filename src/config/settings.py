import os
from typing import Optional

class Settings:
    """Configurazioni generali dell'applicazione"""
    
    # Configurazioni sincronizzazione
    DEFAULT_BATCH_SIZE = 1000
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5
    
    # Configurazioni logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    @classmethod
    def get_batch_size(cls) -> int:
        """Ottieni dimensione batch da env o default"""
        return int(os.getenv("BATCH_SIZE", cls.DEFAULT_BATCH_SIZE))
    
    @classmethod
    def get_max_retries(cls) -> int:
        """Ottieni numero massimo retry da env o default"""
        return int(os.getenv("MAX_RETRIES", cls.MAX_RETRIES))
