import os
from pathlib import Path
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
    
    # Configurazioni logging su file
    LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
    LOG_FILE_MAX_SIZE = int(os.getenv("LOG_FILE_MAX_SIZE", 10 * 1024 * 1024))  # 10MB
    LOG_FILE_BACKUP_COUNT = int(os.getenv("LOG_FILE_BACKUP_COUNT", 5))
    LOG_TO_FILE = os.getenv("LOG_TO_FILE", "true").lower() == "true"
    LOG_TO_CONSOLE = os.getenv("LOG_TO_CONSOLE", "true").lower() == "true"
    
    @classmethod
    def get_batch_size(cls) -> int:
        """Ottieni dimensione batch da env o default"""
        return int(os.getenv("BATCH_SIZE", cls.DEFAULT_BATCH_SIZE))
    
    @classmethod
    def get_max_retries(cls) -> int:
        """Ottieni numero massimo retry da env o default"""
        return int(os.getenv("MAX_RETRIES", cls.MAX_RETRIES))
    
    @classmethod
    def ensure_log_directory(cls) -> Path:
        """Assicura che la directory dei log esista"""
        cls.LOG_DIR.mkdir(exist_ok=True)
        return cls.LOG_DIR
