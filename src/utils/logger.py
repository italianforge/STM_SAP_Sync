import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional
from ..config.settings import Settings

class ColoredFormatter(logging.Formatter):
    """Formatter colorato per output console"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # Cyan
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[35m', # Magenta
        'RESET': '\033[0m'      # Reset
    }
    
    def format(self, record):
        if hasattr(record, 'levelname') and record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
        return super().format(record)

def setup_logger(name: str, log_file: Optional[str] = None) -> logging.Logger:
    """Configura un logger con le impostazioni standard"""
    logger = logging.getLogger(name)
    
    # Evita duplicazione handlers se il logger è già configurato
    if logger.hasHandlers():
        return logger
    
    logger.setLevel(getattr(logging, Settings.LOG_LEVEL))
    
    # Handler per console (se abilitato)
    if Settings.LOG_TO_CONSOLE:
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = ColoredFormatter(Settings.LOG_FORMAT)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # Handler per file (se abilitato)
    if Settings.LOG_TO_FILE:
        Settings.ensure_log_directory()
        
        # Determina il nome del file di log
        if log_file is None:
            log_file = f"{name.lower().replace('.', '_')}.log"
        
        log_path = Settings.LOG_DIR / log_file
        
        # Usa RotatingFileHandler per rotazione automatica
        file_handler = logging.handlers.RotatingFileHandler(
            log_path,
            maxBytes=Settings.LOG_FILE_MAX_SIZE,
            backupCount=Settings.LOG_FILE_BACKUP_COUNT,
            encoding='utf-8'
        )
        
        file_formatter = logging.Formatter(Settings.LOG_FORMAT)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger

def setup_application_logger() -> logging.Logger:
    """Configura il logger principale dell'applicazione"""
    return setup_logger("STM_SAP_Sync", "stm_sap_sync.log")

def setup_error_logger() -> logging.Logger:
    """Configura un logger specifico per gli errori"""
    error_logger = setup_logger("STM_SAP_Sync.Errors", "errors.log")
    # Imposta livello ERROR per questo logger specifico
    error_logger.setLevel(logging.ERROR)
    return error_logger

def setup_sync_logger(table_name: str) -> logging.Logger:
    """Configura un logger specifico per la sincronizzazione di una tabella"""
    return setup_logger(f"STM_SAP_Sync.Sync.{table_name}", f"sync_{table_name.lower()}.log")

def log_performance(logger: logging.Logger, operation: str, duration: float, record_count: int = 0):
    """Logga informazioni di performance"""
    if record_count > 0:
        rate = record_count / duration if duration > 0 else 0
        logger.info(f"PERFORMANCE - {operation}: {duration:.2f}s, {record_count} records, {rate:.2f} records/sec")
    else:
        logger.info(f"PERFORMANCE - {operation}: {duration:.2f}s")

def log_database_error(logger: logging.Logger, operation: str, error: Exception, table_name: str = None):
    """Logga errori del database con contesto"""
    context = f" on table {table_name}" if table_name else ""
    logger.error(f"DATABASE ERROR - {operation}{context}: {type(error).__name__}: {str(error)}")

def log_sync_stats(logger: logging.Logger, table_name: str, total_records: int, processed: int, errors: int, duration: float):
    """Logga statistiche di sincronizzazione"""
    success_rate = (processed / total_records * 100) if total_records > 0 else 0
    logger.info(f"SYNC STATS - {table_name}: {processed}/{total_records} records ({success_rate:.1f}%), {errors} errors, {duration:.2f}s")
