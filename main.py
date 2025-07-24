"""
Script principale per avviare la sincronizzazione SAP
"""
import time
from src.sync.engine import SyncEngine
from src.config.database import DatabaseConfig
from src.utils.logger import setup_application_logger, setup_error_logger, log_performance

def main():
    """Funzione principale"""
    logger = setup_application_logger()
    error_logger = setup_error_logger()
    
    logger.info("=" * 50)
    logger.info("Avvio sincronizzazione STM SAP Sync")
    logger.info("=" * 50)
    
    start_time = time.time()
    
    try:
        # Inizializza configurazione database
        db_config = DatabaseConfig()
        logger.info("Configurazione database inizializzata")
        
        # Crea engine di sincronizzazione
        sync_engine = SyncEngine(db_config)
        logger.info("Engine di sincronizzazione creato")
        
        # Esegui sincronizzazione tabelle
        tables_to_sync = ["anagraficheArticoli"]
        
        for table_name in tables_to_sync:
            table_start = time.time()
            logger.info(f"Avvio sincronizzazione tabella: {table_name}")
            
            try:
                sync_engine.sync_table(table_name)
                table_duration = time.time() - table_start
                log_performance(logger, f"Sincronizzazione {table_name}", table_duration)
                logger.info(f"Completata sincronizzazione tabella: {table_name}")
            except Exception as e:
                table_duration = time.time() - table_start
                error_logger.error(f"Errore sincronizzazione {table_name} dopo {table_duration:.2f}s: {str(e)}")
                logger.error(f"Fallita sincronizzazione tabella: {table_name}")
                raise
        
        total_duration = time.time() - start_time
        log_performance(logger, "Sincronizzazione completa", total_duration)
        logger.info("Sincronizzazione completata con successo")
        
    except Exception as e:
        total_duration = time.time() - start_time
        error_logger.error(f"Errore critico dopo {total_duration:.2f}s: {str(e)}")
        logger.error(f"Errore durante l'esecuzione: {e}")
        raise
    finally:
        logger.info("=" * 50)
        logger.info("Fine sincronizzazione STM SAP Sync")
        logger.info("=" * 50)

if __name__ == "__main__":
    main()
