"""
Script principale per avviare la sincronizzazione SAP
"""
from src.sync.engine import SyncEngine
from src.config.database import DatabaseConfig
from src.utils.logger import setup_logger

def main():
    """Funzione principale"""
    logger = setup_logger("Main")
    
    try:
        # Inizializza configurazione database
        db_config = DatabaseConfig()
        
        # Crea engine di sincronizzazione
        sync_engine = SyncEngine(db_config)
        
        # Esegui sincronizzazione tabelle
        tables_to_sync = ["anagraficheArticoli"]
        
        for table_name in tables_to_sync:
            logger.info(f"Avvio sincronizzazione tabella: {table_name}")
            sync_engine.sync_table(table_name)
            logger.info(f"Completata sincronizzazione tabella: {table_name}")
            
    except Exception as e:
        logger.error(f"Errore durante l'esecuzione: {e}")
        raise

if __name__ == "__main__":
    main()
