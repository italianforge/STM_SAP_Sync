"""
Script principale per avviare la sincronizzazione SAP
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.sync.engine import SyncEngine
from src.config.database import DatabaseConfig
from src.utils.logger import setup_application_logger, setup_error_logger, log_performance

def sync_single_table(table_name: str, logger, error_logger) -> dict:
    """Sincronizza una singola tabella - funzione per threading"""
    table_start = time.time()
    logger.info(f"Avvio sincronizzazione tabella: {table_name}")
    
    try:
        # Ogni thread ha bisogno della propria istanza del database config e sync engine
        db_config = DatabaseConfig()
        sync_engine = SyncEngine(db_config)
        
        sync_engine.sync_table(table_name)
        table_duration = time.time() - table_start
        log_performance(logger, f"Sincronizzazione {table_name}", table_duration)
        logger.info(f"Completata sincronizzazione tabella: {table_name}")
        
        return {
            "table": table_name,
            "success": True,
            "duration": table_duration,
            "error": None
        }
        
    except Exception as e:
        table_duration = time.time() - table_start
        error_logger.error(f"Errore sincronizzazione {table_name} dopo {table_duration:.2f}s: {str(e)}")
        logger.error(f"Fallita sincronizzazione tabella: {table_name}")
        
        return {
            "table": table_name,
            "success": False,
            "duration": table_duration,
            "error": str(e)
        }

def main():
    """Funzione principale"""
    logger = setup_application_logger()
    error_logger = setup_error_logger()
    
    logger.info("=" * 50)
    logger.info("Avvio sincronizzazione STM SAP Sync (PARALLELA)")
    logger.info("=" * 50)
    
    start_time = time.time()
    
    try:
        # Inizializza configurazione database (per test connessione)
        db_config = DatabaseConfig()
        logger.info("Configurazione database inizializzata")
        
        # Esegui sincronizzazione tabelle in parallelo
        tables_to_sync = ["anagraficheArticoli", "anagraficheBusinessPartner", "catalogoBusinessPartner", "ordiniAcquisto", "ordiniAcquistoLines"]
        #tables_to_sync =["anagraficheArticoli"]
        # Configura il numero massimo di thread paralleli
        max_workers = min(len(tables_to_sync), 3)  # Massimo 3 thread per evitare sovraccarico DB
        logger.info(f"Avvio sincronizzazione parallela con {max_workers} thread per {len(tables_to_sync)} tabelle")
        
        results = []
        failed_tables = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Sottometti tutti i job
            future_to_table = {
                executor.submit(sync_single_table, table_name, logger, error_logger): table_name
                for table_name in tables_to_sync
            }
            
            # Raccogli i risultati man mano che completano
            for future in as_completed(future_to_table):
                result = future.result()
                results.append(result)
                
                if result["success"]:
                    logger.info(f"✓ {result['table']} completata in {result['duration']:.2f}s")
                else:
                    failed_tables.append(result['table'])
                    logger.error(f"✗ {result['table']} fallita in {result['duration']:.2f}s: {result['error']}")
        
        # Statistiche finali
        total_duration = time.time() - start_time
        successful_tables = len([r for r in results if r["success"]])
        total_sync_time = sum(r["duration"] for r in results)
        
        logger.info("=" * 50)
        logger.info("RIEPILOGO SINCRONIZZAZIONE PARALLELA")
        logger.info("=" * 50)
        logger.info(f"Tabelle totali: {len(tables_to_sync)}")
        logger.info(f"Tabelle riuscite: {successful_tables}")
        logger.info(f"Tabelle fallite: {len(failed_tables)}")
        logger.info(f"Tempo totale esecuzione: {total_duration:.2f}s")
        logger.info(f"Tempo totale sincronizzazioni: {total_sync_time:.2f}s")
        logger.info(f"Efficienza parallelizzazione: {(total_sync_time/total_duration):.1f}x")
        
        if failed_tables:
            logger.error(f"Tabelle fallite: {', '.join(failed_tables)}")
            raise Exception(f"Sincronizzazione fallita per {len(failed_tables)} tabelle: {', '.join(failed_tables)}")
        
        log_performance(logger, "Sincronizzazione parallela completa", total_duration)
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
