"""
STM SAP Sync — servizio persistente con scheduler automatico.

Avvia un'API Flask in background (porta SAP_API_PORT, default 5001) e
sincronizza le tabelle SAP verso PostgreSQL a intervalli configurabili.

L'intervallo viene letto dalla tabella `settings` su PostgreSQL
(chiave `sap_sync_interval_minutes`, default 60).
"""
import os
import time
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.sync.engine import SyncEngine
from src.config.database import DatabaseConfig
from src.utils.logger import setup_application_logger, setup_error_logger, log_performance
from src.api.app import run_api_server, _sync_status, _sync_lock

# Flag per shutdown graceful
_shutdown_event = threading.Event()

TABLES_TO_SYNC = [
    "anagraficheArticoli",
    "anagraficheBusinessPartner",
    "catalogoBusinessPartner",
    "ordiniAcquisto",
    "ordiniAcquistoLines",
]

DEFAULT_INTERVAL_MINUTES = 60
SETTINGS_KEY = 'sap_sync_interval_minutes'


def _get_sync_interval(logger) -> int:
    """Legge l'intervallo di sincronizzazione da PostgreSQL settings. Fallback al default."""
    try:
        from sqlalchemy import create_engine, text
        db_config = DatabaseConfig()
        engine = create_engine(db_config.postgres_url)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT value FROM settings WHERE key = :key"),
                {'key': SETTINGS_KEY}
            ).fetchone()
        engine.dispose()
        if row and row[0]:
            return max(1, int(row[0]))
    except Exception as e:
        logger.warning(f"Impossibile leggere intervallo da DB, uso default {DEFAULT_INTERVAL_MINUTES}m: {e}")
    return DEFAULT_INTERVAL_MINUTES


def sync_single_table(table_name: str, logger, error_logger) -> dict:
    """Sincronizza una singola tabella — funzione per threading."""
    table_start = time.time()
    logger.info(f"Avvio sincronizzazione tabella: {table_name}")
    try:
        db_config = DatabaseConfig()
        sync_engine = SyncEngine(db_config)
        sync_engine.sync_table(table_name)
        table_duration = time.time() - table_start
        log_performance(logger, f"Sincronizzazione {table_name}", table_duration)
        logger.info(f"Completata sincronizzazione tabella: {table_name}")
        return {"table": table_name, "success": True, "duration": table_duration, "error": None}
    except Exception as e:
        table_duration = time.time() - table_start
        error_logger.error(f"Errore sincronizzazione {table_name} dopo {table_duration:.2f}s: {str(e)}")
        logger.error(f"Fallita sincronizzazione tabella: {table_name}")
        return {"table": table_name, "success": False, "duration": table_duration, "error": str(e)}


def run_full_sync(logger, error_logger) -> dict:
    """
    Esegue la sincronizzazione completa in parallelo.
    Aggiorna _sync_status durante l'esecuzione.
    Restituisce il dict di risultato.
    """
    import src.api.app as api_module

    # Segna come in esecuzione
    with api_module._sync_lock:
        if api_module._sync_status['running']:
            logger.warning("Sincronizzazione già in corso, skip.")
            return None
        api_module._sync_status['running'] = True

    start_time = time.time()
    results = []
    failed_tables = []

    try:
        max_workers = min(len(TABLES_TO_SYNC), 3)
        logger.info(f"Avvio sincronizzazione parallela con {max_workers} thread per {len(TABLES_TO_SYNC)} tabelle")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_table = {
                executor.submit(sync_single_table, t, logger, error_logger): t
                for t in TABLES_TO_SYNC
            }
            for future in as_completed(future_to_table):
                result = future.result()
                results.append(result)
                if result["success"]:
                    logger.info(f"✓ {result['table']} completata in {result['duration']:.2f}s")
                else:
                    failed_tables.append(result['table'])
                    logger.error(f"✗ {result['table']} fallita in {result['duration']:.2f}s: {result['error']}")

        total_duration = time.time() - start_time
        successful = len([r for r in results if r["success"]])
        logger.info(f"Riepilogo: {successful}/{len(TABLES_TO_SYNC)} riuscite in {total_duration:.2f}s")

        sync_result = {
            'success': len(failed_tables) == 0,
            'errors': [{'table': r['table'], 'error': r['error']} for r in results if not r["success"]],
            'completed_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        }
    except Exception as e:
        error_logger.error(f"Errore critico durante sincronizzazione: {str(e)}")
        sync_result = {
            'success': False,
            'errors': [{'table': '*', 'error': str(e)}],
            'completed_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        }
    finally:
        api_module._sync_status['running'] = False
        api_module._sync_status['last_result'] = sync_result

    return sync_result


def scheduler_loop(logger, error_logger):
    """
    Loop principale dello scheduler.
    Legge l'intervallo dal DB, esegue la sync, aspetta, ripete.
    Controlla _reload_requested per ricaricare l'intervallo al volo.
    """
    import src.api.app as api_module

    logger.info("Scheduler loop avviato")

    while not _shutdown_event.is_set():
        # Leggi (o rileggi) l'intervallo configurato
        interval_minutes = _get_sync_interval(logger)
        interval_seconds = interval_minutes * 60
        logger.info(f"Intervallo sincronizzazione: {interval_minutes} minuti")

        # Esegui sincronizzazione
        logger.info("=" * 50)
        logger.info("Avvio ciclo di sincronizzazione")
        logger.info("=" * 50)
        run_full_sync(logger, error_logger)

        # Attendi fino al prossimo ciclo, con possibilità di interruzione e reload
        logger.info(f"Prossima sincronizzazione tra {interval_minutes} minuti")
        elapsed = 0
        check_interval = 10  # controlla ogni 10 secondi
        while elapsed < interval_seconds and not _shutdown_event.is_set():
            time.sleep(check_interval)
            elapsed += check_interval

            # Ricarica intervallo se richiesto dalla API
            if api_module._reload_requested:
                api_module._reload_requested = False
                new_interval = _get_sync_interval(logger)
                if new_interval != interval_minutes:
                    logger.info(f"Intervallo aggiornato da {interval_minutes} a {new_interval} minuti, riavvio ciclo")
                    interval_minutes = new_interval
                    interval_seconds = new_interval * 60
                    # Reset timer
                    elapsed = 0

    logger.info("Scheduler loop terminato")


def main():
    logger = setup_application_logger()
    error_logger = setup_error_logger()

    logger.info("=" * 50)
    logger.info("Avvio STM SAP Sync (servizio persistente)")
    logger.info("=" * 50)

    # Gestione SIGINT/SIGTERM per shutdown graceful
    def _handle_signal(signum, frame):
        logger.info(f"Segnale {signum} ricevuto, shutdown in corso...")
        _shutdown_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Avvia API server in background
    api_thread = threading.Thread(target=run_api_server, daemon=True, name='sap-api-server')
    api_thread.start()
    logger.info("API server thread avviato")

    # Avvia scheduler loop nel thread principale
    try:
        scheduler_loop(logger, error_logger)
    except Exception as e:
        error_logger.error(f"Errore fatale nello scheduler: {str(e)}")
        raise
    finally:
        logger.info("=" * 50)
        logger.info("STM SAP Sync terminato")
        logger.info("=" * 50)


if __name__ == "__main__":
    main()

