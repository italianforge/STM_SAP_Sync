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
import smtplib
import json
import base64
import email.mime.multipart
import email.mime.text
import email.mime.base
from email import encoders
from datetime import datetime, timezone

# Flag per shutdown graceful
_shutdown_event = threading.Event()

TABLES_TO_SYNC = [
    "anagraficheBusinessPartner",
    "anagraficheArticoli",
    "catalogoBusinessPartner",
    "ordiniAcquisto",
    "ordiniAcquistoLines",
]

# Testata prima delle righe: FK entrata_merci_lines -> entrata_merci (ON DELETE CASCADE)
ENTRATA_MERCI_TABLES = [
    "entrataMerci",
    "entrataMerciLines",
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


def _cleanup_orphan_order_headers(logger) -> None:
    """Rimuove testate ordine senza righe valide importate.
    Eseguita sequenzialmente dopo tutti i sync paralleli.
    """
    try:
        from sqlalchemy import create_engine, text
        db_config = DatabaseConfig()
        engine = create_engine(db_config.postgres_url)
        with engine.connect() as conn:
            result = conn.execute(text("""
                DELETE FROM sap.ordini_acquisto
                WHERE id NOT IN (
                    SELECT DISTINCT cod_documento FROM sap.ordini_acquisto_lines
                )
            """))
            conn.commit()
            deleted = result.rowcount
        engine.dispose()
        if deleted:
            logger.info(f"Cleanup ordini: rimossi {deleted} ordini senza righe valide")
    except Exception as e:
        logger.warning(f"Cleanup orphan headers fallito (non bloccante): {e}")


def _cleanup_orphan_entrata_merci_lines(logger) -> None:
    """Rimuove righe entrata merci senza testata (non dovrebbero esistere con FK attiva)."""
    try:
        from sqlalchemy import create_engine, text
        db_config = DatabaseConfig()
        engine = create_engine(db_config.postgres_url)
        with engine.connect() as conn:
            result = conn.execute(text("""
                DELETE FROM sap.entrata_merci_lines l
                WHERE NOT EXISTS (
                    SELECT 1 FROM sap.entrata_merci e WHERE e.id = l.cod_entrata_merci
                )
            """))
            conn.commit()
            deleted = result.rowcount
        engine.dispose()
        if deleted:
            logger.info(f"Cleanup entrata merci: rimosse {deleted} righe orfane")
    except Exception as e:
        logger.warning(f"Cleanup orphan entrata merci lines fallito (non bloccante): {e}")


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
        logger.info(f"Riepilogo parallelo: {successful}/{len(TABLES_TO_SYNC)} riuscite in {total_duration:.2f}s")

        for table_name in ENTRATA_MERCI_TABLES:
            result = sync_single_table(table_name, logger, error_logger)
            results.append(result)
            if result["success"]:
                logger.info(f"✓ {result['table']} completata in {result['duration']:.2f}s")
            else:
                failed_tables.append(result['table'])
                logger.error(f"✗ {result['table']} fallita in {result['duration']:.2f}s: {result['error']}")

        # Cleanup sequenziale: rimuove testate senza righe (entrambe le sync sono completate)
        _cleanup_orphan_order_headers(logger)
        _cleanup_orphan_entrata_merci_lines(logger)

        total_tables = len(TABLES_TO_SYNC) + len(ENTRATA_MERCI_TABLES)
        successful = len([r for r in results if r["success"]])
        logger.info(f"Riepilogo totale: {successful}/{total_tables} riuscite")

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


def _get_scheduler_base_url() -> str:
    """Base URL for STM_Scheduler REST API."""
    return os.environ.get('SCHEDULER_URL', 'http://localhost:5000')


def _get_email_settings(logger) -> dict | None:
    """Fetch email/SMTP settings from STM_Scheduler API. Returns None on error."""
    try:
        import requests
        url = f"{_get_scheduler_base_url()}/api/v1/resources/settings/email"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        logger.warning(f"Impossibile caricare email settings: HTTP {resp.status_code}")
    except Exception as e:
        logger.warning(f"Errore lettura email settings: {e}")
    return None


def dispatch_email_outbox(logger, error_logger) -> int:
    """
    Poll pending messages from email_outbox, dispatch via SMTP, update status.
    Returns number of messages processed.
    """
    import requests

    base = _get_scheduler_base_url()
    processed = 0

    try:
        resp = requests.get(f"{base}/api/v1/resources/notifications/outbox?status=pending&limit=20", timeout=10)
        if resp.status_code != 200:
            return 0
        messages = resp.json()
    except Exception as e:
        logger.warning(f"dispatch_email_outbox: impossibile leggere outbox: {e}")
        return 0

    if not messages:
        return 0

    email_cfg = _get_email_settings(logger)
    if not email_cfg:
        logger.warning("dispatch_email_outbox: configurazione SMTP non disponibile, skip")
        return 0

    smtp_host = email_cfg.get('smtp_host', '').strip()
    if not smtp_host:
        logger.warning("dispatch_email_outbox: smtp_host non configurato, skip")
        return 0

    try:
        smtp_port = int(email_cfg.get('smtp_port') or 587)
    except (ValueError, TypeError):
        smtp_port = 587

    smtp_user = email_cfg.get('smtp_user', '').strip()
    smtp_password = email_cfg.get('smtp_password', '').strip()
    use_tls = str(email_cfg.get('smtp_use_tls', 'true')).lower() == 'true'
    from_addr = email_cfg.get('smtp_from_address', smtp_user).strip()
    from_name = email_cfg.get('smtp_from_name', 'STM Scheduler').strip()

    try:
        max_retries = int(email_cfg.get('notification_max_retries') or 3)
    except (ValueError, TypeError):
        max_retries = 3
    try:
        backoff_minutes = int(email_cfg.get('notification_retry_backoff_minutes') or 15)
    except (ValueError, TypeError):
        backoff_minutes = 15

    for msg in messages:
        msg_id = msg['id']
        try:
            recipients = json.loads(msg.get('recipients', '[]'))
            if not recipients:
                _patch_outbox(base, msg_id, {'status': 'skipped', 'last_error': 'no recipients'}, logger)
                continue

            # Build MIME message
            mime = email.mime.multipart.MIMEMultipart('mixed')
            mime['Subject'] = msg['subject']
            mime['From'] = f"{from_name} <{from_addr}>" if from_name else from_addr
            mime['To'] = ', '.join(recipients)
            if email_cfg.get('smtp_reply_to', '').strip():
                mime.add_header('Reply-To', email_cfg['smtp_reply_to'].strip())

            body_part = email.mime.text.MIMEText(msg['body_html'], 'html', 'utf-8')
            mime.attach(body_part)

            # Optional XLSX attachment
            if msg.get('attachment_xlsx'):
                xlsx_bytes = base64.b64decode(msg['attachment_xlsx'])
                part = email.mime.base.MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                part.set_payload(xlsx_bytes)
                encoders.encode_base64(part)
                event_type = msg.get('event_type', 'attachment')
                part.add_header('Content-Disposition', 'attachment',
                                filename=f"{event_type}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.xlsx")
                mime.attach(part)

            # SMTP dispatch
            if use_tls:
                server = smtplib.SMTP(smtp_host, smtp_port, timeout=30)
                server.ehlo()
                server.starttls()
                server.ehlo()
            else:
                server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30)

            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)

            server.sendmail(from_addr, recipients, mime.as_string())
            server.quit()

            now_iso = datetime.now(timezone.utc).isoformat()
            _patch_outbox(base, msg_id, {'status': 'sent', 'sent_at': now_iso}, logger)
            logger.info(f"Email inviata: outbox_id={msg_id} event_type={msg.get('event_type')} to={recipients}")
            processed += 1

        except smtplib.SMTPException as e:
            error_logger.error(f"SMTP error outbox_id={msg_id}: {e}")
            retry_count = int(msg.get('retry_count', 0)) + 1
            if retry_count >= max_retries:
                _patch_outbox(base, msg_id, {'status': 'skipped', 'retry_count': retry_count,
                                              'last_error': f'SMTP: {e}'}, logger)
            else:
                from datetime import timedelta
                next_retry_minutes = retry_count * backoff_minutes
                next_retry = (datetime.now(timezone.utc) + timedelta(minutes=next_retry_minutes)).isoformat()
                _patch_outbox(base, msg_id, {'status': 'failed', 'retry_count': retry_count,
                                              'last_error': f'SMTP: {e}', 'next_retry_at': next_retry}, logger)
        except Exception as e:
            error_logger.error(f"dispatch_email_outbox error outbox_id={msg_id}: {e}")
            retry_count = int(msg.get('retry_count', 0)) + 1
            if retry_count >= max_retries:
                _patch_outbox(base, msg_id, {'status': 'skipped', 'retry_count': retry_count,
                                              'last_error': str(e)}, logger)
            else:
                from datetime import timedelta
                next_retry_minutes = retry_count * backoff_minutes
                next_retry = (datetime.now(timezone.utc) + timedelta(minutes=next_retry_minutes)).isoformat()
                _patch_outbox(base, msg_id, {'status': 'failed', 'retry_count': retry_count,
                                              'last_error': str(e), 'next_retry_at': next_retry}, logger)

    return processed


def _patch_outbox(base_url: str, msg_id: int, payload: dict, logger) -> None:
    """PATCH outbox message status — fire-and-forget."""
    try:
        import requests
        requests.patch(
            f"{base_url}/api/v1/resources/notifications/outbox/{msg_id}",
            json=payload,
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"_patch_outbox id={msg_id}: {e}")


def _trigger_scheduled_notifications(logger, now: 'datetime') -> None:
    """
    Check if rfq_digest or understock_report should fire for the current hour.
    Calls the STM_Scheduler enqueue endpoints which handle deduplication.
    """
    import requests

    base = _get_scheduler_base_url()

    try:
        from sqlalchemy import create_engine, text as sa_text
        from src.config.database import DatabaseConfig
        db_config = DatabaseConfig()
        engine = create_engine(db_config.postgres_url)
        with engine.connect() as conn:
            rows = conn.execute(sa_text(
                "SELECT key, value FROM settings WHERE key IN ("
                "'rfq_digest_enabled','rfq_digest_hour',"
                "'understock_report_enabled','understock_report_hour')"
            )).fetchall()
        engine.dispose()
        cfg_map = {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.warning(f"_trigger_scheduled_notifications: impossibile leggere settings: {e}")
        return

    current_hour = now.hour

    # RFQ Digest
    if str(cfg_map.get('rfq_digest_enabled', 'false')).lower() == 'true':
        try:
            digest_hour = int(cfg_map.get('rfq_digest_hour', '0'))
        except (ValueError, TypeError):
            digest_hour = 0
        if current_hour == digest_hour:
            try:
                resp = requests.post(f"{base}/api/v1/resources/notifications/enqueue/rfq-digest", timeout=30)
                if resp.status_code == 409:
                    logger.debug("rfq_digest: già inviato per oggi, skip")
                elif resp.status_code == 200:
                    data = resp.json()
                    logger.info(f"rfq_digest trigger: run_id={data.get('run_id')} count={data.get('rfq_count')}")
                else:
                    logger.warning(f"rfq_digest trigger HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.warning(f"rfq_digest trigger error: {e}")

    # Understock Report
    if str(cfg_map.get('understock_report_enabled', 'false')).lower() == 'true':
        try:
            report_hour = int(cfg_map.get('understock_report_hour', '7'))
        except (ValueError, TypeError):
            report_hour = 7
        if current_hour == report_hour:
            try:
                resp = requests.post(f"{base}/api/v1/resources/notifications/enqueue/understock-report", timeout=30)
                if resp.status_code == 409:
                    logger.debug("understock_report: già inviato per oggi, skip")
                elif resp.status_code == 200:
                    data = resp.json()
                    logger.info(f"understock_report trigger: run_id={data.get('run_id')} count={data.get('articoli_count')}")
                else:
                    logger.warning(f"understock_report trigger HTTP {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logger.warning(f"understock_report trigger error: {e}")


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

        # Riconcilia RFQ aperte con ordini di acquisto appena sincronizzati
        try:
            from src.sync.reconciler import reconcile_rfq_with_orders
            db_config_rec = DatabaseConfig()
            reconcile_rfq_with_orders(db_config_rec.postgres_url, logger)
        except Exception as e:
            error_logger.error(f"Post-sync RFQ reconciliation error: {e}")

        # Dispatch eventuali email in coda dopo ogni sync
        try:
            dispatch_email_outbox(logger, error_logger)
        except Exception as e:
            error_logger.error(f"Post-sync email dispatch error: {e}")

        # Trigger notifiche schedulate (primo controllo del ciclo)
        try:
            _trigger_scheduled_notifications(logger, datetime.now(timezone.utc))
        except Exception as e:
            error_logger.error(f"Initial notification trigger error: {e}")

        # Attendi fino al prossimo ciclo, con possibilità di interruzione e reload
        logger.info(f"Prossima sincronizzazione tra {interval_minutes} minuti")
        elapsed = 0
        check_interval = 10  # controlla ogni 10 secondi
        email_dispatch_seconds = 5 * 60   # dispatch email ogni 5 minuti
        elapsed_since_dispatch = 0
        elapsed_since_trigger = 0

        while elapsed < interval_seconds and not _shutdown_event.is_set():
            time.sleep(check_interval)
            elapsed += check_interval
            elapsed_since_dispatch += check_interval
            elapsed_since_trigger += check_interval

            # Ricarica intervallo se richiesto dalla API
            if api_module._reload_requested:
                api_module._reload_requested = False
                new_interval = _get_sync_interval(logger)
                if new_interval != interval_minutes:
                    logger.info(f"Intervallo aggiornato da {interval_minutes} a {new_interval} minuti, riavvio ciclo")
                    interval_minutes = new_interval
                    interval_seconds = new_interval * 60
                    elapsed = 0

            # Dispatch email outbox ogni 5 minuti
            if elapsed_since_dispatch >= email_dispatch_seconds:
                elapsed_since_dispatch = 0
                try:
                    n = dispatch_email_outbox(logger, error_logger)
                    if n:
                        logger.info(f"Email dispatch: {n} messaggio/i inviato/i")
                except Exception as e:
                    error_logger.error(f"Email dispatch error: {e}")

            # Trigger notifiche schedulate ogni 60 minuti
            if elapsed_since_trigger >= 3600:
                elapsed_since_trigger = 0
                try:
                    _trigger_scheduled_notifications(logger, datetime.now(timezone.utc))
                except Exception as e:
                    error_logger.error(f"Scheduled notification trigger error: {e}")

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

