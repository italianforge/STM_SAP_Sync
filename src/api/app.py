"""
Mini Flask API for STM_SAP_Sync.
Exposes endpoints to test the SAP MSSQL connection and trigger synchronization on demand.
Runs on port SAP_API_PORT (default 5001) as a daemon thread alongside the sync process.
"""
import os
import time
import logging
import threading

from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r'/api/*': {'origins': '*', 'methods': ['GET', 'POST', 'OPTIONS']}})


def _build_connection_string(data: dict) -> str:
    """
    Build a pyodbc MSSQL connection string from individual fields.
    data keys: sap_db_server, sap_db_port, sap_db_database, sap_db_username,
               sap_db_password, sap_db_driver
    """
    server = data.get('sap_db_server', '').strip()
    port = data.get('sap_db_port', '1433').strip()
    database = data.get('sap_db_database', '').strip()
    username = data.get('sap_db_username', '').strip()
    password = data.get('sap_db_password', '').strip()
    driver = data.get('sap_db_driver', 'SQL Server').strip()

    # URL-encode special chars in password
    from urllib.parse import quote_plus
    encoded_password = quote_plus(password)

    host = f'{server}:{port}' if port and port != '1433' else server
    driver_encoded = driver.replace(' ', '+')

    return (
        f'mssql+pyodbc://{username}:{encoded_password}@{host}/{database}'
        f'?driver={driver_encoded}&trusted_connection=no'
    )


@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """
    Test the SAP MSSQL connection.
    Expects JSON body with SAP connection fields.
    Returns: { success: bool, message: str, latency_ms: int }
    """
    data = request.get_json()
    if not data:
        return jsonify({'success': False, 'message': 'No data provided', 'latency_ms': 0}), 400

    if not data.get('sap_db_server') or not data.get('sap_db_database'):
        return jsonify({
            'success': False,
            'message': 'Campi obbligatori mancanti: sap_db_server, sap_db_database',
            'latency_ms': 0
        }), 400

    try:
        conn_str = _build_connection_string(data)
        engine = create_engine(conn_str, connect_args={'timeout': 10})

        start = time.monotonic()
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        latency_ms = int((time.monotonic() - start) * 1000)
        engine.dispose()

        return jsonify({
            'success': True,
            'message': f'Connessione riuscita al server {data.get("sap_db_server")}',
            'latency_ms': latency_ms,
        }), 200

    except SQLAlchemyError as e:
        error_str = str(e.orig) if hasattr(e, 'orig') and e.orig else str(e)
        logger.warning(f'SAP connection test failed: {error_str}')
        return jsonify({
            'success': False,
            'message': f'Connessione fallita: {error_str}',
            'latency_ms': 0,
        }), 200  # Return 200 so the caller can read the JSON body

    except Exception as e:
        logger.error(f'Unexpected error in SAP connection test: {e}')
        return jsonify({
            'success': False,
            'message': f'Errore imprevisto: {str(e)}',
            'latency_ms': 0,
        }), 200


@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200


# ─── Sync trigger ────────────────────────────────────────────────────────────

_sync_lock = threading.Lock()
_sync_status = {'running': False, 'last_result': None}
_reload_requested = False


def _run_sync_background():
    """Execute a full sync in a background thread, loading fresh credentials from DB."""
    global _sync_status
    try:
        from ..config.database import DatabaseConfig, get_postgres_setting
        from ..sync.engine import SyncEngine

        db_config = DatabaseConfig()
        article_group_filter = get_postgres_setting(
            'sap_articoli_itms_grp_cod', default='', postgres_url=db_config.postgres_url
        )
        sync_engine = SyncEngine(db_config, article_group_filter=article_group_filter)

        tables = [
            'anagraficheArticoli',
            'anagraficheBusinessPartner',
            'catalogoBusinessPartner',
            'ordiniAcquisto',
            'ordiniAcquistoLines',
        ]
        errors = []
        for table in tables:
            try:
                sync_engine.sync_table(table)
            except Exception as e:
                logger.error(f'Sync error for {table}: {e}')
                errors.append({'table': table, 'error': str(e)})

        _sync_status['last_result'] = {
            'success': len(errors) == 0,
            'errors': errors,
            'completed_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        }
        logger.info('Background sync completed')
    except Exception as e:
        logger.error(f'Background sync failed: {e}')
        _sync_status['last_result'] = {
            'success': False,
            'errors': [{'table': '*', 'error': str(e)}],
            'completed_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        }
    finally:
        _sync_status['running'] = False


@app.route('/api/sync', methods=['POST'])
def trigger_sync():
    """
    Trigger a full SAP sync using credentials stored in the PostgreSQL settings table.
    Returns immediately; sync runs in background.
    Returns: { accepted: bool, message: str }
    """
    if not _sync_lock.acquire(blocking=False):
        return jsonify({'accepted': False, 'message': 'Sync già in corso'}), 409

    _sync_status['running'] = True
    t = threading.Thread(target=_run_sync_background, daemon=True, name='on-demand-sync')
    t.start()
    _sync_lock.release()

    return jsonify({'accepted': True, 'message': 'Sincronizzazione avviata'}), 202


@app.route('/api/sync/status', methods=['GET'])
def sync_status():
    """Return current sync status and last result."""
    return jsonify({
        'running': _sync_status['running'],
        'last_result': _sync_status['last_result'],
    }), 200


@app.route('/api/reload-config', methods=['POST'])
def reload_config():
    """
    Signal the main scheduler loop to reload the sync interval from PostgreSQL settings.
    Sets a global flag that the scheduler loop checks between runs.
    """
    global _reload_requested
    _reload_requested = True
    logger.info('Reload config requested via API')
    return jsonify({'accepted': True, 'message': 'Ricaricamento configurazione richiesto'}), 200


def run_api_server():
    """Start the Flask API server. Intended to run in a daemon thread."""
    port = int(os.environ.get('SAP_API_PORT', 5001))
    logger.info(f'Starting SAP Sync API server on port {port}')
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
