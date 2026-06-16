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
from flasgger import Swagger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={r'/api/*': {'origins': '*', 'methods': ['GET', 'POST', 'OPTIONS']}})

# ─── Swagger / OpenAPI documentation ─────────────────────────────────────────
_swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "STM SAP Sync API",
        "description": (
            "API di controllo per il servizio di sincronizzazione SAP → PostgreSQL.\n\n"
            "Permette di testare la connessione al database SAP MSSQL, avviare "
            "sincronizzazioni manuali e monitorarne lo stato."
        ),
        "version": "1.0.0",
    },
    "basePath": "/api",
    "consumes": ["application/json"],
    "produces": ["application/json"],
    "definitions": {
        "ConnectionTestRequest": {
            "type": "object",
            "required": ["sap_db_server", "sap_db_database"],
            "properties": {
                "sap_db_server": {"type": "string", "example": "192.168.1.100"},
                "sap_db_port": {"type": "string", "default": "1433", "example": "1433"},
                "sap_db_database": {"type": "string", "example": "SBO_COMPANY"},
                "sap_db_username": {"type": "string"},
                "sap_db_password": {"type": "string", "format": "password"},
                "sap_db_driver": {"type": "string", "default": "SQL Server"},
            },
        },
        "ConnectionTestResponse": {
            "type": "object",
            "properties": {
                "success": {"type": "boolean"},
                "message": {"type": "string"},
                "latency_ms": {"type": "integer"},
            },
        },
        "SyncStatus": {
            "type": "object",
            "properties": {
                "running": {"type": "boolean"},
                "last_result": {
                    "type": "object",
                    "properties": {
                        "success": {"type": "boolean"},
                        "errors": {"type": "array", "items": {"type": "object"}},
                        "completed_at": {"type": "string", "format": "date-time"},
                    },
                },
            },
        },
    },
}

Swagger(app, template=_swagger_template)
# ─────────────────────────────────────────────────────────────────────────────


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
        f'?driver={driver_encoded}&trusted_connection=no&TrustServerCertificate=yes'
    )


@app.route('/api/test-connection', methods=['POST'])
def test_connection():
    """
    Testa la connessione al database SAP MSSQL.
    ---
    tags:
      - Connessione SAP
    summary: Test connessione SAP MSSQL
    parameters:
      - in: body
        name: body
        required: true
        schema:
          $ref: '#/definitions/ConnectionTestRequest'
    responses:
      200:
        description: Risultato del test (anche in caso di fallimento connessione)
        schema:
          $ref: '#/definitions/ConnectionTestResponse'
      400:
        description: Parametri obbligatori mancanti
        schema:
          $ref: '#/definitions/ConnectionTestResponse'
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
    """
    Health check del servizio SAP Sync.
    ---
    tags:
      - Sistema
    summary: Health check
    responses:
      200:
        description: Servizio attivo
        schema:
          type: object
          properties:
            status:
              type: string
              example: ok
    """
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
        sync_engine = SyncEngine(db_config)

        tables = [
            'anagraficheBusinessPartner',
            'anagraficheArticoli',
            'catalogoBusinessPartner',
            'ordiniAcquisto',
            'ordiniAcquistoLines',
            'entrataMerci',
            'entrataMerciLines',
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
    Avvia una sincronizzazione completa SAP → PostgreSQL.
    ---
    tags:
      - Sincronizzazione
    summary: Avvia sync manuale
    description: >
      Lancia la sincronizzazione in background usando le credenziali SAP
      salvate nella tabella `settings` di PostgreSQL. Risponde immediatamente;
      usare `/api/sync/status` per monitorare l'avanzamento.
    responses:
      202:
        description: Sincronizzazione avviata
        schema:
          type: object
          properties:
            accepted:
              type: boolean
              example: true
            message:
              type: string
              example: Sincronizzazione avviata
      409:
        description: Una sincronizzazione è già in corso
        schema:
          type: object
          properties:
            accepted:
              type: boolean
              example: false
            message:
              type: string
              example: Sync già in corso
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
    """
    Stato corrente della sincronizzazione e risultato dell'ultima esecuzione.
    ---
    tags:
      - Sincronizzazione
    summary: Stato sincronizzazione
    responses:
      200:
        description: Stato corrente
        schema:
          $ref: '#/definitions/SyncStatus'
    """
    return jsonify({
        'running': _sync_status['running'],
        'last_result': _sync_status['last_result'],
    }), 200


@app.route('/api/reload-config', methods=['POST'])
def reload_config():
    """
    Richiede il ricaricamento della configurazione dal database PostgreSQL.
    ---
    tags:
      - Sistema
    summary: Ricarica configurazione
    description: >
      Segnala al loop principale dello scheduler di ricaricare
      l'intervallo di sincronizzazione dalla tabella `settings`.
    responses:
      200:
        description: Richiesta accettata
        schema:
          type: object
          properties:
            accepted:
              type: boolean
              example: true
            message:
              type: string
              example: Ricaricamento configurazione richiesto
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
