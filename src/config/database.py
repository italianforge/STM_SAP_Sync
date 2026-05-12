import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import logging

logger = logging.getLogger(__name__)

_SAP_SETTING_KEYS = [
    'sap_db_server',
    'sap_db_port',
    'sap_db_database',
    'sap_db_username',
    'sap_db_password',
    'sap_db_driver',
]


def _decrypt(token: str) -> str | None:
    """Decrypt a Fernet token using ENCRYPTION_KEY from environment. Returns None on failure."""
    if not token:
        return None
    try:
        from cryptography.fernet import Fernet, InvalidToken
        key = os.environ.get('ENCRYPTION_KEY')
        if not key:
            return None
        f = Fernet(key.encode())
        return f.decrypt(token.encode()).decode()
    except Exception:
        return None


def _build_sap_url_from_fields(server, port, database, username, password, driver) -> str:
    from urllib.parse import quote_plus
    encoded_password = quote_plus(password or '')
    host = f'{server}:{port}' if port and str(port) != '1433' else server
    driver_encoded = (driver or 'SQL Server').replace(' ', '+')
    return (
        f'mssql+pyodbc://{username}:{encoded_password}@{host}/{database}'
        f'?driver={driver_encoded}&trusted_connection=no'
    )


def get_postgres_setting(key: str, default: str | None = None, postgres_url: str | None = None) -> str | None:
    """
    Read a single value from the PostgreSQL settings table.
    Returns default if the key is not found or on error.
    """
    try:
        url = postgres_url or os.environ.get('POSTGRES_URL')
        if not url:
            return default
        engine = create_engine(url)
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT value FROM settings WHERE key = :key"),
                {'key': key}
            ).fetchone()
        engine.dispose()
        if row and row[0] is not None:
            return str(row[0])
    except Exception as e:
        logger.warning(f'Could not read setting {key!r} from PostgreSQL: {e}')
    return default


def _load_sap_url_from_postgres(postgres_url: str) -> str | None:
    """
    Query the PostgreSQL settings table for SAP credentials.
    Returns a complete SAP_DB_URL string or None if not configured.
    """
    try:
        engine = create_engine(postgres_url)
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT key, value FROM settings WHERE key = ANY(:keys)"),
                {'keys': _SAP_SETTING_KEYS}
            ).fetchall()
        engine.dispose()

        settings = {row[0]: row[1] for row in rows}
        server = settings.get('sap_db_server', '').strip()
        if not server:
            return None  # Not configured in DB

        password_token = settings.get('sap_db_password', '')
        password = _decrypt(password_token) or password_token  # fallback to raw if unencrypted

        return _build_sap_url_from_fields(
            server=server,
            port=settings.get('sap_db_port', '1433'),
            database=settings.get('sap_db_database', ''),
            username=settings.get('sap_db_username', ''),
            password=password,
            driver=settings.get('sap_db_driver', 'SQL Server'),
        )
    except Exception as e:
        logger.warning(f'Could not load SAP settings from PostgreSQL: {e}')
        return None


class DatabaseConfig:
    """Gestione configurazione e connessioni database"""

    def __init__(self):
        # Determina quale file .env caricare in base alla variabile ENV
        env = os.getenv("ENV", "development")
        env_file_map = {
            "development": ".env",
            "test": ".env.test",
            "production": ".env.prod"
        }

        env_file = env_file_map.get(env, ".env")
        load_dotenv(env_file)

        self.postgres_url = os.getenv("POSTGRES_URL")
        self.environment = os.getenv("ENVIRONMENT", env)

        # Try to read SAP connection from PostgreSQL settings table first
        sap_url_from_db = None
        if self.postgres_url:
            sap_url_from_db = _load_sap_url_from_postgres(self.postgres_url)

        if sap_url_from_db:
            logger.info('SAP connection loaded from PostgreSQL settings table')
            self.sap_db_url = sap_url_from_db
        else:
            logger.info('SAP connection loaded from .env file (fallback)')
            self.sap_db_url = os.getenv("SAP_DB_URL")

        # Crea engines
        self.pg_engine = create_engine(self.postgres_url)
        self.sap_engine = create_engine(self.sap_db_url)

        # Crea session makers
        self.PGSession = sessionmaker(bind=self.pg_engine)
        self.SAPSession = sessionmaker(bind=self.sap_engine)

        print(f"Loaded configuration for environment: {self.environment}")

    def get_pg_session(self):
        """Ottieni una nuova sessione PostgreSQL"""
        return self.PGSession()

    def get_sap_session(self):
        """Ottieni una nuova sessione SAP"""
        return self.SAPSession()

