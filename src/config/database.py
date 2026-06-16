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


# Allineato al driver SAP in uso sulle postazioni STM (SQL Server legacy ODBC).
_DEFAULT_MSSQL_DRIVER = 'SQL Server'


def _uses_modern_odbc_driver(driver: str | None) -> bool:
    """TrustServerCertificate è supportato solo da ODBC Driver 17/18."""
    name = (driver or _DEFAULT_MSSQL_DRIVER).lower()
    return 'odbc driver 17' in name or 'odbc driver 18' in name


def _encode_mssql_server(server: str) -> str:
    """Codifica il nome server per URL SQLAlchemy (istanze nominate es. host\\INSTANCE)."""
    return (server or '').replace('\\', '%5C')


def _build_mssql_url_from_fields(
    server,
    port,
    database,
    username,
    password,
    driver=None,
) -> str:
    from urllib.parse import quote_plus

    driver_name = driver or _DEFAULT_MSSQL_DRIVER
    encoded_password = quote_plus(password or '')
    encoded_username = quote_plus(username or '')

    # Istanze nominate (host\INSTANCE): odbc_connect evita problemi di parsing URL.
    if server and '\\' in server:
        host_part, instance_name = server.split('\\', 1)
        if port and str(port) not in ('', '1433'):
            # Porta TCP esplicita: evita SQL Browser (UDP 1434).
            server_arg = f'{host_part},{port}'
        else:
            server_arg = server
        odbc_parts = [
            f'DRIVER={{{driver_name}}}',
            f'SERVER={server_arg}',
            f'DATABASE={database}',
            f'UID={username}',
            f'PWD={password or ""}',
        ]
        if _uses_modern_odbc_driver(driver_name):
            odbc_parts.append('TrustServerCertificate=yes')
        odbc_connect = ';'.join(odbc_parts)
        return f'mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connect)}'

    host = _encode_mssql_server(server)
    if port and str(port) != '1433':
        host = f'{host}:{port}'
    driver_encoded = driver_name.replace(' ', '+')
    query = f'driver={driver_encoded}&trusted_connection=no'
    if _uses_modern_odbc_driver(driver_name):
        query += '&TrustServerCertificate=yes'
    return (
        f'mssql+pyodbc://{encoded_username}:{encoded_password}@{host}/{database}'
        f'?{query}'
    )


def _build_sap_url_from_fields(server, port, database, username, password, driver) -> str:
    return _build_mssql_url_from_fields(server, port, database, username, password, driver)


def _load_mssql_url_from_env(prefix: str) -> str | None:
    """
    Costruisce l'URL MSSQL da variabili d'ambiente con prefisso (es. DEPOSYTA, MODULA).
    Variabili attese: {PREFIX}_DB_URL oppure server/database/username/password.
    """
    direct_url = os.getenv(f'{prefix}_DB_URL', '').strip()
    if direct_url:
        return direct_url

    server = os.getenv(f'{prefix}_DB_SERVER', '').strip()
    database = os.getenv(f'{prefix}_DB_DATABASE', '').strip()
    username = os.getenv(f'{prefix}_DB_USERNAME', '').strip()
    password = os.getenv(f'{prefix}_DB_PASSWORD', '')

    if not all([server, database, username, password]):
        return None

    return _build_mssql_url_from_fields(
        server=server,
        port=os.getenv(f'{prefix}_DB_PORT', '1433'),
        database=database,
        username=username,
        password=password,
        driver=os.getenv(f'{prefix}_DB_DRIVER', _DEFAULT_MSSQL_DRIVER),
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
        load_dotenv(env_file, encoding='utf-8-sig')

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

        self.deposyta_db_url = _load_mssql_url_from_env('DEPOSYTA')
        self.modula_db_url = _load_mssql_url_from_env('MODULA')

        # Crea engines
        self.pg_engine = create_engine(self.postgres_url)
        self.sap_engine = create_engine(self.sap_db_url)

        self.deposyta_engine = (
            create_engine(self.deposyta_db_url) if self.deposyta_db_url else None
        )
        self.modula_engine = (
            create_engine(self.modula_db_url) if self.modula_db_url else None
        )

        # Crea session makers
        self.PGSession = sessionmaker(bind=self.pg_engine)
        self.SAPSession = sessionmaker(bind=self.sap_engine)
        self.DeposytaSession = (
            sessionmaker(bind=self.deposyta_engine) if self.deposyta_engine else None
        )
        self.ModulaSession = (
            sessionmaker(bind=self.modula_engine) if self.modula_engine else None
        )

        if self.deposyta_db_url:
            logger.info('DEPOSYTA connection configured')
        else:
            logger.warning('DEPOSYTA connection not configured (missing env credentials)')

        if self.modula_db_url:
            logger.info('MODULA connection configured')
        else:
            logger.warning('MODULA connection not configured (missing env credentials)')

        print(f"Loaded configuration for environment: {self.environment}")

    def get_pg_session(self):
        """Ottieni una nuova sessione PostgreSQL"""
        return self.PGSession()

    def get_sap_session(self):
        """Ottieni una nuova sessione SAP"""
        return self.SAPSession()

    def get_deposyta_session(self):
        """Ottieni una nuova sessione DEPOSYTA (SQL Server DBDATA)."""
        if not self.DeposytaSession:
            raise RuntimeError(
                'Connessione DEPOSYTA non configurata: impostare DEPOSYTA_DB_USERNAME '
                'e DEPOSYTA_DB_PASSWORD nel file .env'
            )
        return self.DeposytaSession()

    def get_modula_session(self):
        """Ottieni una nuova sessione MODULA (SQL Server SYSTOREDB)."""
        if not self.ModulaSession:
            raise RuntimeError(
                'Connessione MODULA non configurata: impostare MODULA_DB_USERNAME '
                'e MODULA_DB_PASSWORD nel file .env'
            )
        return self.ModulaSession()

