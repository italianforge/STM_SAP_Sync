import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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
        self.sap_db_url = os.getenv("SAP_DB_URL")
        self.environment = os.getenv("ENVIRONMENT", env)
        
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
