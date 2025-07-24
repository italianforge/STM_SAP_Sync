from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from ..config.database import DatabaseConfig
from ..config.settings import Settings
from ..mappings.registry import get_mapping
from ..utils.logger import setup_logger
from .services import SyncStateService

class SyncEngine:
    """Engine principale per la sincronizzazione dati SAP"""
    
    def __init__(self, db_config: DatabaseConfig = None):
        self.db_config = db_config or DatabaseConfig()
        self.logger = setup_logger("SyncEngine")
        self.sync_state_service = SyncStateService()
        self.batch_size = Settings.get_batch_size()
    
    def sync_table(self, table_name: str) -> None:
        """Sincronizza una tabella specifica usando il mapping"""
        try:
            mapping = get_mapping(table_name)
        except ValueError as e:
            self.logger.error(str(e))
            raise
        
        pg_session = self.db_config.get_pg_session()
        sap_session = self.db_config.get_sap_session()

        try:
            last_sync = self.sync_state_service.get_last_sync(pg_session, table_name)
            self.logger.info(f"Ultima sincronizzazione per {table_name}: {last_sync}")

            # Costruisci query
            query = f"SELECT * FROM {mapping.sap_table}"
            if last_sync:
                # Per SAP, confronta usando sia UpdateDate che UpdateTS con conversioni SQL Server
                last_sync_date = last_sync.strftime("%Y%m%d")  # Formato YYYYMMDD per SQL Server
                last_sync_ts = last_sync.hour * 10000 + last_sync.minute * 100 + last_sync.second
                query += f" WHERE CONVERT(date, UpdateDate) > CONVERT(date, '{last_sync_date}') OR (CONVERT(date, UpdateDate) = CONVERT(date, '{last_sync_date}') AND UpdateTS > {last_sync_ts})"

            result = sap_session.execute(text(query))
            rows = result.fetchall()

            self.logger.info(f"{len(rows)} righe da sincronizzare per {table_name}.")
            
            if not rows:
                self.logger.info("Nessuna riga da sincronizzare.")
                return

            max_ts = None
            processed = 0
            batch_records = []

            for row in rows:
                # Converti row in dizionario
                row_dict = dict(row._mapping)
                
                # Trasforma usando il mapping
                pg_data = mapping.transform_row(row_dict)
                batch_records.append(pg_data)
                
                # Traccia timestamp massimo
                if 'last_synced_at' in pg_data and pg_data['last_synced_at']:
                    if max_ts is None or pg_data['last_synced_at'] > max_ts:
                        max_ts = pg_data['last_synced_at']
                
                # Quando il batch è pieno, esegui l'upsert
                if len(batch_records) >= self.batch_size:
                    self._execute_upsert_batch(pg_session, mapping, batch_records)
                    processed += len(batch_records)
                    batch_records = []
                    self.logger.info(f"Processate {processed} righe...")

            # Processa l'ultimo batch se ci sono record rimanenti
            if batch_records:
                self._execute_upsert_batch(pg_session, mapping, batch_records)
                processed += len(batch_records)

            # Aggiorna stato sincronizzazione
            if max_ts:
                self.sync_state_service.update_last_sync(pg_session, table_name, max_ts)
            
            pg_session.commit()
            self.logger.info(f"Sincronizzazione completata: {processed} righe processate.")

        except Exception as e:
            pg_session.rollback()
            self.logger.error(f"Errore durante la sincronizzazione di {table_name}: {e}")
            raise
        finally:
            pg_session.close()
            sap_session.close()
    
    def _execute_upsert_batch(self, session, mapping, records):
        """Esegue upsert di un batch di record"""
        if not records:
            return
        
        stmt = insert(mapping.pg_model).values(records)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_={col.name: stmt.excluded[col.name] 
                  for col in mapping.pg_model.__table__.columns 
                  if col.name != 'id'}
        )
        session.execute(stmt)
        session.flush()  # Libera la memoria senza fare commit
