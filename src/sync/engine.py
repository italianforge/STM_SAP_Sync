import time
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from ..config.database import DatabaseConfig
from ..config.settings import Settings
from ..mappings.registry import get_mapping
from ..utils.logger import setup_sync_logger, setup_error_logger, log_performance, log_database_error, log_sync_stats
from .services import SyncStateService

class SyncEngine:
    """Engine principale per la sincronizzazione dati SAP"""
    
    def __init__(self, db_config: DatabaseConfig = None):
        self.db_config = db_config or DatabaseConfig()
        self.logger = setup_sync_logger("Engine")
        self.error_logger = setup_error_logger()
        self.sync_state_service = SyncStateService()
        self.batch_size = Settings.get_batch_size()
    
    def sync_table(self, table_name: str) -> None:
        """Sincronizza una tabella specifica usando il mapping"""
        table_logger = setup_sync_logger(table_name)
        start_time = time.time()
        total_records = 0
        processed_records = 0
        error_count = 0
        
        try:
            mapping = get_mapping(table_name)
            table_logger.info(f"Inizio sincronizzazione tabella {table_name}")
        except ValueError as e:
            self.error_logger.error(f"Mapping non trovato per {table_name}: {str(e)}")
            raise
        
        pg_session = self.db_config.get_pg_session()
        sap_session = self.db_config.get_sap_session()

        try:
            # Ottieni ultimo sync
            last_sync = self.sync_state_service.get_last_sync(pg_session, table_name)
            table_logger.info(f"Ultima sincronizzazione: {last_sync}")

            # Costruisci e esegui query
            query_start = time.time()
            query = self._build_sync_query(mapping, last_sync)
            table_logger.debug(f"Query SAP: {query}")
            
            result = sap_session.execute(text(query))
            rows = result.fetchall()
            total_records = len(rows)
            
            query_duration = time.time() - query_start
            log_performance(table_logger, f"Query SAP per {table_name}", query_duration, total_records)
            
            if not rows:
                table_logger.info("Nessuna riga da sincronizzare")
                return

            # Processa i dati
            processed_records, error_count, max_ts = self._process_rows(
                rows, mapping, pg_session, table_logger
            )

            # Aggiorna stato sincronizzazione
            if max_ts:
                self.sync_state_service.update_last_sync(pg_session, table_name, max_ts)
                table_logger.info(f"Aggiornato timestamp sincronizzazione: {max_ts}")
            
            pg_session.commit()
            
            # Log statistiche finali
            total_duration = time.time() - start_time
            log_sync_stats(table_logger, table_name, total_records, processed_records, error_count, total_duration)
            table_logger.info(f"Sincronizzazione {table_name} completata con successo")

        except Exception as e:
            pg_session.rollback()
            error_duration = time.time() - start_time
            log_database_error(self.error_logger, "Sincronizzazione", e, table_name)
            table_logger.error(f"Errore durante sincronizzazione: {str(e)}")
            raise
        finally:
            pg_session.close()
            sap_session.close()
    
    def _build_sync_query(self, mapping, last_sync):
        """Costruisce la query di sincronizzazione"""
        query = f"SELECT * FROM {mapping.sap_table}"
        if last_sync:
            last_sync_date = last_sync.strftime("%Y%m%d")
            last_sync_ts = last_sync.hour * 10000 + last_sync.minute * 100 + last_sync.second
            query += f" WHERE CONVERT(date, UpdateDate) > CONVERT(date, '{last_sync_date}') OR (CONVERT(date, UpdateDate) = CONVERT(date, '{last_sync_date}') AND UpdateTS > {last_sync_ts})"
        return query
    
    def _process_rows(self, rows, mapping, pg_session, logger):
        """Processa le righe dalla query SAP"""
        max_ts = None
        processed = 0
        error_count = 0
        batch_records = []

        for row_num, row in enumerate(rows, 1):
            try:
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
                    batch_start = time.time()
                    self._execute_upsert_batch(pg_session, mapping, batch_records)
                    batch_duration = time.time() - batch_start
                    processed += len(batch_records)
                    log_performance(logger, f"Batch upsert", batch_duration, len(batch_records))
                    logger.info(f"Processate {processed}/{len(rows)} righe...")
                    batch_records = []
                    
            except Exception as e:
                error_count += 1
                log_database_error(logger, f"Elaborazione riga {row_num}", e)
                # Continua con la prossima riga

        # Processa l'ultimo batch se ci sono record rimanenti
        if batch_records:
            try:
                batch_start = time.time()
                self._execute_upsert_batch(pg_session, mapping, batch_records)
                batch_duration = time.time() - batch_start
                processed += len(batch_records)
                log_performance(logger, f"Final batch upsert", batch_duration, len(batch_records))
            except Exception as e:
                error_count += 1
                log_database_error(logger, "Final batch upsert", e)

        return processed, error_count, max_ts
    
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
