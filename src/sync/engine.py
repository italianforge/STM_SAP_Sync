import time
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from ..config.database import DatabaseConfig
from ..config.settings import Settings
from ..mappings.registry import get_mapping
from ..mappings.base import SyncStrategy
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
            table_logger.info(f"Inizio sincronizzazione tabella {table_name} (strategia: {mapping.sync_strategy.value})")
        except ValueError as e:
            self.error_logger.error(f"Mapping non trovato per {table_name}: {str(e)}")
            raise
        
        pg_session = self.db_config.get_pg_session()
        sap_session = self.db_config.get_sap_session()

        try:
            # Per truncate_insert non consideriamo il last_sync
            if mapping.requires_truncate():
                last_sync = None
                table_logger.info("Modalità TRUNCATE_INSERT: recupero tutti i dati")
            else:
                # Ottieni ultimo sync per modalità UPSERT
                last_sync = self.sync_state_service.get_last_sync(pg_session, table_name)
                table_logger.info(f"Modalità UPSERT: ultima sincronizzazione: {last_sync}")

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

            # Se richiesto, truncate la tabella prima di inserire i nuovi dati
            if mapping.requires_truncate():
                self._truncate_table(pg_session, mapping, table_logger)

            # Processa i dati
            processed_records, error_count, max_ts = self._process_rows(
                rows, mapping, pg_session, table_logger
            )

            # Aggiorna stato sincronizzazione solo se non è truncate_insert
            if max_ts and not mapping.requires_truncate():
                self.sync_state_service.update_last_sync(pg_session, table_name, max_ts)
                table_logger.info(f"Aggiornato timestamp sincronizzazione: {max_ts}")
            elif mapping.requires_truncate():
                # Per truncate_insert, aggiorna con il timestamp corrente
                from datetime import datetime
                current_ts = datetime.now()
                self.sync_state_service.update_last_sync(pg_session, table_name, current_ts)
                table_logger.info(f"Aggiornato timestamp sincronizzazione (truncate_insert): {current_ts}")
            
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
    
    def _truncate_table(self, session, mapping, logger):
        """Svuota completamente una tabella PostgreSQL"""
        table_name = mapping.pg_model.__tablename__
        truncate_start = time.time()
        
        try:
            logger.info(f"Svuotamento tabella {table_name}...")
            session.execute(text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE'))
            session.flush()
            
            truncate_duration = time.time() - truncate_start
            log_performance(logger, f"Truncate tabella {table_name}", truncate_duration)
            logger.info(f"Tabella {table_name} svuotata con successo")
            
        except Exception as e:
            log_database_error(logger, f"Truncate tabella {table_name}", e)
            raise
    
    def _build_sync_query(self, mapping, last_sync):
        """Costruisce la query di sincronizzazione"""
        # Ottieni solo le colonne SAP necessarie dal mapping
        sap_columns = list(mapping.column_mappings.keys())
        columns_str = ", ".join(sap_columns)
        
        query = f"SELECT {columns_str} FROM {mapping.sap_table}"
        
        # Solo per UPSERT aggiungiamo il filtro temporale
        if last_sync and not mapping.requires_truncate():
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
                
                # Traccia timestamp massimo solo per UPSERT
                if not mapping.requires_truncate() and 'last_synced_at' in pg_data and pg_data['last_synced_at']:
                    if max_ts is None or pg_data['last_synced_at'] > max_ts:
                        max_ts = pg_data['last_synced_at']
                
                # Quando il batch è pieno, esegui l'operazione
                if len(batch_records) >= self.batch_size:
                    batch_start = time.time()
                    
                    if mapping.requires_truncate():
                        self._execute_insert_batch(pg_session, mapping, batch_records)
                        operation_name = "Batch insert"
                    else:
                        self._execute_upsert_batch(pg_session, mapping, batch_records)
                        operation_name = "Batch upsert"
                    
                    batch_duration = time.time() - batch_start
                    processed += len(batch_records)
                    log_performance(logger, operation_name, batch_duration, len(batch_records))
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
                
                if mapping.requires_truncate():
                    self._execute_insert_batch(pg_session, mapping, batch_records)
                    operation_name = "Final batch insert"
                else:
                    self._execute_upsert_batch(pg_session, mapping, batch_records)
                    operation_name = "Final batch upsert"
                
                batch_duration = time.time() - batch_start
                processed += len(batch_records)
                log_performance(logger, operation_name, batch_duration, len(batch_records))
            except Exception as e:
                error_count += 1
                log_database_error(logger, f"Final batch {operation_name.lower()}", e)

        return processed, error_count, max_ts
    
    def _execute_insert_batch(self, session, mapping, records):
        """Esegue insert semplice di un batch di record (per truncate_insert)"""
        if not records:
            return
        
        stmt = insert(mapping.pg_model).values(records)
        session.execute(stmt)
        session.flush()  # Libera la memoria senza fare commit
    
    def _execute_upsert_batch(self, session, mapping, records):
        """Esegue upsert di un batch di record (per strategia normale)"""
        if not records:
            return
        
        stmt = insert(mapping.pg_model).values(records)
        
        # Usa le colonne della chiave primaria PostgreSQL per il conflict
        pg_primary_keys = mapping.get_pg_primary_key_columns()
        
        # Trova le colonne che NON sono primary key
        non_pk_columns = [col.name for col in mapping.pg_model.__table__.columns 
                        if col.name not in pg_primary_keys]
        
        # Se ci sono colonne non-PK, usa ON CONFLICT DO UPDATE
        if non_pk_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=pg_primary_keys,
                set_={col_name: stmt.excluded[col_name] for col_name in non_pk_columns}
            )
        else:
            # Se tutte le colonne sono PK, usa ON CONFLICT DO NOTHING
            stmt = stmt.on_conflict_do_nothing(index_elements=pg_primary_keys)
        
        session.execute(stmt)
        session.flush()  # Libera la memoria senza fare commit
