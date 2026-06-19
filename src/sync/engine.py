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
    
    def sync_table(self, table_name: str, force_full: bool = False) -> None:
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
            elif force_full:
                last_sync = None
                table_logger.info("Modalità UPSERT: full resync forzato (primo avvio)")
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
            else:
                # #region agent log
                if table_name == "anagraficheArticoli":
                    from ..utils.debug_session_log import debug_log
                    debug_log(
                        "engine.py:sync_table",
                        "anagraficheArticoli sync batch",
                        {
                            "last_sync": last_sync.isoformat() if last_sync else None,
                            "is_delta": bool(last_sync and not mapping.requires_truncate()),
                            "rows_fetched": total_records,
                        },
                        hypothesis_id="H5",
                    )
                # #endregion
                # Se richiesto, truncate la tabella prima di inserire i nuovi dati
                if mapping.requires_truncate():
                    self._truncate_table(pg_session, mapping, table_logger)

                if mapping.pre_sync_callback:
                    mapping.pre_sync_callback(sap_session)

                # Processa i dati
                processed_records, error_count, max_ts = self._process_rows(
                    rows, mapping, pg_session, table_logger
                )

                # Aggiorna stato sincronizzazione solo se non è truncate_insert
                if max_ts and not mapping.requires_truncate():
                    self.sync_state_service.update_last_sync(pg_session, table_name, max_ts)
                    table_logger.info(f"Aggiornato timestamp sincronizzazione: {max_ts}")
                elif mapping.requires_truncate():
                    from datetime import datetime
                    current_ts = datetime.now()
                    self.sync_state_service.update_last_sync(pg_session, table_name, current_ts)
                    table_logger.info(
                        f"Aggiornato timestamp sincronizzazione (truncate_insert): {current_ts}"
                    )

            # Callback anche senza delta SAP (es. arricchimento stock DEPOSYTA)
            if mapping.post_sync_callback:
                table_logger.info("Esecuzione post_sync_callback...")
                mapping.post_sync_callback(pg_session, rows)
                table_logger.info("post_sync_callback completato")

            pg_session.commit()
            
            # Log statistiche finali
            total_duration = time.time() - start_time
            log_sync_stats(table_logger, table_name, total_records, processed_records, error_count, total_duration)
            if error_count:
                table_logger.warning(
                    f"Sincronizzazione {table_name} completata con {error_count} righe skippate"
                )
            else:
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
        schema = getattr(mapping.pg_model, '__table_args__', {}) 
        if isinstance(schema, dict):
            schema_name = schema.get('schema', 'public')
        elif isinstance(schema, tuple):
            schema_name = next((d.get('schema', 'public') for d in schema if isinstance(d, dict)), 'public')
        else:
            schema_name = 'public'
        table_name = mapping.pg_model.__tablename__
        full_table_name = f'{schema_name}.{table_name}'
        truncate_start = time.time()
        
        try:
            logger.info(f"Svuotamento tabella {full_table_name}...")
            session.execute(text(f'TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE'))
            session.flush()
            
            truncate_duration = time.time() - truncate_start
            log_performance(logger, f"Truncate tabella {table_name}", truncate_duration)
            logger.info(f"Tabella {table_name} svuotata con successo")
            
        except Exception as e:
            log_database_error(logger, f"Truncate tabella {table_name}", e)
            raise
    
    def _build_sync_query(self, mapping, last_sync):
        """Costruisce la query di sincronizzazione"""
        if mapping.sap_query:
            # Usa la query custom (es. JOIN multi-tabella)
            query = mapping.sap_query
            # Applica il filtro temporale se necessario
            if last_sync and not mapping.requires_truncate():
                prefix = f"{mapping.sap_timestamp_prefix}." if mapping.sap_timestamp_prefix else ""
                last_sync_date = last_sync.strftime("%Y%m%d")
                last_sync_ts = last_sync.hour * 10000 + last_sync.minute * 100 + last_sync.second
                connector = "AND" if "WHERE" in query.upper() else "WHERE"
                query += (
                    f" {connector} (CONVERT(date, {prefix}UpdateDate) > CONVERT(date, '{last_sync_date}')"
                    f" OR (CONVERT(date, {prefix}UpdateDate) = CONVERT(date, '{last_sync_date}')"
                    f" AND {prefix}UpdateTS > {last_sync_ts}))"
                )
        else:
            # Costruisci la query automaticamente dalle chiavi del mapping
            sap_columns = list(mapping.column_mappings.keys())
            columns_str = ", ".join(sap_columns)
            query = f"SELECT {columns_str} FROM {mapping.sap_table}"
            # Solo per UPSERT aggiungiamo il filtro temporale
            if last_sync and not mapping.requires_truncate():
                last_sync_date = last_sync.strftime("%Y%m%d")
                last_sync_ts = last_sync.hour * 10000 + last_sync.minute * 100 + last_sync.second
                query += (
                    f" WHERE CONVERT(date, UpdateDate) > CONVERT(date, '{last_sync_date}')"
                    f" OR (CONVERT(date, UpdateDate) = CONVERT(date, '{last_sync_date}')"
                    f" AND UpdateTS > {last_sync_ts})"
                )

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
                    operation_name = "Batch insert" if mapping.requires_truncate() else "Batch upsert"
                    batch_ok, batch_err = self._execute_batch_with_fallback(
                        pg_session, mapping, batch_records, logger, operation_name
                    )
                    batch_duration = time.time() - batch_start
                    processed += batch_ok
                    error_count += batch_err
                    if batch_ok:
                        log_performance(logger, operation_name, batch_duration, batch_ok)
                        logger.info(f"Processate {processed}/{len(rows)} righe...")
                    batch_records = []
                    
            except Exception as e:
                error_count += 1
                log_database_error(logger, f"Elaborazione riga {row_num}", e)
                # Continua con la prossima riga

        # Processa l'ultimo batch se ci sono record rimanenti
        if batch_records:
            operation_name = "Final batch insert" if mapping.requires_truncate() else "Final batch upsert"
            batch_start = time.time()
            batch_ok, batch_err = self._execute_batch_with_fallback(
                pg_session, mapping, batch_records, logger, operation_name
            )
            batch_duration = time.time() - batch_start
            processed += batch_ok
            error_count += batch_err
            if batch_ok:
                log_performance(logger, operation_name, batch_duration, batch_ok)

        return processed, error_count, max_ts
    
    def _record_label(self, mapping, record: dict) -> str:
        """Identificativo leggibile di una riga per i log."""
        pk_cols = mapping.get_pg_primary_key_columns()
        if not pk_cols:
            return '?'
        parts = [str(record.get(col, '')) for col in pk_cols]
        return '/'.join(parts) or '?'

    def _execute_batch_with_fallback(self, session, mapping, records, logger, operation_name):
        """
        Esegue un batch; se fallisce, riprova riga per riga skippando quelle in errore.
        Usa savepoint per non annullare i batch già applicati nella stessa transazione.
        Restituisce (righe_ok, righe_skippate).
        """
        if not records:
            return 0, 0

        savepoint = session.begin_nested()
        try:
            if mapping.requires_truncate():
                self._execute_insert_batch(session, mapping, records)
            else:
                self._execute_upsert_batch(session, mapping, records)
            savepoint.commit()
            return len(records), 0
        except Exception as batch_e:
            savepoint.rollback()
            logger.warning(
                f"{operation_name} fallito su {len(records)} righe, retry singolo: "
                f"{type(batch_e).__name__}: {batch_e}"
            )

        ok, err = 0, 0
        for rec in records:
            row_sp = session.begin_nested()
            try:
                if mapping.requires_truncate():
                    self._execute_insert_batch(session, mapping, [rec])
                else:
                    self._execute_upsert_batch(session, mapping, [rec])
                row_sp.commit()
                ok += 1
            except Exception as row_e:
                row_sp.rollback()
                err += 1
                # #region agent log
                if rec.get("art_equivalente"):
                    from ..utils.debug_session_log import debug_log
                    debug_log(
                        "engine.py:_execute_batch_with_fallback",
                        "row skipped with art_equivalente",
                        {
                            "record_id": self._record_label(mapping, rec),
                            "art_equivalente": rec.get("art_equivalente"),
                            "error": f"{type(row_e).__name__}: {row_e}",
                        },
                        hypothesis_id="H3,H4",
                    )
                # #endregion
                log_database_error(
                    logger,
                    f"Riga skippata ({self._record_label(mapping, rec)})",
                    row_e,
                )
        if err:
            logger.warning(f"{operation_name}: {ok} righe salvate, {err} skippate")
        return ok, err
    
    def _execute_insert_batch(self, session, mapping, records):
        """Esegue insert semplice di un batch di record (per truncate_insert)"""
        if not records:
            return
        
        stmt = insert(mapping.pg_model).values(records).on_conflict_do_nothing()
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
        # Aggiorna solo colonne presenti nel payload sync (es. art_equivalente non mappato da SAP)
        synced_columns = set()
        for rec in records:
            synced_columns.update(rec.keys())
        update_columns = [c for c in non_pk_columns if c in synced_columns]

        # Se ci sono colonne non-PK, usa ON CONFLICT DO UPDATE
        if update_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=pg_primary_keys,
                set_={col_name: stmt.excluded[col_name] for col_name in update_columns}
            )
        else:
            # Se tutte le colonne sono PK, usa ON CONFLICT DO NOTHING
            stmt = stmt.on_conflict_do_nothing(index_elements=pg_primary_keys)
        
        session.execute(stmt)
        session.flush()  # Libera la memoria senza fare commit
