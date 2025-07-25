from typing import Dict, Any, Callable, Optional, Union, List
from enum import Enum
from ..utils.transformers import transform_sap_timestamp

class SyncStrategy(Enum):
    """Strategie di sincronizzazione disponibili"""
    UPSERT = "upsert"          # Aggiorna/inserisce (default)
    TRUNCATE_INSERT = "truncate_insert"  # Svuota e riempi

class TableMapping:
    """Gestisce il mapping tra tabelle SAP e modelli PostgreSQL"""
    
    def __init__(self, sap_table: str, pg_model, column_mappings: Dict[str, str], 
                 transformations: Optional[Dict[str, Callable]] = None,
                 primary_key_sap: Union[str, List[str]] = "ItemCode",
                 sync_strategy: SyncStrategy = SyncStrategy.UPSERT):
        self.sap_table = sap_table
        self.pg_model = pg_model
        self.column_mappings = column_mappings  # sap_column -> pg_column
        self.transformations = transformations or {}
        self.sync_strategy = sync_strategy
        # Normalizza primary_key_sap come lista
        if isinstance(primary_key_sap, str):
            self.primary_key_sap = [primary_key_sap]
        else:
            self.primary_key_sap = primary_key_sap
    
    def get_primary_key_columns(self) -> List[str]:
        """Restituisce le colonne della chiave primaria SAP"""
        return self.primary_key_sap
    
    def get_pg_primary_key_columns(self) -> List[str]:
        """Restituisce le colonne della chiave primaria PostgreSQL mappate"""
        return [self.column_mappings[col] for col in self.primary_key_sap if col in self.column_mappings]
    
    def requires_truncate(self) -> bool:
        """Verifica se questa tabella richiede il truncate prima dell'inserimento"""
        return self.sync_strategy == SyncStrategy.TRUNCATE_INSERT
    
    def transform_row(self, sap_row: Dict[str, Any]) -> Dict[str, Any]:
        """Trasforma una riga SAP in formato PostgreSQL"""
        pg_data = {}
        
        for sap_col, pg_col in self.column_mappings.items():
            if sap_col in sap_row:
                value = sap_row[sap_col]
                
                # Applica trasformazioni se presenti
                if pg_col in self.transformations:
                    value = self.transformations[pg_col](value)
                
                # Non aggiungere campi temporanei che iniziano con _
                if not pg_col.startswith('_'):
                    pg_data[pg_col] = value
        
        # Gestione speciale per il timestamp SAP combinato
        if '_update_date' in [col for col in self.column_mappings.values()] and '_update_ts' in [col for col in self.column_mappings.values()]:
            pg_data['last_synced_at'] = transform_sap_timestamp(sap_row)
        
        return pg_data
