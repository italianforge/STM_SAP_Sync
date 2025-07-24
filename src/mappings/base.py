from typing import Dict, Any, Callable, Optional
from ..utils.transformers import transform_sap_timestamp

class TableMapping:
    """Gestisce il mapping tra tabelle SAP e modelli PostgreSQL"""
    
    def __init__(self, sap_table: str, pg_model, column_mappings: Dict[str, str], 
                 transformations: Optional[Dict[str, Callable]] = None,
                 primary_key_sap: str = "ItemCode"):
        self.sap_table = sap_table
        self.pg_model = pg_model
        self.column_mappings = column_mappings  # sap_column -> pg_column
        self.transformations = transformations or {}
        self.primary_key_sap = primary_key_sap
    
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
