from .base import TableMapping
from ..models.anagrafica_articoli import SAP_AnagraficheArticoli
from ..utils.transformers import safe_datetime, safe_string

# Mapping per la tabella OITM -> SAP_AnagraficheArticoli
MAPPING_ANAGRAFICHE_ARTICOLI = TableMapping(
    sap_table="dbo.OITM",
    pg_model=SAP_AnagraficheArticoli,  
    column_mappings={
        "ItemCode": "id",
        "ItemName": "description", 
        "U_FamigliaTornitura": "famiglia_tornitura",
        "MinLevel": "min_level",
        "UpdateDate": "_update_date",  # Campo temporaneo per la trasformazione
        "UpdateTS": "_update_ts"       # Campo temporaneo per la trasformazione
    },
    transformations={
        "min_level": safe_datetime,
        "description": safe_string,
        "famiglia_tornitura": safe_string
    },
    primary_key_sap="ItemCode"
)
