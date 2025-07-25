from ..models.ordini_acquisto import SAP_OrdiniAcquisto
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_string, safe_int, safe_datetime


MAPPING_ORDINI_ACQUISTO = TableMapping(
    sap_table="dbo.OPOR",
    pg_model=SAP_OrdiniAcquisto,  
    column_mappings={
        "DocEntry": "id",
        "DocNum": "code",
        "DocDate": "doc_date",
        "DocDueDate": "doc_due_date",
        "CardCode": "cod_business_partner",
        "UpdateDate": "_update_date",  # Campo temporaneo per la trasformazione
        "UpdateTS": "_update_ts"       # Campo temporaneo per la trasformazione
    },
    transformations={
        "code": safe_int,
        "doc_date": safe_datetime,
        "doc_due_date": safe_datetime,
        "cod_business_partner": safe_string
    },
    primary_key_sap=["DocEntry"],  # Chiave primaria multipla
    sync_strategy=SyncStrategy.UPSERT  # Usa truncate e insert
)

