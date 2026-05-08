from ..models.ordini_acquisto import SAP_OrdiniAcquisto
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_string, safe_int, safe_datetime


_ORDINI_ACQUISTO_QUERY = """
SELECT
    DocEntry,
    DocNum,
    DocDate,
    DocDueDate,
    CardCode,
    UpdateDate,
    UpdateTS
FROM dbo.OPOR
WHERE DocDate >= DATEADD(year, -1, GETDATE())
""".strip()


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
    primary_key_sap=["DocEntry"],
    sync_strategy=SyncStrategy.UPSERT,
    sap_query=_ORDINI_ACQUISTO_QUERY,
)

