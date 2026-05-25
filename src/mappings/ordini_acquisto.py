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
    DocStatus,
    UpdateDate,
    UpdateTS
FROM dbo.OPOR
WHERE DocDate >= DATEADD(year, -1, GETDATE())
  AND EXISTS (
      SELECT 1 FROM dbo.POR1 p
      INNER JOIN dbo.OITM i ON p.ItemCode = i.ItemCode
      WHERE p.DocEntry = dbo.OPOR.DocEntry
        AND (i.QryGroup14 = 'Y' OR i.QryGroup15 = 'Y' OR i.QryGroup16 = 'Y')
  )
""".strip()


def _map_doc_status(value):
    """Mappa DocStatus SAP (C/O) in status leggibile (CHIUSO/APERTO)"""
    if value == 'C':
        return 'CHIUSO'
    return 'APERTO'


MAPPING_ORDINI_ACQUISTO = TableMapping(
    sap_table="dbo.OPOR",
    pg_model=SAP_OrdiniAcquisto,  
    column_mappings={
        "DocEntry": "id",
        "DocNum": "code",
        "DocDate": "doc_date",
        "DocDueDate": "doc_due_date",
        "CardCode": "cod_business_partner",
        "DocStatus": "status",
        "UpdateDate": "_update_date",  # Campo temporaneo per la trasformazione
        "UpdateTS": "_update_ts"       # Campo temporaneo per la trasformazione
    },
    transformations={
        "code": safe_int,
        "doc_date": safe_datetime,
        "doc_due_date": safe_datetime,
        "cod_business_partner": safe_string,
        "status": _map_doc_status
    },
    primary_key_sap=["DocEntry"],
    sync_strategy=SyncStrategy.UPSERT,
    sap_query=_ORDINI_ACQUISTO_QUERY,
)

