from ..models.ordini_acquisto_lines import SAP_OrdiniAcquistoLine
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_date, safe_float, safe_string


_ORDINI_ACQUISTO_LINES_QUERY = """
SELECT
    p.LineNum,
    p.DocEntry,
    p.ItemCode,
    p.Quantity,
    p.ShipDate
FROM dbo.POR1 p
INNER JOIN dbo.OPOR o ON p.DocEntry = o.DocEntry
WHERE o.DocDate >= DATEADD(year, -1, GETDATE())
""".strip()


MAPPING_ORDINI_ACQUISTO_LINES = TableMapping(
    sap_table="dbo.POR1",
    pg_model=SAP_OrdiniAcquistoLine,  
    column_mappings={
        "LineNum": "id",
        "DocEntry": "cod_documento",
        "ItemCode": "cod_articolo",
        "Quantity": "quantity",
        "ShipDate": "data_consegna",
    },
    transformations={
        "cod_articolo": safe_string,
        "quantity": safe_float,
        "data_consegna": safe_date,
    },
    primary_key_sap=["LineNum", "DocEntry"],
    sync_strategy=SyncStrategy.TRUNCATE_INSERT,
    sap_query=_ORDINI_ACQUISTO_LINES_QUERY,
)

