from ..models.ordini_acquisto_lines import SAP_OrdiniAcquistoLine
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_float, safe_string


MAPPING_ORDINI_ACQUISTO_LINES = TableMapping(
    sap_table="dbo.POR1",
    pg_model=SAP_OrdiniAcquistoLine,  
    column_mappings={
        "LineNum": "id",
        "DocEntry": "cod_documento",
        "ItemCode": "cod_articolo",
        "Quantity": "quantity",
    },
    transformations={
        "cod_articolo": safe_string,
        "quantity": safe_float,
    },
    primary_key_sap=["LineNum", "DocEntry"],  # Chiave primaria multipla
    sync_strategy=SyncStrategy.TRUNCATE_INSERT  # Usa truncate e insert
)

