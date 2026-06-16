from ..models.entrata_merci import SAP_EntrataMerci
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_datetime, safe_int, safe_string


_ENTRATA_MERCI_QUERY = """
SELECT
    DocEntry,
    DocDate,
    CardCode
FROM dbo.OPDN
WHERE DocDate >= DATEADD(year, -1, GETDATE())
""".strip()


MAPPING_ENTRATA_MERCI = TableMapping(
    sap_table="dbo.OPDN",
    pg_model=SAP_EntrataMerci,
    column_mappings={
        "DocEntry": "id",
        "DocDate": "date_registration",
        "CardCode": "cod_business_partner",
    },
    transformations={
        "id": safe_int,
        "date_registration": safe_datetime,
        "cod_business_partner": safe_string,
    },
    primary_key_sap=["DocEntry"],
    sync_strategy=SyncStrategy.TRUNCATE_INSERT,
    sap_query=_ENTRATA_MERCI_QUERY,
)
