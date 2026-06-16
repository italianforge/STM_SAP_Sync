from ..models.catalogo_business_partner import SAP_CatalogoBusinessPartner
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_float, safe_string


# Prezzo da SPP1 (prezzi speciali BP): join su ItemCode + CardCode.
# OUTER APPLY evita duplicati OSCN quando SPP1 ha più righe (scaglioni quantità).
_CATALOGO_BUSINESS_PARTNER_QUERY = """
SELECT
    o.ItemCode,
    o.CardCode,
    o.Substitute,
    sp.Price
FROM dbo.OSCN o
OUTER APPLY (
    SELECT TOP 1 Price
    FROM dbo.SPP1
    WHERE ItemCode = o.ItemCode
      AND CardCode = o.CardCode
    ORDER BY SPP1.LINENUM
) sp
""".strip()


MAPPING_CATALOGO_BUSINESS_PARTNER = TableMapping(
    sap_table="dbo.OSCN",
    pg_model=SAP_CatalogoBusinessPartner,
    column_mappings={
        "ItemCode": "cod_articolo",
        "CardCode": "cod_business_partner",
        "Substitute": "translation",
        "Price": "prezzo",
    },
    transformations={
        "cod_articolo": safe_string,
        "cod_business_partner": safe_string,
        "translation": safe_string,
        "prezzo": safe_float,
    },
    primary_key_sap=["ItemCode", "CardCode", "Substitute"],
    sync_strategy=SyncStrategy.TRUNCATE_INSERT,
    sap_query=_CATALOGO_BUSINESS_PARTNER_QUERY,
)
