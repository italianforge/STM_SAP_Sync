from ..models.catalogo_business_partner import SAP_CatalogoBusinessPartner
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_string


MAPPING_CATALOGO_BUSINESS_PARTNER = TableMapping(
    sap_table="dbo.OSCN",
    pg_model=SAP_CatalogoBusinessPartner,  
    column_mappings={
        "ItemCode": "cod_articolo",
        "CardCode": "cod_business_partner",
        "Substitute": "translation",
    },
    transformations={
        "cod_articolo": safe_string,
        "cod_business_partner": safe_string,
        "translation": safe_string,
    },
    primary_key_sap=["ItemCode", "CardCode", "Substitute"],  # Chiave primaria multipla
    sync_strategy=SyncStrategy.TRUNCATE_INSERT  # Usa truncate e insert
)

