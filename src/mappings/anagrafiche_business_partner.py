
from .base import SyncStrategy, TableMapping
from ..models.anagrafiche_business_partner import SAP_AnagraficheBusinessPartner
from ..utils.transformers import safe_datetime, safe_string


MAPPING_ANAGRAFICHE_BUSINESS_PARTNER = TableMapping(
    sap_table="dbo.OCRD",
    pg_model=SAP_AnagraficheBusinessPartner,  
    column_mappings={
        "CardCode": "id",
        "CardName": "name",
        "CardType": "type",
        "UpdateDate": "_update_date",  # Campo temporaneo per la trasformazione
        "UpdateTS": "_update_ts"       # Campo temporaneo per la trasformazione
    },
    transformations={
        "name": safe_string,
        "type": safe_string
    },
    primary_key_sap="CardCode",
    sync_strategy=SyncStrategy.UPSERT
)