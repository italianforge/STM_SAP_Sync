from .base import SyncStrategy, TableMapping
from ..models.anagrafica_articoli import SAP_AnagraficheArticoli
from ..utils.transformers import safe_datetime, safe_string, safe_float, safe_int

# Mapping per la tabella OITM -> SAP_AnagraficheArticoli
MAPPING_ANAGRAFICHE_ARTICOLI = TableMapping(
    sap_table="dbo.OITM",
    pg_model=SAP_AnagraficheArticoli,  
    column_mappings={
        "ItemCode": "id",
        "ItemName": "description", 
        "FrgnName": "caratt_destination",
        "U_Aggiuntiva": "critico",   
        "U_FamigliaTornitura": "ubicazione",
        "U_FamigliaLEV2": "macchina_applicazione",
        "U_FamigliaLEV3": "stato",
        "U_SFT_FAMILY_LEV1": "costruttore",
        "U_SFT_FAMILY_LEV2": "fornitore",
        "U_SFT_FAMILY_LEV3": "tipo_articolo",
        "U_SFT_PURCH_SPEC": "auto_ingranaggi",
        "U_Dev_ArtBase": "note_acquisti",  
        "MinLevel": "scorta_minima",
        "ReorderQty": "qty_riordino",
        "CardCode": "cod_business_partner_pref",
        "UpdateDate": "_update_date",  # Campo temporaneo per la trasformazione
        "UpdateTS": "_update_ts"       # Campo temporaneo per la trasformazione
    },
    transformations={
        "description": safe_string,
        "caratt_destination": safe_string,
        "critico": safe_string,
        "ubicazione": safe_string,
        "macchina_applicazione": safe_string,
        "stato": safe_string,
        "costruttore": safe_string,
        "fornitore": safe_string,
        "tipo_articolo": safe_string,
        "auto_ingranaggi": safe_string,
        "note_acquisti": safe_string,
        "scorta_minima": safe_float,
        "qty_riordino": safe_float,
        "cod_business_partner_pref": safe_string,
    },
    primary_key_sap="ItemCode",
    sync_strategy=SyncStrategy.UPSERT
)
