from .base import SyncStrategy, TableMapping
from ..models.anagrafica_articoli import SAP_AnagraficheArticoli
from ..utils.transformers import safe_datetime, safe_string, safe_float, safe_int

# Query con JOIN tra OITM (articoli) e OSCN (ricambi/fornitori preferiti per articolo).
# OSCN può avere più righe per articolo; si prende un solo valore tramite subquery con MAX.
_ANAGRAFICA_ARTICOLI_QUERY = """
SELECT
    o.ItemCode,
    o.ItemName,
    o.FrgnName,
    o.U_Aggiuntiva,
    o.U_FamigliaTornitura,
    o.U_FamigliaLEV2,
    o.U_FamigliaLEV3,
    o.U_SFT_FAMILY_LEV1,
    o.U_SFT_FAMILY_LEV2,
    o.U_SFT_FAMILY_LEV3,
    o.U_SFT_PURCH_SPEC,
    o.U_Dev_ArtBase,
    o.MinLevel,
    o.ReorderQty,
    o.CardCode,
    o.UpdateDate,
    o.UpdateTS,
    s.Substitute,
    s.CardCode AS CardCodeOSCN
FROM dbo.OITM o
LEFT JOIN (
    SELECT ItemCode, MAX(Substitute) AS Substitute, MAX(CardCode) AS CardCode
    FROM dbo.OSCN
    GROUP BY ItemCode
) s ON o.ItemCode = s.ItemCode
""".strip()

# Mapping per la tabella OITM + OSCN -> SAP_AnagraficheArticoli
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
        "CardCode": "cod_business_partner_pref",   # OITM
        "Substitute": "cod_ricambio",              # OSCN
        "CardCodeOSCN": "cod_fornitore",           # OSCN (aliasato per evitare conflitto con OITM.CardCode)
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
        "cod_ricambio": safe_string,
        "cod_fornitore": safe_string,
    },
    primary_key_sap="ItemCode",
    sync_strategy=SyncStrategy.UPSERT,
    sap_query=_ANAGRAFICA_ARTICOLI_QUERY,
    sap_timestamp_prefix="o",
)
