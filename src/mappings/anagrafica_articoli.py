from .base import SyncStrategy, TableMapping
from ..models.anagrafica_articoli import SAP_AnagraficheArticoli
from ..utils.transformers import safe_datetime, safe_string, safe_float, safe_int
from typing import Dict, Any, Optional, Set
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

_sap_item_codes: Optional[Set[str]] = None
_debug_art_eq_stats: Dict[str, Any] = {
    "raw_nonempty": 0,
    "sanitized_ok": 0,
    "sanitized_null": 0,
    "rejected_samples": [],
}


def _reset_sap_item_codes_cache() -> None:
    global _sap_item_codes
    _sap_item_codes = None


def _load_sap_item_codes(sap_session) -> Set[str]:
    """Carica tutti gli ItemCode SAP validi (una query per sync)."""
    global _sap_item_codes
    if _sap_item_codes is None:
        rows = sap_session.execute(text("SELECT ItemCode FROM dbo.OITM")).fetchall()
        _sap_item_codes = {str(row[0]).strip() for row in rows if row[0]}
        logger.info(f"Caricati {len(_sap_item_codes)} ItemCode SAP per validazione art_equivalente")
    return _sap_item_codes


def _pre_sync_articoli(sap_session) -> None:
    global _debug_art_eq_stats
    _debug_art_eq_stats = {
        "raw_nonempty": 0,
        "sanitized_ok": 0,
        "sanitized_null": 0,
        "rejected_samples": [],
    }
    _reset_sap_item_codes_cache()
    codes = _load_sap_item_codes(sap_session)
    # #region agent log
    from ..utils.debug_session_log import debug_log
    debug_log(
        "anagrafica_articoli.py:_pre_sync_articoli",
        "ItemCode cache loaded",
        {"cache_size": len(codes)},
        hypothesis_id="H2",
    )
    # #endregion


def _sanitize_art_equivalente(value: Any) -> Optional[str]:
    """
    art_equivalente ha FK verso sap.anagrafica_articoli.id.
    Se U_SFT_SUBCAT non è un ItemCode SAP valido, azzera il valore.
    """
    if value is None:
        return None
    code = str(value).strip()
    if not code:
        return None
    # #region agent log
    _debug_art_eq_stats["raw_nonempty"] += 1
    # #endregion
    if _sap_item_codes is None or code not in _sap_item_codes:
        # #region agent log
        _debug_art_eq_stats["sanitized_null"] += 1
        samples = _debug_art_eq_stats["rejected_samples"]
        if len(samples) < 5:
            samples.append(
                {
                    "raw": code,
                    "cache_loaded": _sap_item_codes is not None,
                    "in_cache": bool(_sap_item_codes and code in _sap_item_codes),
                }
            )
        # #endregion
        return None
    # #region agent log
    _debug_art_eq_stats["sanitized_ok"] += 1
    # #endregion
    return code


def _post_transform_articoli(row: Dict[str, Any]) -> Dict[str, Any]:
    """Deriva priorita da U_Aggiuntiva (campo critico): null -> ORDINARIO, PRIORITA% -> PRIORITARIO, CRITICO -> CRITICO."""
    val = row.get("critico")
    if val is None or str(val).strip() == "":
        row["priorita"] = "ORDINARIO"
    elif str(val).upper().startswith("PRIORITA"):
        row["priorita"] = "PRIORITARIO"
    elif str(val).upper() == "CRITICO":
        row["priorita"] = "CRITICO"
    else:
        row["priorita"] = "ORDINARIO"

    # Deriva categoria da QryGroup14/15/16
    # QryGroup16 = Y -> MODULA, QryGroup14 = Y -> DEPOSITA, QryGroup15 = Y -> RICAMBI
    qry14 = str(row.pop("_qry_group14", "") or "").strip().upper()
    qry15 = str(row.pop("_qry_group15", "") or "").strip().upper()
    qry16 = str(row.pop("_qry_group16", "") or "").strip().upper()
    if qry16 == "Y":
        row["categoria"] = "MODULA"
    elif qry14 == "Y":
        row["categoria"] = "DEPOSITA"
    elif qry15 == "Y":
        row["categoria"] = "RICAMBI"
    else:
        row["categoria"] = None

    row["art_equivalente"] = _sanitize_art_equivalente(row.get("art_equivalente"))

    categoria = row.get("categoria")
    if categoria in ("RICAMBI", "MODULA"):
        row["qta_x_conf"] = 1.0

    return row


def _post_sync_articoli(pg_session, rows):
    """Callback post-sync: associazioni macchina + bootstrap magazzino + stock DEPOSYTA e MODULA."""
    # #region agent log
    from ..utils.debug_session_log import debug_log
    pg_stats = pg_session.execute(
        text(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(art_equivalente) FILTER (WHERE art_equivalente IS NOT NULL AND TRIM(art_equivalente) <> '') AS with_art_eq
            FROM sap.anagrafica_articoli
            """
        )
    ).mappings().first()
    debug_log(
        "anagrafica_articoli.py:_post_sync_articoli",
        "art_equivalente sync stats",
        {
            "delta_rows_processed": len(rows),
            **_debug_art_eq_stats,
            "pg_total": int(pg_stats["total"] or 0),
            "pg_with_art_equivalente": int(pg_stats["with_art_eq"] or 0),
        },
        hypothesis_id="H2,H4,H5",
    )
    # #endregion
    _sync_assoc_articoli_macchina(pg_session, rows)
    from ..config.database import DatabaseConfig
    from ..sync.magazzino_bootstrap import bootstrap_magazzino_from_sap
    from ..sync.deposyta_enrichment import enrich_deposita_stock
    from ..sync.modula_enrichment import enrich_modula_stock

    bootstrap_magazzino_from_sap(pg_session)
    db_config = DatabaseConfig()
    enrich_deposita_stock(pg_session, db_config)
    enrich_modula_stock(pg_session, db_config)


def _sync_assoc_articoli_macchina(pg_session, rows):
    """
    Per ogni articolo nei raw SAP rows, aggiorna sap.assoc_articoli_macchina
    splittando U_FamigliaLEV2 per '-'.
    Usa DELETE + INSERT per articolo per supportare sia sync completo che delta.
    """
    for row in rows:
        row_dict = dict(row._mapping)
        item_code = row_dict.get("ItemCode")
        if not item_code:
            continue
        pg_session.execute(
            text("DELETE FROM sap.assoc_articoli_macchina WHERE id_articolo = :id"),
            {"id": item_code}
        )
        macchina_raw = row_dict.get("U_FamigliaLEV2")
        if macchina_raw and str(macchina_raw).strip():
            for m in str(macchina_raw).split('-'):
                m = m.strip()
                if m:
                    pg_session.execute(
                        text(
                            "INSERT INTO sap.assoc_articoli_macchina (id_articolo, id_macchina) "
                            "VALUES (:id_articolo, :id_macchina) ON CONFLICT DO NOTHING"
                        ),
                        {"id_articolo": item_code, "id_macchina": m}
                    )
    pg_session.flush()

# Query su OITM (articoli).
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
    o.U_SFT_SUBCAT,
    o.U_SFT_FAMILY_LEV3,
    o.U_SFT_PURCH_SPEC,
    o.U_Dev_ArtBase,
    o.MinLevel,
    o.ReorderQty,
    o.CardCode,
    o.QryGroup14,
    o.QryGroup15,
    o.QryGroup16,
    o.UpdateDate,
    o.UpdateTS
FROM dbo.OITM o
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
        "U_FamigliaLEV3": "stato",
        "U_SFT_FAMILY_LEV1": "costruttore",
        "U_SFT_SUBCAT": "art_equivalente",
        # U_SFT_FAMILY_LEV2 -> fornitore: disabilitato, usare cod_business_partner_pref (CardCode)
        # "U_SFT_FAMILY_LEV2": "fornitore",
        "U_SFT_FAMILY_LEV3": "tipo_articolo",
        "U_SFT_PURCH_SPEC": "auto_ingranaggi",
        "U_Dev_ArtBase": "note_acquisti",  
        "MinLevel": "scorta_minima",
        "ReorderQty": "qty_riordino",
        "CardCode": "cod_business_partner_pref",   # OITM
        "QryGroup14": "_qry_group14",  # Campo temporaneo per derivare categoria
        "QryGroup15": "_qry_group15",  # Campo temporaneo per derivare categoria
        "QryGroup16": "_qry_group16",  # Campo temporaneo per derivare categoria
        "UpdateDate": "_update_date",  # Campo temporaneo per la trasformazione
        "UpdateTS": "_update_ts"       # Campo temporaneo per la trasformazione
    },
    transformations={
        "description": safe_string,
        "caratt_destination": safe_string,
        "critico": safe_string,
        "ubicazione": safe_string,
        "stato": safe_string,
        "costruttore": safe_string,
        "art_equivalente": safe_string,
        "tipo_articolo": safe_string,
        "auto_ingranaggi": safe_string,
        "note_acquisti": safe_string,
        "scorta_minima": safe_float,
        "qty_riordino": safe_float,
        "cod_business_partner_pref": safe_string,
    },
    primary_key_sap="ItemCode",
    sync_strategy=SyncStrategy.UPSERT,
    sap_query=_ANAGRAFICA_ARTICOLI_QUERY,
    sap_timestamp_prefix="o",
    pre_sync_callback=_pre_sync_articoli,
    post_transform=_post_transform_articoli,
    post_sync_callback=_post_sync_articoli,
)
