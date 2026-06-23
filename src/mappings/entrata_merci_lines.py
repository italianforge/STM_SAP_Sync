from ..models.entrata_merci_lines import SAP_EntrataMerciLine
from .base import SyncStrategy, TableMapping
from ..utils.transformers import safe_float, safe_int, safe_string


_ENTRATA_MERCI_LINES_QUERY = """
SELECT
    l.DocEntry,
    l.LineNum,
    l.ItemCode,
    l.Quantity,
    l.BaseEntry,
    l.LineStatus
FROM dbo.PDN1 l
INNER JOIN dbo.OPDN h ON l.DocEntry = h.DocEntry
WHERE h.DocDate >= DATEADD(year, -1, GETDATE())
""".strip()


def _map_line_status(value):
    """Mappa LineStatus SAP (C/O) in status (CLOSED/OPEN)."""
    if value == 'C':
        return 'CLOSED'
    return 'OPEN'


def _map_order_acquisto_ref(value):
    ref = safe_int(value)
    return ref if ref else None


MAPPING_ENTRATA_MERCI_LINES = TableMapping(
    sap_table="dbo.PDN1",
    pg_model=SAP_EntrataMerciLine,
    column_mappings={
        "DocEntry": "cod_entrata_merci",
        "LineNum": "line_num",
        "ItemCode": "cod_articolo",
        "Quantity": "quantity",
        "BaseEntry": "cod_order_acquisto",
        "LineStatus": "status",
    },
    transformations={
        "cod_entrata_merci": safe_int,
        "line_num": safe_int,
        "cod_articolo": safe_string,
        "quantity": safe_float,
        "cod_order_acquisto": _map_order_acquisto_ref,
        "status": _map_line_status,
    },
    primary_key_sap=["DocEntry", "LineNum"],
    sync_strategy=SyncStrategy.TRUNCATE_INSERT,
    sap_query=_ENTRATA_MERCI_LINES_QUERY,
)
