"""
Crea/aggiorna righe magazzino da sap.anagrafica_articoli nel ciclo SAP Sync.

Logica allineata a STM_Scheduler.services.magazzino_service.sync_magazzino_from_sap
(fonte canonica per il pipeline automatico: questo modulo).
"""
from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Mantenere allineata a magazzino_service.sync_magazzino_from_sap (STM_Scheduler).
_BOOTSTRAP_MAGAZZINO_SQL = """
INSERT INTO magazzino (articolo, posizione, quantita, note)
SELECT
    sa.id AS articolo,
    COALESCE(NULLIF(TRIM(sa.ubicazione), ''), 'NON_SPECIFICATA') AS posizione,
    0 AS quantita,
    CONCAT('Sincronizzato da SAP - ', COALESCE(sa.description, '')) AS note
FROM sap.anagrafica_articoli sa
WHERE sa.id IS NOT NULL AND TRIM(sa.id) != ''
ON CONFLICT (articolo, posizione)
DO UPDATE SET
    note = CASE
        WHEN magazzino.note NOT LIKE '%Sincronizzato da SAP%'
        THEN CONCAT(magazzino.note, ' - Sincronizzato da SAP - ',
             COALESCE((SELECT description FROM sap.anagrafica_articoli WHERE id = EXCLUDED.articolo), ''))
        ELSE CONCAT('Sincronizzato da SAP - ',
             COALESCE((SELECT description FROM sap.anagrafica_articoli WHERE id = EXCLUDED.articolo), ''))
    END,
    data_aggiornamento = CURRENT_TIMESTAMP
""".strip()


def bootstrap_magazzino_from_sap(pg_session: Session) -> int:
    """
    Inserisce righe magazzino (quantita=0) per tutti gli articoli SAP.
    Su conflitto aggiorna solo note e data_aggiornamento, mai quantita.

    Returns:
        Numero di righe inserite/aggiornate (rowcount).
    """
    result = pg_session.execute(text(_BOOTSTRAP_MAGAZZINO_SQL))
    rowcount = result.rowcount or 0
    logger.info('Bootstrap magazzino da SAP: %s righe inserite/aggiornate', rowcount)
    return rowcount
