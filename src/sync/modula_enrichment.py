"""
Arricchisce articoli MODULA con giacenze e scorta minima da MODULA (SYSTOREDB).

Nel ciclo SAP Sync, dopo anagrafica e bootstrap magazzino (magazzino_bootstrap):
- scorta_minima  <- DAT_ARTICOLI.ART_SOTTOSCO
- magazzino.quantita <- SUM(DAT_SCOMPART.SCO_GIAC) per SCO_ARTICOLO

Non inserisce righe in magazzino: aggiorna solo ciò creato da bootstrap_magazzino_from_sap.

Join MODULA: ART_ARTICOLO / SCO_ARTICOLO = descrizione articolo SAP
(sap.anagrafica_articoli.description, da OITM.ItemName).
"""
from __future__ import annotations

import logging
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..config.database import DatabaseConfig
from ..utils.transformers import safe_float

logger = logging.getLogger(__name__)

_MODULA_SCORTA_QUERY = """
SELECT
    LTRIM(RTRIM(a.ART_ARTICOLO)) AS articolo_descr,
    a.ART_SOTTOSCO AS scorta_minima
FROM dbo.DAT_ARTICOLI a
WHERE LTRIM(RTRIM(a.ART_ARTICOLO)) <> ''
""".strip()

_MODULA_GIACENZA_QUERY = """
SELECT
    LTRIM(RTRIM(s.SCO_ARTICOLO)) AS articolo_descr,
    SUM(s.SCO_GIAC) AS quantita
FROM dbo.DAT_SCOMPART s
WHERE LTRIM(RTRIM(s.SCO_ARTICOLO)) <> ''
GROUP BY LTRIM(RTRIM(s.SCO_ARTICOLO))
""".strip()


def _load_modula_by_description(pg_session: Session) -> dict[str, list[str]]:
    """Mappa descrizione SAP (ItemName) -> lista id articolo MODULA."""
    index: dict[str, list[str]] = defaultdict(list)
    rows = pg_session.execute(
        text(
            """
            SELECT id, description
            FROM sap.anagrafica_articoli
            WHERE categoria = 'MODULA' AND TRIM(id) <> ''
            """
        )
    ).fetchall()
    for row in rows:
        descr = (row[1] or '').strip()
        if descr:
            index[descr].append(row[0])
    return dict(index)


def _update_modula_magazzino(
    pg_session: Session,
    articolo_id: str,
    quantita: float,
    stats: dict,
) -> None:
    try:
        result = pg_session.execute(
            text(
                """
                UPDATE magazzino m
                SET quantita = :quantita,
                    data_aggiornamento = CURRENT_TIMESTAMP,
                    note = CASE
                        WHEN m.note IS NULL OR TRIM(m.note) = ''
                        THEN :note
                        WHEN m.note NOT LIKE '%MODULA%'
                        THEN m.note || ' - ' || :note
                        ELSE m.note
                    END
                FROM sap.anagrafica_articoli sa
                WHERE m.articolo = sa.id
                  AND sa.id = :articolo
                  AND sa.categoria = 'MODULA'
                  AND m.posizione = COALESCE(NULLIF(TRIM(sa.ubicazione), ''), 'NON_SPECIFICATA')
                """
            ),
            {
                'articolo': articolo_id,
                'quantita': quantita,
                'note': 'Arricchito da MODULA',
            },
        )
        if result.rowcount:
            stats['updated_magazzino'] += 1
        else:
            stats['skipped'] += 1
            logger.debug(
                'Nessuna riga magazzino per %s (bootstrap_magazzino_from_sap?)',
                articolo_id,
            )
    except Exception as e:
        logger.error('Aggiornamento magazzino %s fallito: %s', articolo_id, e)
        stats['errors'] += 1


def enrich_modula_stock(pg_session: Session, db_config: DatabaseConfig | None = None) -> dict:
    """
    Aggiorna scorta_minima e magazzino per gli articoli con categoria MODULA
    leggendo i dati da SYSTOREDB.

    Returns:
        dict con statistiche (updated_anagrafica, updated_magazzino, skipped, errors).
    """
    stats = {
        'updated_anagrafica': 0,
        'updated_magazzino': 0,
        'skipped': 0,
        'errors': 0,
    }

    config = db_config or DatabaseConfig()
    if not config.modula_db_url:
        logger.info('MODULA non configurato: skip arricchimento stock MODULA')
        return stats

    modula_by_descr = _load_modula_by_description(pg_session)
    if not modula_by_descr:
        logger.info('Nessun articolo MODULA in anagrafica: skip arricchimento MODULA')
        return stats

    modula_session = config.get_modula_session()
    try:
        scorta_rows = modula_session.execute(text(_MODULA_SCORTA_QUERY)).fetchall()
        giacenza_rows = modula_session.execute(text(_MODULA_GIACENZA_QUERY)).fetchall()
    except Exception as e:
        logger.error('Lettura stock MODULA fallita: %s', e)
        stats['errors'] += 1
        return stats
    finally:
        modula_session.close()

    for row in scorta_rows:
        row_map = dict(row._mapping)
        descr = (row_map.get('articolo_descr') or '').strip()
        articolo_ids = modula_by_descr.get(descr)
        if not descr or not articolo_ids:
            stats['skipped'] += 1
            continue

        scorta = safe_float(row_map.get('scorta_minima'))
        if scorta is None:
            stats['skipped'] += 1
            continue

        for articolo_id in articolo_ids:
            try:
                pg_session.execute(
                    text(
                        """
                        UPDATE sap.anagrafica_articoli
                        SET scorta_minima = :scorta_minima
                        WHERE id = :id AND categoria = 'MODULA'
                        """
                    ),
                    {'id': articolo_id, 'scorta_minima': scorta},
                )
                stats['updated_anagrafica'] += 1
            except Exception as e:
                logger.error('Aggiornamento scorta_minima %s fallito: %s', articolo_id, e)
                stats['errors'] += 1

    for row in giacenza_rows:
        row_map = dict(row._mapping)
        descr = (row_map.get('articolo_descr') or '').strip()
        articolo_ids = modula_by_descr.get(descr)
        if not descr or not articolo_ids:
            stats['skipped'] += 1
            continue

        quantita = safe_float(row_map.get('quantita'))
        if quantita is None:
            stats['skipped'] += 1
            continue

        for articolo_id in articolo_ids:
            _update_modula_magazzino(pg_session, articolo_id, quantita, stats)

    pg_session.flush()
    logger.info(
        'Arricchimento MODULA completato: anagrafica=%s magazzino=%s skipped=%s errors=%s',
        stats['updated_anagrafica'],
        stats['updated_magazzino'],
        stats['skipped'],
        stats['errors'],
    )
    return stats
