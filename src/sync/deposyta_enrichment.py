"""
Arricchisce articoli DEPOSITA con giacenze e scorta minima da DEPOSYTA (DBDATA).

Dopo la sync SAP (anagrafica + riga magazzino creata da sync_magazzino_from_sap):
- scorta_minima  <- v_objects_locations.MinQuantity * Oggetti.QtaConfezione
- magazzino.quantita <- UPDATE sulla riga SAP esistente (stessa posizione: ubicazione o NON_SPECIFICATA)

Non inserisce mai righe in magazzino: aggiorna solo ciò che SAP ha già creato.

Join DEPOSYTA: dbo.Oggetti.Codice3 = codice articolo SAP (sap.anagrafica_articoli.id).
Si considera solo v_objects_locations.Location = 'Magazzino'.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..config.database import DatabaseConfig
from ..utils.transformers import safe_float

logger = logging.getLogger(__name__)

# Per ogni Code3 possono esistere più righe in v_objects_locations:
# si usa solo quella con Location = 'Magazzino'.
# Join: v_objects_locations.Code3 = Oggetti.Codice3 (codice articolo SAP).
_DEPOSYTA_STOCK_QUERY = """
SELECT
    LTRIM(RTRIM(vol.Code3)) AS codice_articolo,
    vol.MinQuantity AS min_quantity_confezioni,
    vol.Quantity AS quantity_confezioni,
    o.QtaConfezione AS qta_confezione
FROM dbo.v_objects_locations vol
INNER JOIN dbo.Oggetti o
    ON LTRIM(RTRIM(o.Codice3)) = LTRIM(RTRIM(vol.Code3))
WHERE LTRIM(RTRIM(vol.Code3)) <> ''
  AND LTRIM(RTRIM(vol.Location)) = 'Magazzino'
""".strip()

def pezzi_da_confezioni(quantity_confezioni: Any, qta_confezione: Any) -> float | None:
    """Converte quantità in confezioni in pezzi usando QtaConfezione."""
    qty = safe_float(quantity_confezioni)
    if qty is None:
        return None
    conf = safe_float(qta_confezione)
    if conf is None or conf <= 0:
        conf = 1.0
    return qty * conf


def enrich_deposita_stock(pg_session: Session, db_config: DatabaseConfig | None = None) -> dict:
    """
    Aggiorna scorta_minima e magazzino per gli articoli con categoria DEPOSITA
    leggendo i dati da DEPOSYTA.

    Returns:
        dict con statistiche (updated_anagrafica, updated_magazzino, skipped, errors).
    """
    stats = {
        'updated_anagrafica': 0,
        'updated_magazzino': 0,
        'removed_placeholders': 0,
        'skipped': 0,
        'errors': 0,
    }

    config = db_config or DatabaseConfig()
    if not config.deposyta_db_url:
        logger.info('DEPOSYTA non configurato: skip arricchimento stock DEPOSITA')
        return stats

    deposita_ids = {
        row[0]
        for row in pg_session.execute(
            text(
                "SELECT id FROM sap.anagrafica_articoli "
                "WHERE categoria = 'DEPOSITA' AND TRIM(id) <> ''"
            )
        ).fetchall()
    }
    if not deposita_ids:
        logger.info('Nessun articolo DEPOSITA in anagrafica: skip arricchimento DEPOSYTA')
        return stats

    # Rimuove righe spurie create da versioni precedenti (posizione 'Magazzino').
    removed = pg_session.execute(
        text(
            """
            DELETE FROM magazzino m
            USING sap.anagrafica_articoli sa
            WHERE m.articolo = sa.id
              AND sa.categoria = 'DEPOSITA'
              AND m.posizione = 'Magazzino'
            """
        )
    )
    stats['removed_placeholders'] = removed.rowcount or 0
    if stats['removed_placeholders']:
        logger.info(
            'Rimosse %s righe magazzino spurie (posizione Magazzino)',
            stats['removed_placeholders'],
        )

    deposyta_session = config.get_deposyta_session()
    try:
        rows = deposyta_session.execute(text(_DEPOSYTA_STOCK_QUERY)).fetchall()
    except Exception as e:
        logger.error('Lettura stock DEPOSYTA fallita: %s', e)
        stats['errors'] += 1
        deposyta_session.close()
        raise
    finally:
        deposyta_session.close()

    for row in rows:
        row_map = dict(row._mapping)
        codice = (row_map.get('codice_articolo') or '').strip()
        if not codice or codice not in deposita_ids:
            stats['skipped'] += 1
            continue

        scorta_pezzi = pezzi_da_confezioni(
            row_map.get('min_quantity_confezioni'),
            row_map.get('qta_confezione'),
        )
        quantita_pezzi = pezzi_da_confezioni(
            row_map.get('quantity_confezioni'),
            row_map.get('qta_confezione'),
        )
        if quantita_pezzi is None:
            stats['skipped'] += 1
            continue

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
                            WHEN m.note NOT LIKE '%DEPOSYTA%'
                            THEN m.note || ' - ' || :note
                            ELSE m.note
                        END
                    FROM sap.anagrafica_articoli sa
                    WHERE m.articolo = sa.id
                      AND sa.id = :articolo
                      AND sa.categoria = 'DEPOSITA'
                      AND m.posizione = COALESCE(NULLIF(TRIM(sa.ubicazione), ''), 'NON_SPECIFICATA')
                    """
                ),
                {
                    'articolo': codice,
                    'quantita': quantita_pezzi,
                    'note': 'Arricchito da DEPOSYTA',
                },
            )
            if result.rowcount:
                stats['updated_magazzino'] += 1
            else:
                stats['skipped'] += 1
                logger.debug(
                    'Nessuna riga magazzino SAP per %s (sync_magazzino_from_sap non eseguita?)',
                    codice,
                )
        except Exception as e:
            logger.error('Aggiornamento magazzino %s fallito: %s', codice, e)
            stats['errors'] += 1

        if scorta_pezzi is not None:
            try:
                pg_session.execute(
                    text(
                        """
                        UPDATE sap.anagrafica_articoli
                        SET scorta_minima = :scorta_minima
                        WHERE id = :id AND categoria = 'DEPOSITA'
                        """
                    ),
                    {'id': codice, 'scorta_minima': scorta_pezzi},
                )
                stats['updated_anagrafica'] += 1
            except Exception as e:
                logger.error('Aggiornamento scorta_minima %s fallito: %s', codice, e)
                stats['errors'] += 1

    pg_session.flush()
    logger.info(
        'Arricchimento DEPOSYTA completato: anagrafica=%s magazzino=%s '
        'removed=%s skipped=%s errors=%s',
        stats['updated_anagrafica'],
        stats['updated_magazzino'],
        stats['removed_placeholders'],
        stats['skipped'],
        stats['errors'],
    )
    return stats
