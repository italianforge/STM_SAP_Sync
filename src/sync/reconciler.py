"""
Riconciliazione RFQ con ordini di acquisto SAP.

Per ogni richiesta di offerta con stato IN ('NEW', 'PROCESSED'), cerca il primo
ordine di acquisto SAP con:
  - stesso cod_business_partner
  - almeno una riga con lo stesso cod_articolo
  - doc_date > created_at della RFQ

Se trovato, imposta stato='COMPLETED' e ordine_acquisto_id sull'intera batch
di RFQ aperte per quella coppia (cod_articolo, cod_business_partner).
"""

from sqlalchemy import create_engine, text


_RECONCILE_SQL = text("""
WITH open_rfq AS (
    SELECT
        r.id,
        r.cod_articolo,
        r.cod_business_partner,
        r.created_at
    FROM sap.richieste_offerte r
    WHERE r.stato IN ('NEW', 'PROCESSED')
),
best_order AS (
    SELECT DISTINCT ON (r.cod_articolo, r.cod_business_partner)
        r.cod_articolo,
        r.cod_business_partner,
        o.id AS ordine_acquisto_id
    FROM open_rfq r
    JOIN sap.ordini_acquisto o
        ON o.cod_business_partner = r.cod_business_partner
        AND o.doc_date > r.created_at
    JOIN sap.ordini_acquisto_lines ol
        ON ol.cod_documento = o.code
        AND ol.cod_articolo = r.cod_articolo
    ORDER BY r.cod_articolo, r.cod_business_partner, o.doc_date ASC
)
UPDATE sap.richieste_offerte r
SET
    stato = 'COMPLETED',
    ordine_acquisto_id = b.ordine_acquisto_id
FROM best_order b
WHERE r.cod_articolo = b.cod_articolo
  AND r.cod_business_partner = b.cod_business_partner
  AND r.stato IN ('NEW', 'PROCESSED')
""")


def reconcile_rfq_with_orders(postgres_url: str, logger) -> int:
    """
    Esegue la riconciliazione RFQ → ordini di acquisto.
    Ritorna il numero di RFQ chiuse (passate a COMPLETED).
    """
    try:
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            result = conn.execute(_RECONCILE_SQL)
            closed = result.rowcount
        engine.dispose()

        if closed > 0:
            logger.info(f"Riconciliate {closed} RFQ → stato=COMPLETED")
        else:
            logger.debug("Riconciliazione RFQ: nessuna corrispondenza trovata")

        return closed
    except Exception as e:
        logger.error(f"Errore riconciliazione RFQ: {e}")
        return 0
