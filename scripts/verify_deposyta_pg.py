import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv('.env')
from sqlalchemy import create_engine, text

pg = create_engine(os.environ['POSTGRES_URL'])
with pg.connect() as c:
    mag = c.execute(text(
        "SELECT COUNT(*), COALESCE(SUM(quantita), 0) FROM magazzino WHERE posizione = 'Magazzino'"
    )).fetchone()
    dep_scorta = c.execute(text(
        "SELECT COUNT(*) FROM sap.anagrafica_articoli WHERE categoria = 'DEPOSITA' AND scorta_minima > 0"
    )).scalar()
    de00074 = c.execute(text(
        "SELECT articolo, posizione, quantita, note FROM magazzino WHERE articolo = 'DE00074'"
    )).fetchall()
    with_qty = c.execute(text("""
        SELECT COUNT(*) FROM magazzino m
        JOIN sap.anagrafica_articoli sa ON sa.id = m.articolo
        WHERE sa.categoria = 'DEPOSITA' AND m.quantita > 0
    """)).scalar()
    print('magazzino posizione Magazzino (legacy):', mag)
    print('DEPOSITA con scorta_minima > 0:', dep_scorta)
    print('DEPOSITA magazzino con qty > 0:', with_qty)
    print('DE00074 rows:', de00074)
