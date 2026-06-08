"""Esegue arricchimento DEPOSYTA e mostra statistiche."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv('.env')
os.environ.setdefault('DEPOSYTA_DB_DRIVER', 'SQL Server')

from sqlalchemy import create_engine, text
from src.config.database import DatabaseConfig, _load_mssql_url_from_env
from src.sync.deposyta_enrichment import enrich_deposita_stock

config = DatabaseConfig()
pg = config.get_pg_session()

dep = {
    r[0]
    for r in pg.execute(
        text("SELECT id FROM sap.anagrafica_articoli WHERE categoria = 'DEPOSITA'")
    ).fetchall()
}

eng = create_engine(_load_mssql_url_from_env('DEPOSYTA'), connect_args={'timeout': 30})
with eng.connect() as c:
    codes = [
        r[0]
        for r in c.execute(
            text("""
                SELECT LTRIM(RTRIM(vol.Code3))
                FROM dbo.v_objects_locations vol
                WHERE LTRIM(RTRIM(vol.Location)) = 'Magazzino'
                  AND LTRIM(RTRIM(vol.Code3)) <> ''
            """)
        ).fetchall()
    ]
overlap = [x for x in codes if x in dep]
print('DEPOSYTA codes (Magazzino):', len(codes))
print('PG DEPOSITA articoli:', len(dep))
print('Overlap:', len(overlap))
print('Sample overlap:', overlap[:10])

stats = enrich_deposita_stock(pg, config)
pg.commit()
print('Enrichment stats:', stats)

mag = pg.execute(
    text("SELECT COUNT(*) FROM magazzino WHERE posizione = 'Magazzino'")
).scalar()
print('Magazzino rows posizione Magazzino:', mag)

pg.close()
eng.dispose()
