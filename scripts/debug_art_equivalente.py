"""Diagnostica art_equivalente: confronta SAP (U_SFT_SUBCAT) vs PostgreSQL."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
from sqlalchemy import text

from src.config.database import DatabaseConfig
from src.utils.debug_session_log import debug_log


def main(env_file: str | None = None) -> None:
    load_dotenv(ROOT / (env_file or ".env"))
    db = DatabaseConfig()
    sap = db.get_sap_session()
    pg = db.get_pg_session()

    try:
        sap_rows = sap.execute(
            text(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN NULLIF(LTRIM(RTRIM(U_SFT_SUBCAT)), '') IS NOT NULL THEN 1 ELSE 0 END) AS with_subcat
                FROM dbo.OITM
                """
            )
        ).mappings().first()

        item_codes = {
            str(r[0]).strip()
            for r in sap.execute(text("SELECT ItemCode FROM dbo.OITM")).fetchall()
            if r[0]
        }

        invalid_samples = sap.execute(
            text(
                """
                SELECT TOP 5 ItemCode, U_SFT_SUBCAT
                FROM dbo.OITM
                WHERE NULLIF(LTRIM(RTRIM(U_SFT_SUBCAT)), '') IS NOT NULL
                  AND LTRIM(RTRIM(U_SFT_SUBCAT)) NOT IN (
                      SELECT ItemCode FROM dbo.OITM
                  )
                """
            )
        ).fetchall()

        pg_rows = pg.execute(
            text(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(art_equivalente) FILTER (
                        WHERE art_equivalente IS NOT NULL AND TRIM(art_equivalente) <> ''
                    ) AS with_art_eq
                FROM sap.anagrafica_articoli
                """
            )
        ).mappings().first()

        last_sync = pg.execute(
            text(
                """
                SELECT last_sync_timestamp
                FROM sap.sync_state
                WHERE table_name = 'anagraficheArticoli'
                ORDER BY last_sync_timestamp DESC
                LIMIT 1
                """
            )
        ).scalar()

        column_exists = pg.execute(
            text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'sap'
                      AND table_name = 'anagrafica_articoli'
                      AND column_name = 'art_equivalente'
                ) AS exists
                """
            )
        ).scalar()

        debug_log(
            "scripts/debug_art_equivalente.py:main",
            "SAP vs PG art_equivalente diagnostic",
            {
                "sap_total": int(sap_rows["total"] or 0),
                "sap_with_u_sft_subcat": int(sap_rows["with_subcat"] or 0),
                "sap_item_codes": len(item_codes),
                "invalid_subcat_samples": [
                    {"item": r[0], "u_sft_subcat": r[1]} for r in invalid_samples
                ],
                "pg_total": int(pg_rows["total"] or 0),
                "pg_with_art_equivalente": int(pg_rows["with_art_eq"] or 0),
                "pg_column_exists": bool(column_exists),
                "last_sync_anagraficheArticoli": last_sync.isoformat() if last_sync else None,
            },
            hypothesis_id="H1,H2,H3,H5",
            run_id="diagnostic",
        )

        print(
            f"SAP U_SFT_SUBCAT valorizzati: {sap_rows['with_subcat']}/{sap_rows['total']}"
        )
        print(
            f"PG art_equivalente valorizzati: {pg_rows['with_art_eq']}/{pg_rows['total']}"
        )
        print(f"Ultimo sync anagraficheArticoli: {last_sync}")
        print(f"Colonna art_equivalente presente: {column_exists}")
        if invalid_samples:
            print("Esempi U_SFT_SUBCAT non-ItemCode:", invalid_samples[:3])
    finally:
        sap.close()
        pg.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Diagnostica art_equivalente SAP vs PostgreSQL")
    parser.add_argument(
        "--env-file",
        default=".env",
        help="File env da caricare (es. .env.prod sul server di produzione)",
    )
    args = parser.parse_args()
    main(env_file=args.env_file)
