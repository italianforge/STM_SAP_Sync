"""Test rapido connessioni DEPOSYTA e MODULA."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sqlalchemy import text

from src.config.database import DatabaseConfig


def _probe(name: str, get_session) -> None:
    try:
        session = get_session()
        try:
            val = session.execute(text("SELECT 1")).scalar()
            print(f"{name}: OK (SELECT 1 = {val})")
        finally:
            session.close()
    except Exception as e:
        print(f"{name}: FAIL ({type(e).__name__}: {e})")


def main() -> None:
    cfg = DatabaseConfig()
    print(f"deposyta_url configured: {bool(cfg.deposyta_db_url)}")
    print(f"modula_url configured: {bool(cfg.modula_db_url)}")
    if cfg.deposyta_db_url:
        _probe("DEPOSYTA", cfg.get_deposyta_session)
    if cfg.modula_db_url:
        _probe("MODULA", cfg.get_modula_session)


if __name__ == "__main__":
    main()
