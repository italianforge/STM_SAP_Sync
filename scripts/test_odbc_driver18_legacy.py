"""Verifica connessioni ODBC: Driver 18 per SAP, FreeTDS per DEPOSYTA/MODULA."""
import pyodbc

print("Driver disponibili:", pyodbc.drivers())

VARIANTS = [
    (
        "sap_driver18",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.4;DATABASE=STM_Master;"
        "UID=SBO_READER;PWD=Sb0read@01;TrustServerCertificate=yes;Encrypt=yes",
    ),
    (
        "deposyta_freetds",
        "DRIVER={FreeTDS};SERVER=192.168.0.5;PORT=1433;DATABASE=DBDATA;"
        "UID=Luca_gescor;PWD=G3scor@01;TDS_Version=7.1",
    ),
    (
        "modula_freetds",
        r"DRIVER={FreeTDS};SERVER=192.168.0.5\MODULA;DATABASE=SYSTOREDB;"
        "UID=modula_read;PWD=M0dula@01;TDS_Version=7.1",
    ),
    (
        "modula_freetds_port",
        "DRIVER={FreeTDS};SERVER=192.168.0.5;PORT=1433;DATABASE=SYSTOREDB;"
        "UID=modula_read;PWD=M0dula@01;TDS_Version=7.1",
    ),
]

print("\n=== Test connessioni ===")
for name, cs in VARIANTS:
    try:
        conn = pyodbc.connect(cs, timeout=8)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        val = cur.fetchone()[0]
        conn.close()
        print(f"{name}: OK (SELECT 1={val})")
    except Exception as e:
        err = str(e).replace("\n", " ")[:200]
        print(f"{name}: FAIL [{type(e).__name__}] {err}")
