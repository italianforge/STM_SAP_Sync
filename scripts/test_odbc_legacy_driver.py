"""Diagnostica ODBC con driver legacy SQL Server (come in locale Windows)."""
import pyodbc

VARIANTS = [
    (
        "deposita_sql_server_driver",
        "DRIVER={SQL Server};SERVER=192.168.0.5;DATABASE=DBDATA;"
        "UID=Luca_gescor;PWD=G3scor@01",
    ),
    (
        "modula_sql_server_driver",
        "DRIVER={SQL Server};SERVER=192.168.0.5\\MODULA;DATABASE=SYSTOREDB;"
        "UID=modula_read;PWD=M0dula@01",
    ),
]

print("=== ODBC SQL Server driver (Windows) ===")
for name, cs in VARIANTS:
    try:
        conn = pyodbc.connect(cs, timeout=8)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        val = cur.fetchone()[0]
        conn.close()
        print(f"{name}: OK (SELECT 1 = {val})")
    except Exception as e:
        print(f"{name}: FAIL [{type(e).__name__}] {str(e)[:150]}")
