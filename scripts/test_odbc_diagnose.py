"""Diagnostica ODBC verso DEPOSYTA: distingue rete vs credenziali vs SQL Server."""
import pyodbc

VARIANTS = [
    (
        "deposita_encrypt_no",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5;DATABASE=DBDATA;"
        "UID=Luca_gescor;PWD=G3scor@01;TrustServerCertificate=yes;Encrypt=no",
    ),
    (
        "deposita_encrypt_yes",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5;DATABASE=DBDATA;"
        "UID=Luca_gescor;PWD=G3scor@01;TrustServerCertificate=yes;Encrypt=yes",
    ),
    (
        "master_db",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5;DATABASE=master;"
        "UID=Luca_gescor;PWD=G3scor@01;TrustServerCertificate=yes;Encrypt=no",
    ),
    (
        "wrong_password",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5;DATABASE=DBDATA;"
        "UID=Luca_gescor;PWD=wrongpassword;TrustServerCertificate=yes;Encrypt=no",
    ),
    (
        "sap_control",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.4;DATABASE=STM_Master;"
        "UID=SBO_READER;PWD=Sb0read@01;TrustServerCertificate=yes;Encrypt=yes",
    ),
]

print("=== ODBC diagnose ===")
for name, cs in VARIANTS:
    try:
        conn = pyodbc.connect(cs, timeout=8)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        val = cur.fetchone()[0]
        conn.close()
        print(f"{name}: OK (SELECT 1 = {val})")
    except Exception as e:
        err = str(e).replace("\n", " ")[:150]
        print(f"{name}: FAIL [{type(e).__name__}] {err}")
