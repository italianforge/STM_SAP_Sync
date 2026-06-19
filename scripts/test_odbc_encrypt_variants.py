"""Test varianti ODBC 18 per SQL Server legacy su 192.168.0.5."""
import pyodbc

BASE = (
    "DRIVER={{ODBC Driver 18 for SQL Server}};"
    "SERVER=192.168.0.5;DATABASE=DBDATA;UID=Luca_gescor;PWD=G3scor@01;"
    "TrustServerCertificate=yes"
)

VARIANTS = [
    ("encrypt_no", BASE.format() + "Encrypt=no"),
    ("encrypt_yes", BASE.format() + "Encrypt=yes"),
    ("encrypt_optional", BASE.format() + "Encrypt=optional"),
    ("encrypt_strict", BASE.format() + "Encrypt=strict"),
    (
        "server_port_explicit",
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5,1433;DATABASE=DBDATA;"
        "UID=Luca_gescor;PWD=G3scor@01;TrustServerCertificate=yes;Encrypt=no",
    ),
]

print("=== ODBC 18 encrypt variants ===")
for name, cs in VARIANTS:
    try:
        conn = pyodbc.connect(cs, timeout=8)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print(f"{name}: OK")
        conn.close()
    except Exception as e:
        print(f"{name}: FAIL {str(e)[:100]}")
