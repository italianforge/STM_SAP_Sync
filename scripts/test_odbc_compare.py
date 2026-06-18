import os
import pyodbc

sap_cs = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=192.168.0.4;"
    "DATABASE=STM_Master;"
    "UID=SBO_READER;"
    "PWD=Sb0read@01;"
    "TrustServerCertificate=yes;"
    "Encrypt=yes"
)
deposita_cs = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=192.168.0.5;"
    "DATABASE=DBDATA;"
    "UID=Luca_gescor;"
    "PWD=G3scor@01;"
    "TrustServerCertificate=yes;"
    "Encrypt=no"
)
for name, cs in [("SAP_0.4", sap_cs), ("DEPOSYTA_0.5", deposita_cs)]:
    try:
        conn = pyodbc.connect(cs, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print(name, "OK", cur.fetchone()[0])
        conn.close()
    except Exception as e:
        print(name, "FAIL", e)
