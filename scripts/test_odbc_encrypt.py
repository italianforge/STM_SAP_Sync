import pyodbc

configs = [
    ("encrypt_yes", "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5;DATABASE=DBDATA;UID=Luca_gescor;PWD=G3scor@01;TrustServerCertificate=yes;Encrypt=yes"),
    ("encrypt_no", "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5;DATABASE=DBDATA;UID=Luca_gescor;PWD=G3scor@01;TrustServerCertificate=yes;Encrypt=no"),
    ("modula_encrypt_no", "DRIVER={ODBC Driver 18 for SQL Server};SERVER=192.168.0.5\\MODULA;DATABASE=SYSTOREDB;UID=modula_read;PWD=M0dula@01;TrustServerCertificate=yes;Encrypt=no"),
]
for name, cs in configs:
    try:
        conn = pyodbc.connect(cs, timeout=5)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        print(name, "OK", cur.fetchone()[0])
        conn.close()
    except Exception as e:
        print(name, "FAIL", e)
