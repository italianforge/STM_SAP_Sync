FROM python:3.12.8-slim

# ODBC Driver 18 (SAP) + FreeTDS (DEPOSYTA/MODULA su SQL Server legacy)
RUN apt-get update && apt-get install -y curl gnupg gcc libpq-dev unixodbc-dev freetds-dev tdsodbc \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && printf '[global]\ntds version = 7.1\ntext size = 64512\n' > /etc/freetds/freetds.conf \
    && printf '\n[FreeTDS]\nDescription = FreeTDS Driver\nDriver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nSetup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so\nFileUsage = 1\n' >> /etc/odbcinst.ini

# Cartella di lavoro
WORKDIR /app

# Copia requisiti
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia codice
COPY . .

# Punto di ingresso
CMD ["python", "main.py"]
