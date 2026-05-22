FROM python:3.12.8-slim

# Installazione dipendenze di sistema + Microsoft ODBC Driver 18 for SQL Server
RUN apt-get update && apt-get install -y curl gnupg gcc libpq-dev unixodbc-dev \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Cartella di lavoro
WORKDIR /app

# Copia requisiti
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia codice
COPY . .

# Punto di ingresso
CMD ["python", "main.py"]
