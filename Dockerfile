FROM python:3.12.8-slim

# Installazione dipendenze di sistema
RUN apt-get update && apt-get install -y gcc libpq-dev unixodbc-dev

# Cartella di lavoro
WORKDIR /app

# Copia requisiti
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia codice
COPY . .

# Punto di ingresso
CMD ["python", "sync.py"]
