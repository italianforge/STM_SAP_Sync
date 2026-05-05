# STM SAP Sync

Servizio Python persistente per la sincronizzazione dei dati da SAP (SQL Server/MSSQL) verso il database PostgreSQL di STM Scheduler.

## Architettura

Il servizio si compone di due parti che girano in parallelo:

| Componente | Descrizione |
|---|---|
| **Scheduler loop** | Thread principale. Esegue la sync a intervallo configurabile, letto da DB. |
| **API Flask** | Thread background (daemon). Espone endpoint REST per status, trigger manuale e test connessione. Porta `SAP_API_PORT` (default `5001`). |

### Flusso di funzionamento

```
Avvio → leggi intervallo da PostgreSQL settings → sync parallela → attendi → ripeti
                         ↑                                 ↑
                  reload-config API             trigger /api/sync
```

## Struttura del progetto

```
STM_SAP_Sync/
├── main.py                     # Entry point — scheduler persistente
├── requirements.txt
├── .env                        # Configurazione ambiente (development)
├── .env.prod                   # Configurazione produzione
├── .env.example                # Template variabili d'ambiente
├── src/
│   ├── api/
│   │   └── app.py              # Flask API (health, sync, test-connection, status)
│   ├── config/
│   │   ├── database.py         # DatabaseConfig — legge credenziali SAP da PostgreSQL
│   │   └── settings.py         # Costanti di configurazione
│   ├── mappings/
│   │   ├── registry.py         # Registro dei mapping tabella→strategia
│   │   ├── base.py             # Classi base SyncMapping, SyncStrategy
│   │   ├── anagrafica_articoli.py
│   │   ├── anagrafiche_business_partner.py
│   │   ├── catalogo_business_partner.py
│   │   ├── ordini_acquisto.py
│   │   └── ordini_acquisto_lines.py
│   ├── models/                 # Modelli SQLAlchemy per PostgreSQL
│   ├── sync/
│   │   ├── engine.py           # SyncEngine — orchestrazione sync per tabella
│   │   └── services.py         # SyncStateService — gestione timestamp ultima sync
│   └── utils/
│       └── logger.py           # Setup logger applicativo
└── tests/
```

## Installazione

```bash
# Crea e attiva virtualenv
python -m venv venv
.\venv\Scripts\activate        # Windows
source venv/bin/activate       # Linux/macOS

# Installa dipendenze
pip install -r requirements.txt
```

## Configurazione

Copia `.env.example` in `.env` e compila le variabili:

```ini
# PostgreSQL (STM Scheduler)
POSTGRES_URL=postgresql://postgres:postgres@localhost:5432/stm

# Chiave Fernet per decifrare la password SAP salvata su DB
ENCRYPTION_KEY=<chiave-base64-32-byte>

# Fallback: URL diretto SAP (usato solo se le credenziali non sono in DB)
SAP_DB_URL=mssql+pyodbc://user:password@server/database?driver=SQL+Server

# Porta dell'API Flask interna
SAP_API_PORT=5001

# Ambiente (development | production)
ENV=development
```

> **Credenziali SAP da DB**: se la tabella `settings` di PostgreSQL contiene `sap_db_server`, il servizio usa quelle credenziali (la password è decifrata con `ENCRYPTION_KEY`). Il fallback `.env` è usato solo se DB non è configurato.

## Avvio

```bash
python main.py
```

Il servizio:
1. Legge l'intervallo di sync da `settings.sap_sync_interval_minutes` (default: 60 min)
2. Avvia l'API Flask su porta 5001
3. Esegue la sync iniziale
4. Aspetta l'intervallo configurato, poi ripete

Per fermare: `Ctrl+C` (shutdown graceful via `SIGINT`/`SIGTERM`).

## Intervallo di sincronizzazione

L'intervallo è gestito dalla UI di STM Scheduler → **Impostazioni → Sincronizzazione SAP**.

Viene salvato nella tabella `settings` con chiave `sap_sync_interval_minutes`.
Al salvataggio, la UI notifica il servizio via `POST /api/reload-config` affinché applichi
il nuovo valore senza riavvio.

## API Flask

| Metodo | Endpoint | Descrizione |
|--------|----------|-------------|
| `GET` | `/api/health` | Health check |
| `GET` | `/api/sync/status` | Stato sync corrente e ultimo risultato |
| `POST` | `/api/sync` | Trigger sync manuale (asincrono) |
| `POST` | `/api/reload-config` | Ricarica intervallo da DB senza riavvio |
| `POST` | `/api/test-connection` | Test connessione SAP (body JSON con credenziali) |

## Tabelle sincronizzate

| Nome logico | Tabella PostgreSQL | Strategia |
|---|---|---|
| `anagraficheArticoli` | `sap.anagrafica_articoli` | UPSERT (delta per timestamp) |
| `anagraficheBusinessPartner` | `sap.anagrafiche_business_partner` | UPSERT |
| `catalogoBusinessPartner` | `sap.catalogo_business_partner` | TRUNCATE + INSERT |
| `ordiniAcquisto` | `sap.ordini_acquisto` | UPSERT |
| `ordiniAcquistoLines` | `sap.ordini_acquisto_lines` | TRUNCATE + INSERT |

## Aggiungere una nuova tabella

1. Crea il modello SQLAlchemy in `src/models/<nome>.py`
2. Crea il mapping in `src/mappings/<nome>.py` (estendi `SyncMapping`)
3. Registra il mapping in `src/mappings/registry.py`
4. Aggiungi il nome alla lista `TABLES_TO_SYNC` in `main.py`

## Docker

```bash
# Avvia solo il servizio sync (richiede PostgreSQL già in esecuzione)
docker-compose up --build
```

Le variabili d'ambiente possono essere passate tramite `.env` o direttamente in `docker-compose.yml`.

