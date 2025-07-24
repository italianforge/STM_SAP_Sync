# STM SAP Sync

Sistema di sincronizzazione dati da SAP verso PostgreSQL.

## Struttura Progetto

```
├── src/                    # Codice sorgente principale
│   ├── config/            # Configurazioni
│   ├── models/            # Modelli database
│   ├── mappings/          # Mappings tabelle SAP->PostgreSQL
│   ├── sync/              # Engine di sincronizzazione
│   └── utils/             # Utilità e trasformatori
├── tests/                 # Test unitari
├── main.py               # Script principale
├── requirements.txt      # Dipendenze
└── .env                 # Configurazioni ambiente
```

## Installazione

1. Installa le dipendenze:
```bash
pip install -r requirements.txt
```

2. Configura le variabili d'ambiente nel file `.env`

3. Esegui la sincronizzazione:
```bash
python main.py
```

## Aggiungere Nuove Tabelle

1. Crea il modello in `src/models/`
2. Crea il mapping in `src/mappings/`
3. Registra il mapping in `src/mappings/registry.py`
4. Aggiungi la tabella alla lista in `main.py`
