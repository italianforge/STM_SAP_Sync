from .anagrafica_articoli import MAPPING_ANAGRAFICHE_ARTICOLI

# Registry dei mapping
MAPPINGS_REGISTRY = {
    "anagraficheArticoli": MAPPING_ANAGRAFICHE_ARTICOLI,
    # Aggiungi altri mapping qui
    # "customers": OCRD_MAPPING,
    # "vendors": OCRD_MAPPING,
}

def get_mapping(table_name: str):
    """Ottieni un mapping per nome tabella con gestione errori"""
    if table_name not in MAPPINGS_REGISTRY:
        raise ValueError(f"Mapping non trovato per: {table_name}")
    return MAPPINGS_REGISTRY[table_name]
