from .ordini_acquisto import MAPPING_ORDINI_ACQUISTO
from .ordini_acquisto_lines import MAPPING_ORDINI_ACQUISTO_LINES
from .catalogo_business_partner import MAPPING_CATALOGO_BUSINESS_PARTNER
from .anagrafiche_business_partner import MAPPING_ANAGRAFICHE_BUSINESS_PARTNER
from .anagrafica_articoli import MAPPING_ANAGRAFICHE_ARTICOLI

# Registry dei mapping
MAPPINGS_REGISTRY = {
    "anagraficheArticoli": MAPPING_ANAGRAFICHE_ARTICOLI,
    "anagraficheBusinessPartner": MAPPING_ANAGRAFICHE_BUSINESS_PARTNER,
    "catalogoBusinessPartner": MAPPING_CATALOGO_BUSINESS_PARTNER,
    "ordiniAcquisto": MAPPING_ORDINI_ACQUISTO,
    "ordiniAcquistoLines": MAPPING_ORDINI_ACQUISTO_LINES,
    # "customers": OCRD_MAPPING,
    # "vendors": OCRD_MAPPING,
}

def get_mapping(table_name: str):
    """Ottieni un mapping per nome tabella con gestione errori"""
    if table_name not in MAPPINGS_REGISTRY:
        raise ValueError(f"Mapping non trovato per: {table_name}")
    return MAPPINGS_REGISTRY[table_name]
