from .base import TableMapping
from .anagrafica_articoli import MAPPING_ANAGRAFICHE_ARTICOLI
from .anagrafiche_business_partner import MAPPING_ANAGRAFICHE_BUSINESS_PARTNER
from .ordini_acquisto import MAPPING_ORDINI_ACQUISTO
from .registry import MAPPINGS_REGISTRY

__all__ = ['TableMapping', 
           'MAPPING_ANAGRAFICHE_ARTICOLI', 
           'MAPPING_ANAGRAFICHE_BUSINESS_PARTNER', 
           'MAPPING_ORDINI_ACQUISTO',
           'MAPPINGS_REGISTRY']
