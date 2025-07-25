from sqlalchemy import Column, String, DateTime
from .base import Base

class SAP_AnagraficheBusinessPartner(Base):
    """Modello per anagrafica business partner SAP"""
    __tablename__ = "SAP_anagrafica_business_partner"

    id = Column(String, primary_key=True)
    name = Column(String)
    type = Column(String)
    last_synced_at = Column(DateTime)
    
    def __repr__(self):
        return f"<SAP_AnagraficheBusinessPartner(id='{self.id}', name='{self.name}', type='{self.type}')>"
