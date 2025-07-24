from sqlalchemy import Column, String, DateTime
from .base import Base

class SAP_AnagraficheArticoli(Base):
    """Modello per anagrafica articoli SAP"""
    __tablename__ = "SAP_anagrafica_articoli"

    id = Column(String, primary_key=True)
    description = Column(String)
    famiglia_tornitura = Column(String)
    min_level = Column(DateTime)
    last_synced_at = Column(DateTime)
    
    def __repr__(self):
        return f"<SAP_AnagraficheArticoli(id='{self.id}', description='{self.description}')>"
