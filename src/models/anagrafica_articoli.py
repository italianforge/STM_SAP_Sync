from sqlalchemy import Column, Float, String, DateTime
from .base import Base

class SAP_AnagraficheArticoli(Base):
    """Modello per anagrafica articoli SAP"""
    __tablename__ = "SAP_anagrafica_articoli"

    id = Column(String, primary_key=True)
    description = Column(String)
    caratt_destination = Column(String)
    critico = Column(String)
    ubicazione = Column(String)
    macchina_applicazione = Column(String)
    stato = Column(String)
    costruttore = Column(String)
    fornitore = Column(String)
    tipo_articolo = Column(String)
    auto_ingranaggi = Column(String)
    note_acquisti = Column(String)
    scorta_minima = Column(Float)
    qty_riordino = Column(Float)
    cod_business_partner_pref = Column(String)
    last_synced_at = Column(DateTime)
    
    def __repr__(self):
        return f"<SAP_AnagraficheArticoli(id='{self.id}', description='{self.description}')>"
