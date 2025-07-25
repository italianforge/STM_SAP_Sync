from sqlalchemy import Column, Float, Integer, String
from .base import Base

class SAP_OrdiniAcquistoLine(Base):
    """Modello per le righe degli ordini di acquisto SAP"""
    __tablename__ = "SAP_ordini_acquisto_lines"

    id = Column(Integer, primary_key=True)
    cod_documento = Column(Integer, primary_key=True)
    cod_articolo = Column(String)
    quantity = Column(Float, default=0.0)

    def __repr__(self):
        return f"<SAP_OrdiniAcquistoLine(id='{self.id}', cod_documento='{self.cod_documento}', cod_articolo='{self.cod_articolo}', quantity='{self.quantity}')>"
