from sqlalchemy import Column, Integer, String, DateTime
from .base import Base

class SAP_OrdiniAcquisto(Base):
    """Modello per ordini di acquisto SAP"""
    __tablename__ = "SAP_ordini_acquisto"

    id = Column(Integer, primary_key=True)
    code = Column(Integer)
    doc_date = Column(DateTime)
    doc_due_date = Column(DateTime)
    cod_business_partner = Column(String)
    last_synced_at = Column(DateTime)

    def __repr__(self):
        return f"<SAP_OrdiniAcquisto(id='{self.id}', code='{self.code}', doc_date='{self.doc_date}', doc_due_date='{self.doc_due_date}', cod_business_partner='{self.cod_business_partner}')>"
