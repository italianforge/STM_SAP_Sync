from sqlalchemy import Column, DateTime, Integer, String
from .base import Base


class SAP_EntrataMerci(Base):
    """Testata entrata merci da ricevimento acquisto SAP (OPDN)."""
    __tablename__ = "entrata_merci"
    __table_args__ = {"schema": "sap"}

    id = Column(Integer, primary_key=True)
    date_registration = Column(DateTime)
    cod_business_partner = Column(String)
    status = Column(String(10), default=None)

    def __repr__(self):
        return (
            f"<SAP_EntrataMerci(id={self.id}, date_registration={self.date_registration}, "
            f"cod_business_partner='{self.cod_business_partner}')>"
        )
