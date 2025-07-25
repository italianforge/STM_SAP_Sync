from sqlalchemy import Column, String, DateTime
from .base import Base

class SAP_CatalogoBusinessPartner(Base):
    """Modello per catalogo business partner SAP"""
    __tablename__ = "SAP_catalogo_business_partner"

    cod_articolo = Column(String, primary_key=True)
    cod_business_partner = Column(String, primary_key=True)
    translation = Column(String, primary_key=True)
    
    def __repr__(self):
        return f"<SAP_CatalogoBusinessPartner(cod_articolo='{self.cod_articolo}', cod_business_partner='{self.cod_business_partner}', translation='{self.translation}')>"
