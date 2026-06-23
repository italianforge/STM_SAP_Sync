from sqlalchemy import Column, Float, ForeignKey, Integer, String
from .base import Base


class SAP_EntrataMerciLine(Base):
    """Righe entrata merci da ricevimento acquisto SAP (PDN1)."""
    __tablename__ = "entrata_merci_lines"
    __table_args__ = {"schema": "sap"}

    cod_entrata_merci = Column(
        Integer,
        ForeignKey("sap.entrata_merci.id", ondelete="CASCADE"),
        primary_key=True,
    )
    line_num = Column(Integer, primary_key=True)
    cod_articolo = Column(String)
    quantity = Column(Float, default=0.0)
    cod_order_acquisto = Column(
        Integer,
        ForeignKey("sap.ordini_acquisto.id"),
        nullable=True,
    )
    status = Column(String(10), default=None)

    def __repr__(self):
        return (
            f"<SAP_EntrataMerciLine(cod_entrata_merci={self.cod_entrata_merci}, "
            f"line_num={self.line_num}, cod_articolo='{self.cod_articolo}', "
            f"quantity={self.quantity})>"
        )
