from sqlalchemy import Column, String, DateTime
from .base import Base

class SAP_SyncState(Base):
    """Tabella per tracciare lo stato delle sincronizzazioni"""
    __tablename__ = "sync_state"
    __table_args__ = {"schema": "sap"}
    
    table_name = Column(String, primary_key=True)
    last_synced_at = Column(DateTime, name="last_sync_timestamp")
    
    def __repr__(self):
        return f"<SAP_SyncState(table_name='{self.table_name}', last_synced_at='{self.last_synced_at}')>"
