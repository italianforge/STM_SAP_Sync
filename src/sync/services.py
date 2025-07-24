from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import Session
from ..models.sync_state import SAP_SyncState

class SyncStateService:
    """Servizio per gestire lo stato delle sincronizzazioni"""
    
    @staticmethod
    def get_last_sync(session: Session, table_name: str) -> datetime:
        """Ottieni l'ultimo timestamp di sincronizzazione per una tabella"""
        return session.query(func.max(SAP_SyncState.last_synced_at)).filter(
            SAP_SyncState.table_name == table_name
        ).scalar()
    
    @staticmethod
    def update_last_sync(session: Session, table_name: str, ts: datetime):
        """Aggiorna l'ultimo timestamp di sincronizzazione"""
        state = session.query(SAP_SyncState).filter_by(table_name=table_name).first()
        if state:
            state.last_synced_at = ts
        else:
            session.add(SAP_SyncState(table_name=table_name, last_synced_at=ts))
