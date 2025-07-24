from datetime import datetime, timedelta, date
from typing import Any, Optional

def safe_datetime(value: Any) -> Optional[datetime]:
    """Converte safely a datetime, handling None and SAP timestamps"""
    if value is None or value == '':
        return None
    
    # Se è già un datetime, restituiscilo
    if isinstance(value, (datetime, date)):
        return value
    
    # Se è una stringa ISO
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            return None
    
    # Se è un numero (timestamp SAP)
    if isinstance(value, (int, float)):
        try:
            # Per ora usa 1899-12-30 (standard SAP)
            if value > 0:
                result = datetime(1899, 12, 30) + timedelta(days=int(value))
                return result
        except Exception as e:
            return None
    
    return None

def safe_float(value: Any) -> Optional[float]:
    """Converte safely a float, handling None and empty strings"""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except:
        return None

def safe_string(value: Any) -> Optional[str]:
    """Converte safely a string, handling None"""
    return str(value) if value is not None else None

def parse_update_ts(update_date: datetime, update_ts: int) -> Optional[datetime]:
    """Combina UpdateDate e UpdateTS di SAP per ottenere il timestamp completo"""
    if not update_date or update_ts is None:
        return None
    
    try:
        ts_str = f"{update_ts:06d}"  # pad left with zeros
        hours = int(ts_str[0:2])
        minutes = int(ts_str[2:4])
        seconds = int(ts_str[4:6])
        full_datetime = update_date.replace(hour=hours, minute=minutes, second=seconds)
        return full_datetime
    except Exception as e:
        return None

def transform_sap_timestamp(row_data: dict) -> Optional[datetime]:
    """Trasforma i campi UpdateDate e UpdateTS in un singolo timestamp"""
    update_date = row_data.get('UpdateDate')
    update_ts = row_data.get('UpdateTS')
    return parse_update_ts(update_date, update_ts)
