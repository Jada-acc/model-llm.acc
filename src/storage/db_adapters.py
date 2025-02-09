import sqlite3
from datetime import datetime

def adapt_datetime(dt: datetime) -> str:
    """Convert datetime to ISO format string for SQLite storage."""
    return dt.isoformat()

def convert_datetime(s: str) -> datetime:
    """Convert ISO format string back to datetime."""
    return datetime.fromisoformat(s)

def register_adapters():
    """Register custom adapters for SQLite."""
    sqlite3.register_adapter(datetime, adapt_datetime)
    sqlite3.register_converter("datetime", convert_datetime) 