"""Database adapters for different database systems."""

from .base import DatabaseAdapter
from .sqlite import SQLiteAdapter

# Import other adapters conditionally
try:
    from .mysql import MySQLAdapter
    MYSQL_AVAILABLE = True
except ImportError:
    MySQLAdapter = None
    MYSQL_AVAILABLE = False

try:
    from .mongodb import MongoDBAdapter
    MONGODB_AVAILABLE = True
except ImportError:
    MongoDBAdapter = None
    MONGODB_AVAILABLE = False

__all__ = ['DatabaseAdapter', 'SQLiteAdapter']
if MYSQL_AVAILABLE:
    __all__.append('MySQLAdapter')
if MONGODB_AVAILABLE:
    __all__.append('MongoDBAdapter')


def get_adapter(db_type: str, connection_config: dict) -> DatabaseAdapter:
    """Factory function to get the appropriate database adapter."""
    adapters = {
        'sqlite': SQLiteAdapter,
    }
    
    if MYSQL_AVAILABLE and MySQLAdapter:
        adapters['mysql'] = MySQLAdapter
    if MONGODB_AVAILABLE and MongoDBAdapter:
        adapters['mongodb'] = MongoDBAdapter
    
    if db_type not in adapters:
        available_types = list(adapters.keys())
        raise ValueError(f"Unsupported database type: {db_type}. Available types: {available_types}")
    
    return adapters[db_type](connection_config)