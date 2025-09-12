"""Base adapter interface for database operations."""

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Iterator, Tuple
import logging


class DatabaseAdapter(ABC):
    """Abstract base class for database adapters."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize the adapter with connection configuration."""
        self.connection_config = connection_config
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the database."""
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test if the connection is working."""
        pass
    
    @abstractmethod
    def get_tables(self) -> List[str]:
        """Get list of tables/collections in the database."""
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for a specific table."""
        pass
    
    @abstractmethod
    def get_table_count(self, table_name: str) -> int:
        """Get the number of records in a table."""
        pass
    
    @abstractmethod
    def read_data(self, table_name: str, batch_size: int = 1000, 
                  offset: int = 0) -> Iterator[List[Dict[str, Any]]]:
        """Read data from a table in batches."""
        pass
    
    @abstractmethod
    def write_data(self, table_name: str, data: List[Dict[str, Any]], 
                   create_table: bool = True) -> bool:
        """Write data to a table."""
        pass
    
    @abstractmethod
    def create_table(self, table_name: str, schema: Dict[str, Any]) -> bool:
        """Create a table with the given schema."""
        pass
    
    @abstractmethod
    def drop_table(self, table_name: str) -> bool:
        """Drop a table."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()