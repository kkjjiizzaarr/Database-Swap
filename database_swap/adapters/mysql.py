"""MySQL database adapter."""

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

from typing import Dict, List, Any, Iterator
from .base import DatabaseAdapter


class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter implementation."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        if not MYSQL_AVAILABLE:
            raise ImportError("mysql-connector-python is required for MySQL support. Install it with: pip install mysql-connector-python")
        super().__init__(connection_config)
    
    def connect(self) -> bool:
        """Establish connection to MySQL database."""
        try:
            config = {
                'host': self.connection_config.get('host', 'localhost'),
                'port': self.connection_config.get('port', 3306),
                'database': self.connection_config.get('database'),
                'user': self.connection_config.get('username'),
                'password': self.connection_config.get('password'),
                'autocommit': True
            }
            
            # Remove None values
            config = {k: v for k, v in config.items() if v is not None}
            
            self.connection = mysql.connector.connect(**config)
            self.logger.info(f"Connected to MySQL database: {config.get('database')}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to MySQL: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close MySQL connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from MySQL database")
    
    def test_connection(self) -> bool:
        """Test MySQL connection."""
        try:
            if not self.connection or not self.connection.is_connected():
                return False
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"MySQL connection test failed: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """Get list of tables in MySQL database."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return tables
        except Exception as e:
            self.logger.error(f"Failed to get tables: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for MySQL table."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DESCRIBE {table_name}")
            columns = cursor.fetchall()
            
            schema = {
                'columns': {},
                'primary_keys': [],
                'indexes': []
            }
            
            for col in columns:
                col_name = col[0]
                col_type = col[1]
                nullable = col[2] == 'YES'
                default_value = col[4]
                is_pk = col[3] == 'PRI'
                
                schema['columns'][col_name] = {
                    'type': col_type,
                    'nullable': nullable,
                    'default': default_value
                }
                
                if is_pk:
                    schema['primary_keys'].append(col_name)
            
            cursor.close()
            return schema
        except Exception as e:
            self.logger.error(f"Failed to get schema for table {table_name}: {e}")
            return {}
    
    def get_table_count(self, table_name: str) -> int:
        """Get number of records in MySQL table."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            self.logger.error(f"Failed to get count for table {table_name}: {e}")
            return 0
    
    def read_data(self, table_name: str, batch_size: int = 1000, 
                  offset: int = 0) -> Iterator[List[Dict[str, Any]]]:
        """Read data from MySQL table in batches."""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            while True:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}")
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                yield rows
                offset += batch_size
                
                if len(rows) < batch_size:
                    break
            
            cursor.close()
        except Exception as e:
            self.logger.error(f"Failed to read data from table {table_name}: {e}")
            yield []
    
    def write_data(self, table_name: str, data: List[Dict[str, Any]], 
                   create_table: bool = True) -> bool:
        """Write data to MySQL table."""
        try:
            if not data:
                return True
            
            # Create table if it doesn't exist and create_table is True
            if create_table:
                self._create_table_from_data(table_name, data[0])
            
            cursor = self.connection.cursor()
            
            # Prepare INSERT statement
            columns = list(data[0].keys())
            placeholders = ', '.join(['%s' for _ in columns])
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Insert data
            for row in data:
                values = [row.get(col) for col in columns]
                cursor.execute(query, values)
            
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Failed to write data to table {table_name}: {e}")
            self.connection.rollback()
            return False
    
    def create_table(self, table_name: str, schema: Dict[str, Any]) -> bool:
        """Create MySQL table with given schema."""
        try:
            cursor = self.connection.cursor()
            
            columns_def = []
            for col_name, col_info in schema.get('columns', {}).items():
                col_type = col_info.get('type', 'TEXT')
                nullable = col_info.get('nullable', True)
                default = col_info.get('default')
                
                col_def = f"{col_name} {col_type}"
                if not nullable:
                    col_def += " NOT NULL"
                if default is not None:
                    col_def += f" DEFAULT {default}"
                
                columns_def.append(col_def)
            
            # Add primary key constraint
            primary_keys = schema.get('primary_keys', [])
            if primary_keys:
                pk_def = f"PRIMARY KEY ({', '.join(primary_keys)})"
                columns_def.append(pk_def)
            
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_def)})"
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """Drop MySQL table."""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop table {table_name}: {e}")
            return False
    
    def _create_table_from_data(self, table_name: str, sample_row: Dict[str, Any]) -> bool:
        """Create table based on sample data."""
        try:
            cursor = self.connection.cursor()
            
            # Check if table exists
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if cursor.fetchone():
                cursor.close()
                return True
            
            # Infer column types from sample data
            columns_def = []
            for col_name, value in sample_row.items():
                if isinstance(value, int):
                    col_type = "INT"
                elif isinstance(value, float):
                    col_type = "DOUBLE"
                elif isinstance(value, bool):
                    col_type = "BOOLEAN"
                else:
                    col_type = "TEXT"
                
                columns_def.append(f"{col_name} {col_type}")
            
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_def)})"
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"Failed to create table {table_name} from data: {e}")
            return False