"""MongoDB database adapter."""

try:
    from pymongo import MongoClient
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

from typing import Dict, List, Any, Iterator
from .base import DatabaseAdapter


class MongoDBAdapter(DatabaseAdapter):
    """MongoDB database adapter implementation."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        if not MONGODB_AVAILABLE:
            raise ImportError("pymongo is required for MongoDB support. Install it with: pip install pymongo")
        super().__init__(connection_config)
    
    def connect(self) -> bool:
        """Establish connection to MongoDB database."""
        try:
            host = self.connection_config.get('host', 'localhost')
            port = self.connection_config.get('port', 27017)
            username = self.connection_config.get('username')
            password = self.connection_config.get('password')
            database = self.connection_config.get('database')
            
            # Build connection string
            if username and password:
                connection_string = f"mongodb://{username}:{password}@{host}:{port}/{database}"
            else:
                connection_string = f"mongodb://{host}:{port}"
            
            self.client = MongoClient(connection_string)
            self.connection = self.client[database] if database else None
            
            # Test connection
            self.client.admin.command('ping')
            self.logger.info(f"Connected to MongoDB database: {database}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close MongoDB connection."""
        if hasattr(self, 'client') and self.client:
            self.client.close()
            self.client = None
            self.connection = None
            self.logger.info("Disconnected from MongoDB database")
    
    def test_connection(self) -> bool:
        """Test MongoDB connection."""
        try:
            if not hasattr(self, 'client') or not self.client:
                return False
            self.client.admin.command('ping')
            return True
        except Exception as e:
            self.logger.error(f"MongoDB connection test failed: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """Get list of collections in MongoDB database."""
        try:
            if not self.connection:
                return []
            return self.connection.list_collection_names()
        except Exception as e:
            self.logger.error(f"Failed to get collections: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema information for MongoDB collection."""
        try:
            if not self.connection:
                return {}
            
            collection = self.connection[table_name]
            
            # Sample documents to infer schema
            sample_docs = list(collection.find().limit(10))
            
            if not sample_docs:
                return {'columns': {}, 'primary_keys': ['_id'], 'indexes': []}
            
            # Infer schema from sample documents
            schema = {
                'columns': {},
                'primary_keys': ['_id'],
                'indexes': []
            }
            
            all_fields = set()
            for doc in sample_docs:
                all_fields.update(doc.keys())
            
            for field in all_fields:
                # Find the most common type for this field
                types = []
                for doc in sample_docs:
                    if field in doc:
                        value = doc[field]
                        if isinstance(value, int):
                            types.append('int')
                        elif isinstance(value, float):
                            types.append('float')
                        elif isinstance(value, bool):
                            types.append('bool')
                        elif isinstance(value, list):
                            types.append('array')
                        elif isinstance(value, dict):
                            types.append('object')
                        else:
                            types.append('string')
                
                # Use the most common type
                most_common_type = max(set(types), key=types.count) if types else 'string'
                schema['columns'][field] = {
                    'type': most_common_type,
                    'nullable': True,
                    'default': None
                }
            
            return schema
        except Exception as e:
            self.logger.error(f"Failed to get schema for collection {table_name}: {e}")
            return {}
    
    def get_table_count(self, table_name: str) -> int:
        """Get number of documents in MongoDB collection."""
        try:
            if not self.connection:
                return 0
            collection = self.connection[table_name]
            return collection.count_documents({})
        except Exception as e:
            self.logger.error(f"Failed to get count for collection {table_name}: {e}")
            return 0
    
    def read_data(self, table_name: str, batch_size: int = 1000, 
                  offset: int = 0) -> Iterator[List[Dict[str, Any]]]:
        """Read data from MongoDB collection in batches."""
        try:
            if not self.connection:
                yield []
                return
            
            collection = self.connection[table_name]
            
            while True:
                # Use skip and limit for pagination
                cursor = collection.find().skip(offset).limit(batch_size)
                documents = list(cursor)
                
                if not documents:
                    break
                
                # Convert ObjectId to string for JSON serialization
                for doc in documents:
                    if '_id' in doc:
                        doc['_id'] = str(doc['_id'])
                
                yield documents
                offset += batch_size
                
                if len(documents) < batch_size:
                    break
                    
        except Exception as e:
            self.logger.error(f"Failed to read data from collection {table_name}: {e}")
            yield []
    
    def write_data(self, table_name: str, data: List[Dict[str, Any]], 
                   create_table: bool = True) -> bool:
        """Write data to MongoDB collection."""
        try:
            if not data or not self.connection:
                return True
            
            collection = self.connection[table_name]
            
            # MongoDB automatically creates collections when data is inserted
            # Convert string _id back to ObjectId if needed
            for doc in data:
                if '_id' in doc and isinstance(doc['_id'], str):
                    try:
                        from bson import ObjectId
                        doc['_id'] = ObjectId(doc['_id'])
                    except:
                        # If conversion fails, remove _id and let MongoDB generate new one
                        del doc['_id']
            
            collection.insert_many(data)
            return True
        except Exception as e:
            self.logger.error(f"Failed to write data to collection {table_name}: {e}")
            return False
    
    def create_table(self, table_name: str, schema: Dict[str, Any]) -> bool:
        """Create MongoDB collection (collections are created automatically)."""
        try:
            if not self.connection:
                return False
            
            # MongoDB creates collections automatically when data is inserted
            # We can create it explicitly to set up indexes if needed
            collection = self.connection[table_name]
            
            # Create indexes if specified in schema
            indexes = schema.get('indexes', [])
            for index in indexes:
                collection.create_index(index)
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to create collection {table_name}: {e}")
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """Drop MongoDB collection."""
        try:
            if not self.connection:
                return False
            
            collection = self.connection[table_name]
            collection.drop()
            return True
        except Exception as e:
            self.logger.error(f"Failed to drop collection {table_name}: {e}")
            return False