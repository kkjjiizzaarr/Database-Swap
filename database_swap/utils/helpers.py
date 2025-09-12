"""Helper utilities for database operations."""

import time
from typing import Dict, Any, List, Optional
import json
from datetime import datetime, date


def sanitize_table_name(name: str) -> str:
    """Sanitize table name for different database systems."""
    # Remove or replace invalid characters
    sanitized = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"table_{sanitized}"
    
    # Ensure it's not empty
    if not sanitized:
        sanitized = "unnamed_table"
    
    return sanitized


def convert_data_types(data: List[Dict[str, Any]], source_type: str, target_type: str) -> List[Dict[str, Any]]:
    """Convert data types between different database systems."""
    if source_type == target_type:
        return data
    
    converted_data = []
    
    for row in data:
        converted_row = {}
        for key, value in row.items():
            converted_row[key] = convert_value(value, source_type, target_type)
        converted_data.append(converted_row)
    
    return converted_data


def convert_value(value: Any, source_type: str, target_type: str) -> Any:
    """Convert a single value between database types."""
    if value is None:
        return None
    
    # Handle datetime objects
    if isinstance(value, (datetime, date)):
        if target_type == 'mongodb':
            return value  # MongoDB handles datetime natively
        else:
            return value.isoformat()  # Convert to string for SQL databases
    
    # Handle boolean values
    if isinstance(value, bool):
        if target_type in ['mysql', 'sqlite']:
            return int(value)  # Convert to 0/1 for SQL databases
        return value
    
    # Handle complex types for MongoDB
    if target_type == 'mongodb':
        if isinstance(value, (dict, list)):
            return value  # MongoDB handles these natively
    else:
        # Convert complex types to JSON strings for SQL databases
        if isinstance(value, (dict, list)):
            return json.dumps(value)
    
    return value


def estimate_migration_time(total_records: int, batch_size: int, rate_limit_delay: float) -> float:
    """Estimate migration time in seconds."""
    num_batches = (total_records + batch_size - 1) // batch_size  # Ceiling division
    delay_time = num_batches * rate_limit_delay
    
    # Estimate processing time (very rough estimate)
    processing_time = total_records * 0.001  # 1ms per record
    
    return processing_time + delay_time


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def validate_connection_config(config: Dict[str, Any], db_type: str) -> List[str]:
    """Validate database connection configuration."""
    errors = []
    
    required_fields = {
        'sqlite': ['database'],
        'mysql': ['host', 'database', 'username'],
        'mongodb': ['host', 'database']
    }
    
    if db_type not in required_fields:
        errors.append(f"Unsupported database type: {db_type}")
        return errors
    
    connection = config.get('connection', {})
    
    for field in required_fields[db_type]:
        if not connection.get(field):
            errors.append(f"Missing required field for {db_type}: {field}")
    
    # Validate port numbers
    port = connection.get('port')
    if port is not None:
        try:
            port_num = int(port)
            if port_num < 1 or port_num > 65535:
                errors.append("Port number must be between 1 and 65535")
        except (ValueError, TypeError):
            errors.append("Port must be a valid integer")
    
    return errors


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into chunks of specified size."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def safe_cast(value: Any, target_type: type, default: Any = None) -> Any:
    """Safely cast a value to target type with fallback."""
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return default


def get_timestamp() -> str:
    """Get current timestamp as string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class Timer:
    """Simple timer context manager."""
    
    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
    
    @property
    def duration(self) -> float:
        """Get duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    def __str__(self) -> str:
        return f"{self.name}: {format_duration(self.duration)}"