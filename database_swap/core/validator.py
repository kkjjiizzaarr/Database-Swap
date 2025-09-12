"""Data validation functionality for database migration."""

from typing import Dict, Any, List, Optional, Union
import re
from datetime import datetime, date
import json


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class DataValidator:
    """Validator for data consistency and type validation."""
    
    def __init__(self, strict_mode: bool = True, data_type_validation: bool = True):
        """
        Initialize data validator.
        
        Args:
            strict_mode: If True, validation errors will raise exceptions
            data_type_validation: If True, validates data types
        """
        self.strict_mode = strict_mode
        self.data_type_validation = data_type_validation
        self.errors = []
    
    def validate_row(self, row: Dict[str, Any], schema: Optional[Dict[str, Any]] = None) -> bool:
        """
        Validate a single row of data.
        
        Args:
            row: Dictionary representing a row of data
            schema: Optional schema to validate against
            
        Returns:
            True if validation passes, False otherwise
        """
        self.errors.clear()
        
        try:
            # Basic validation
            if not isinstance(row, dict):
                self._add_error("Row must be a dictionary")
                return False
            
            if not row:
                self._add_error("Row cannot be empty")
                return False
            
            # Schema validation if provided
            if schema and self.data_type_validation:
                self._validate_against_schema(row, schema)
            
            # Data type validation
            if self.data_type_validation:
                self._validate_data_types(row)
            
            # Check for SQL injection patterns
            self._validate_sql_injection(row)
            
            return len(self.errors) == 0
            
        except Exception as e:
            self._add_error(f"Validation error: {e}")
            return False
    
    def validate_batch(self, batch: List[Dict[str, Any]], 
                      schema: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate a batch of rows.
        
        Args:
            batch: List of dictionaries representing rows
            schema: Optional schema to validate against
            
        Returns:
            Dictionary with validation results
        """
        if not batch:
            return {'valid': True, 'errors': [], 'valid_rows': 0, 'total_rows': 0}
        
        valid_rows = 0
        all_errors = []
        
        for i, row in enumerate(batch):
            if self.validate_row(row, schema):
                valid_rows += 1
            else:
                for error in self.errors:
                    all_errors.append(f"Row {i}: {error}")
        
        return {
            'valid': len(all_errors) == 0,
            'errors': all_errors,
            'valid_rows': valid_rows,
            'total_rows': len(batch),
            'success_rate': valid_rows / len(batch) if batch else 0.0
        }
    
    def _validate_against_schema(self, row: Dict[str, Any], schema: Dict[str, Any]) -> None:
        """Validate row against schema definition."""
        columns = schema.get('columns', {})
        
        for col_name, col_info in columns.items():
            if col_name in row:
                value = row[col_name]
                expected_type = col_info.get('type', '').lower()
                nullable = col_info.get('nullable', True)
                
                # Check null values
                if value is None and not nullable:
                    self._add_error(f"Column '{col_name}' cannot be null")
                    continue
                
                if value is not None:
                    # Type checking
                    if not self._is_compatible_type(value, expected_type):
                        self._add_error(
                            f"Column '{col_name}' type mismatch: "
                            f"expected {expected_type}, got {type(value).__name__}"
                        )
    
    def _validate_data_types(self, row: Dict[str, Any]) -> None:
        """Validate data types in a row."""
        for key, value in row.items():
            if value is not None:
                # Check for problematic types
                if isinstance(value, (set, frozenset)):
                    self._add_error(f"Column '{key}' contains unsupported set type")
                
                # Validate string lengths (reasonable limits)
                if isinstance(value, str) and len(value) > 65535:
                    self._add_error(f"Column '{key}' string too long ({len(value)} chars)")
                
                # Validate numeric ranges
                if isinstance(value, int) and (value < -2147483648 or value > 2147483647):
                    # Check if it fits in 32-bit integer range
                    pass  # Will be handled by database adapter
    
    def _validate_sql_injection(self, row: Dict[str, Any]) -> None:
        """Check for potential SQL injection patterns."""
        dangerous_patterns = [
            r"';.*--",  # SQL comment injection
            r"union.*select",  # UNION SELECT
            r"drop.*table",  # DROP TABLE
            r"delete.*from",  # DELETE FROM
            r"insert.*into",  # INSERT INTO
            r"update.*set",  # UPDATE SET
        ]
        
        for key, value in row.items():
            if isinstance(value, str):
                for pattern in dangerous_patterns:
                    if re.search(pattern, value.lower()):
                        self._add_error(f"Column '{key}' contains potentially dangerous SQL pattern")
                        break
    
    def _is_compatible_type(self, value: Any, expected_type: str) -> bool:
        """Check if value is compatible with expected type."""
        type_mapping = {
            'int': (int,),
            'integer': (int,),
            'bigint': (int,),
            'smallint': (int,),
            'float': (float, int),
            'double': (float, int),
            'real': (float, int),
            'decimal': (float, int),
            'numeric': (float, int),
            'text': (str,),
            'varchar': (str,),
            'char': (str,),
            'string': (str,),
            'boolean': (bool, int),
            'bool': (bool, int),
            'date': (str, date, datetime),
            'datetime': (str, date, datetime),
            'timestamp': (str, date, datetime),
            'json': (str, dict, list),
            'blob': (bytes, str),
            'binary': (bytes, str),
        }
        
        # Remove size specifications like VARCHAR(255)
        clean_type = re.sub(r'\([^)]*\)', '', expected_type).strip()
        
        if clean_type in type_mapping:
            return isinstance(value, type_mapping[clean_type])
        
        return True  # Unknown type, allow it
    
    def _add_error(self, error: str) -> None:
        """Add error to the error list."""
        self.errors.append(error)
        
        if self.strict_mode:
            raise ValidationError(error)
    
    def get_errors(self) -> List[str]:
        """Get list of validation errors."""
        return self.errors.copy()
    
    def clear_errors(self) -> None:
        """Clear error list."""
        self.errors.clear()


class SchemaValidator:
    """Validator for database schema compatibility."""
    
    def __init__(self):
        """Initialize schema validator."""
        self.errors = []
    
    def validate_schema_compatibility(self, source_schema: Dict[str, Any], 
                                    target_schema: Dict[str, Any], 
                                    source_type: str, target_type: str) -> bool:
        """
        Validate compatibility between source and target schemas.
        
        Args:
            source_schema: Source database schema
            target_schema: Target database schema
            source_type: Source database type
            target_type: Target database type
            
        Returns:
            True if schemas are compatible, False otherwise
        """
        self.errors.clear()
        
        try:
            source_columns = source_schema.get('columns', {})
            target_columns = target_schema.get('columns', {})
            
            # Check for missing columns in target
            for col_name in source_columns:
                if col_name not in target_columns:
                    self._add_schema_error(f"Column '{col_name}' missing in target schema")
            
            # Check type compatibility
            for col_name, source_col_info in source_columns.items():
                if col_name in target_columns:
                    target_col_info = target_columns[col_name]
                    
                    if not self._are_types_compatible(
                        source_col_info.get('type', ''),
                        target_col_info.get('type', ''),
                        source_type,
                        target_type
                    ):
                        self._add_schema_error(
                            f"Incompatible types for column '{col_name}': "
                            f"{source_col_info.get('type')} -> {target_col_info.get('type')}"
                        )
            
            return len(self.errors) == 0
            
        except Exception as e:
            self._add_schema_error(f"Schema validation error: {e}")
            return False
    
    def _are_types_compatible(self, source_type: str, target_type: str, 
                            source_db: str, target_db: str) -> bool:
        """Check if two column types are compatible."""
        # Normalize types
        source_type = re.sub(r'\([^)]*\)', '', source_type.lower()).strip()
        target_type = re.sub(r'\([^)]*\)', '', target_type.lower()).strip()
        
        # Define compatibility matrix
        compatible_types = {
            ('integer', 'int'): True,
            ('int', 'integer'): True,
            ('varchar', 'text'): True,
            ('text', 'varchar'): True,
            ('char', 'varchar'): True,
            ('varchar', 'char'): True,
            ('float', 'double'): True,
            ('double', 'float'): True,
            ('boolean', 'bool'): True,
            ('bool', 'boolean'): True,
        }
        
        # Exact match
        if source_type == target_type:
            return True
        
        # Check compatibility matrix
        return compatible_types.get((source_type, target_type), False)
    
    def _add_schema_error(self, error: str) -> None:
        """Add schema validation error."""
        self.errors.append(error)
    
    def get_schema_errors(self) -> List[str]:
        """Get list of schema validation errors."""
        return self.errors.copy()


def create_validator(config: Dict[str, Any]) -> DataValidator:
    """Create a data validator based on configuration."""
    return DataValidator(
        strict_mode=config.get('strict_mode', True),
        data_type_validation=config.get('data_type_validation', True)
    )