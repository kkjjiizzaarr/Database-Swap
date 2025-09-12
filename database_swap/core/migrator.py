"""Core migration engine for database swap operations."""

import time
from typing import Dict, Any, List, Optional, Tuple
from ..adapters import get_adapter, DatabaseAdapter
from ..config import Config
from ..utils import (
    get_logger, ProgressLogger, Timer, convert_data_types,
    sanitize_table_name, estimate_migration_time, format_duration
)
from .rate_limiter import RateLimiter, AdaptiveRateLimiter
from .validator import DataValidator, SchemaValidator, ValidationError


class MigrationError(Exception):
    """Custom exception for migration errors."""
    pass


class MigrationStats:
    """Statistics tracker for migration operations."""
    
    def __init__(self):
        self.tables_processed = 0
        self.tables_failed = 0
        self.total_records = 0
        self.records_migrated = 0
        self.records_failed = 0
        self.start_time = None
        self.end_time = None
        self.errors = []
    
    def start(self):
        """Start timing the migration."""
        self.start_time = time.time()
    
    def finish(self):
        """Finish timing the migration."""
        self.end_time = time.time()
    
    @property
    def duration(self) -> float:
        """Get migration duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Get success rate as percentage."""
        if self.total_records == 0:
            return 100.0
        return (self.records_migrated / self.total_records) * 100
    
    def add_error(self, error: str):
        """Add an error to the stats."""
        self.errors.append(error)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            'tables_processed': self.tables_processed,
            'tables_failed': self.tables_failed,
            'total_records': self.total_records,
            'records_migrated': self.records_migrated,
            'records_failed': self.records_failed,
            'duration': self.duration,
            'success_rate': self.success_rate,
            'errors': self.errors.copy()
        }


class DatabaseMigrator:
    """Main database migration engine."""
    
    def __init__(self, config: Config):
        """
        Initialize the database migrator.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.logger = get_logger('migrator')
        self.progress_logger = ProgressLogger(self.logger)
        
        # Initialize components
        self.source_adapter = None
        self.target_adapter = None
        self.rate_limiter = None
        self.validator = None
        self.schema_validator = SchemaValidator()
        self.stats = MigrationStats()
        
        # Migration settings
        migration_config = config.get_migration_config()
        self.batch_size = migration_config.get('batch_size', 1000)
        self.max_retries = migration_config.get('max_retries', 3)
        self.timeout = migration_config.get('timeout', 30)
        self.rate_limit_delay = migration_config.get('rate_limit_delay', 0.1)
        self.tables_to_migrate = migration_config.get('tables')
        
        # Validation settings
        validation_config = config.get_validation_config()
        self.validator = DataValidator(
            strict_mode=validation_config.get('strict_mode', True),
            data_type_validation=validation_config.get('data_type_validation', True)
        )
    
    def migrate(self) -> MigrationStats:
        """
        Perform the database migration.
        
        Returns:
            MigrationStats object with migration results
        """
        self.logger.info("Starting database migration")
        self.stats.start()
        
        try:
            # Initialize connections
            self._initialize_connections()
            
            # Initialize rate limiter
            self.rate_limiter = AdaptiveRateLimiter(
                initial_delay=self.rate_limit_delay,
                min_delay=0.01,
                max_delay=5.0
            )
            
            # Get tables to migrate
            tables = self._get_tables_to_migrate()
            self.logger.info(f"Found {len(tables)} tables to migrate: {tables}")
            
            # Migrate each table
            for table_name in tables:
                try:
                    self._migrate_table(table_name)
                    self.stats.tables_processed += 1
                except Exception as e:
                    self.logger.error(f"Failed to migrate table '{table_name}': {e}")
                    self.stats.tables_failed += 1
                    self.stats.add_error(f"Table '{table_name}': {e}")
            
            self.stats.finish()
            self._log_final_stats()
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            self.stats.add_error(f"Migration failed: {e}")
            raise MigrationError(f"Migration failed: {e}")
        
        finally:
            self._cleanup_connections()
        
        return self.stats
    
    def _initialize_connections(self) -> None:
        """Initialize database connections."""
        source_config = self.config.get_source_config()
        target_config = self.config.get_target_config()
        
        # Create adapters
        self.source_adapter = get_adapter(
            source_config['type'], 
            source_config['connection']
        )
        self.target_adapter = get_adapter(
            target_config['type'], 
            target_config['connection']
        )
        
        # Test connections
        if not self.source_adapter.connect():
            raise MigrationError("Failed to connect to source database")
        
        if not self.target_adapter.connect():
            raise MigrationError("Failed to connect to target database")
        
        if not self.source_adapter.test_connection():
            raise MigrationError("Source database connection test failed")
        
        if not self.target_adapter.test_connection():
            raise MigrationError("Target database connection test failed")
        
        self.logger.info("Successfully connected to source and target databases")
    
    def _cleanup_connections(self) -> None:
        """Clean up database connections."""
        if self.source_adapter:
            self.source_adapter.disconnect()
        if self.target_adapter:
            self.target_adapter.disconnect()
    
    def _get_tables_to_migrate(self) -> List[str]:
        """Get list of tables to migrate."""
        tables_specified = self.tables_to_migrate
        if tables_specified:
            # Use specified tables
            return tables_specified
        else:
            # Get all tables from source
            return self.source_adapter.get_tables()
    
    def _migrate_table(self, table_name: str) -> None:
        """
        Migrate a single table.
        
        Args:
            table_name: Name of the table to migrate
        """
        self.logger.info(f"Starting migration of table: {table_name}")
        
        with Timer(f"Table {table_name} migration") as timer:
            # Get table info
            record_count = self.source_adapter.get_table_count(table_name)
            source_schema = self.source_adapter.get_table_schema(table_name)
            
            self.stats.total_records += record_count
            self.progress_logger.start_table(table_name, record_count)
            
            # Estimate migration time
            estimated_time = estimate_migration_time(
                record_count, self.batch_size, self.rate_limit_delay
            )
            self.logger.info(f"Estimated migration time: {format_duration(estimated_time)}")
            
            # Sanitize table name for target database
            target_table_name = sanitize_table_name(table_name)
            if target_table_name != table_name:
                self.logger.info(f"Table name sanitized: '{table_name}' -> '{target_table_name}'")
            
            # Create target table
            if not self.target_adapter.create_table(target_table_name, source_schema):
                raise MigrationError(f"Failed to create target table: {target_table_name}")
            
            # Migrate data in batches
            processed_records = 0
            failed_records = 0
            
            for batch in self.source_adapter.read_data(table_name, self.batch_size):
                if not batch:
                    break
                
                # Apply rate limiting
                self.rate_limiter.wait()
                
                # Validate batch
                validation_result = self.validator.validate_batch(batch, source_schema)
                if not validation_result['valid'] and validation_result['errors']:
                    self.logger.warning(f"Validation warnings for table '{table_name}': {validation_result['errors'][:5]}")
                
                # Convert data types if needed
                source_type = self.config.get('source.type')
                target_type = self.config.get('target.type')
                converted_batch = convert_data_types(batch, source_type, target_type)
                
                # Write batch to target
                retry_count = 0
                success = False
                
                while retry_count <= self.max_retries and not success:
                    try:
                        if self.target_adapter.write_data(target_table_name, converted_batch, False):
                            success = True
                            processed_records += len(converted_batch)
                            self.stats.records_migrated += len(converted_batch)
                            self.rate_limiter.record_operation(True)
                        else:
                            raise Exception("Write operation failed")
                    
                    except Exception as e:
                        retry_count += 1
                        failed_records += len(converted_batch)
                        self.rate_limiter.record_operation(False)
                        
                        if retry_count <= self.max_retries:
                            self.logger.warning(f"Batch write failed, retrying ({retry_count}/{self.max_retries}): {e}")
                            time.sleep(retry_count * 0.5)  # Exponential backoff
                        else:
                            self.logger.error(f"Batch write failed after {self.max_retries} retries: {e}")
                            self.stats.records_failed += len(converted_batch)
                
                # Update progress
                self.progress_logger.update_progress(processed_records, failed_records)
            
            # Verify migration
            target_count = self.target_adapter.get_table_count(target_table_name)
            
            if target_count != record_count:
                self.logger.warning(
                    f"Record count mismatch for table '{table_name}': "
                    f"source={record_count}, target={target_count}"
                )
            
            self.progress_logger.finish_table(success=True)
            self.logger.info(f"Completed table '{table_name}' migration: {timer}")
    
    def _log_final_stats(self) -> None:
        """Log final migration statistics."""
        stats = self.stats.to_dict()
        
        self.logger.info("=" * 60)
        self.logger.info("MIGRATION COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Duration: {format_duration(stats['duration'])}")
        self.logger.info(f"Tables processed: {stats['tables_processed']}")
        self.logger.info(f"Tables failed: {stats['tables_failed']}")
        self.logger.info(f"Total records: {stats['total_records']}")
        self.logger.info(f"Records migrated: {stats['records_migrated']}")
        self.logger.info(f"Records failed: {stats['records_failed']}")
        self.logger.info(f"Success rate: {stats['success_rate']:.1f}%")
        
        if stats['errors']:
            self.logger.info(f"Errors encountered: {len(stats['errors'])}")
            for error in stats['errors'][:5]:  # Show first 5 errors
                self.logger.error(f"  - {error}")
            if len(stats['errors']) > 5:
                self.logger.info(f"  ... and {len(stats['errors']) - 5} more errors")
        
        # Rate limiter stats
        if self.rate_limiter:
            rate_stats = self.rate_limiter.get_stats()
            self.logger.info(f"Rate limiter stats: {rate_stats}")
        
        self.logger.info("=" * 60)


def create_migrator(config: Config) -> DatabaseMigrator:
    """Create a database migrator instance."""
    return DatabaseMigrator(config)