# Database Swap

A powerful Python tool for migrating data between different database systems (MongoDB, MySQL, SQLite). Database Swap provides a reliable, fast, and user-friendly way to transfer data with proper validations, error handling, rate limiting, and progress tracking.

## Features

- **Multi-Database Support**: SQLite, MySQL, and MongoDB
- **Robust Migration Engine**: Batch processing with configurable sizes
- **Rate Limiting**: Prevents overwhelming target databases
- **Data Validation**: Comprehensive validation with error reporting
- **Error Handling**: Retry mechanisms and detailed error logging
- **Progress Tracking**: Real-time progress updates and statistics
- **Flexible Configuration**: YAML configuration files and CLI arguments
- **Modular Architecture**: Easy to extend for additional database types
- **CLI Interface**: User-friendly command-line interface
- **Dry Run Mode**: Test configurations without actual migration

## Installation

### From Source

```bash
git clone https://github.com/zaber-dev/Database-Swap.git
cd Database-Swap
pip install -r requirements.txt
pip install -e .
```

### Using pip (when published)

```bash
pip install database-swap
```

## Quick Start

### 1. Initialize Configuration

```bash
database-swap init-config
```

This creates a `config.yaml` file with default settings.

### 2. Test Database Connections

```bash
# Test SQLite connection
database-swap test-connection --db-type sqlite --database source.db

# Test MySQL connection
database-swap test-connection --db-type mysql --host localhost --database mydb --username user --password

# Test MongoDB connection
database-swap test-connection --db-type mongodb --host localhost --database mydb
```

### 3. Analyze Database Structure

```bash
# Analyze entire database
database-swap analyze --db-type sqlite --database source.db

# Analyze specific table
database-swap analyze --db-type mysql --host localhost --database mydb --username user --table users
```

### 4. Perform Migration

```bash
# Dry run (recommended first)
database-swap migrate --dry-run --source-type sqlite --source-database source.db --target-type mysql --target-host localhost --target-database target_db --target-username user

# Actual migration
database-swap migrate --source-type sqlite --source-database source.db --target-type mysql --target-host localhost --target-database target_db --target-username user
```

## Configuration

### Configuration File (config.yaml)

```yaml
source:
  type: "sqlite"  # mongodb, mysql, sqlite
  connection:
    host: "localhost"
    port: null
    database: "source.db"
    username: null
    password: null
    
target:
  type: "mongodb"  # mongodb, mysql, sqlite
  connection:
    host: "localhost"
    port: 27017
    database: "target_db"
    username: null
    password: null

migration:
  batch_size: 1000
  rate_limit_delay: 0.1  # seconds between batches
  max_retries: 3
  timeout: 30  # seconds
  tables: null  # null means all tables, or specify list: ["table1", "table2"]
  
validation:
  strict_mode: true
  data_type_validation: true
  foreign_key_validation: false
  
logging:
  level: "INFO"  # DEBUG, INFO, WARNING, ERROR
  file: "database_swap.log"
  console: true
```

### Database-Specific Configuration

#### SQLite
- **database**: Path to SQLite database file

#### MySQL
- **host**: MySQL server hostname
- **port**: MySQL server port (default: 3306)
- **database**: Database name
- **username**: MySQL username
- **password**: MySQL password

#### MongoDB
- **host**: MongoDB server hostname
- **port**: MongoDB server port (default: 27017)
- **database**: Database name
- **username**: MongoDB username (optional)
- **password**: MongoDB password (optional)

## CLI Commands

### migrate
Migrate data from source to target database.

```bash
database-swap migrate [OPTIONS]
```

**Options:**
- `--source-type`: Source database type (sqlite, mysql, mongodb)
- `--source-host`: Source database host
- `--source-database`: Source database name
- `--target-type`: Target database type
- `--target-host`: Target database host
- `--target-database`: Target database name
- `--tables`: Comma-separated list of tables to migrate
- `--batch-size`: Number of records per batch
- `--rate-limit-delay`: Delay between batches in seconds
- `--dry-run`: Perform a dry run without actual migration

### test-connection
Test database connection.

```bash
database-swap test-connection --db-type sqlite --database test.db
```

### analyze
Analyze database structure and data.

```bash
database-swap analyze --db-type mysql --host localhost --database mydb --username user
```

### init-config
Initialize a new configuration file.

```bash
database-swap init-config --output my-config.yaml
```

## Usage Examples

### SQLite to MySQL Migration

```bash
database-swap migrate \
  --source-type sqlite \
  --source-database app.db \
  --target-type mysql \
  --target-host localhost \
  --target-database migrated_app \
  --target-username root \
  --target-password \
  --batch-size 500 \
  --rate-limit-delay 0.2
```

### MySQL to MongoDB Migration

```bash
database-swap migrate \
  --source-type mysql \
  --source-host prod-mysql.example.com \
  --source-database ecommerce \
  --source-username readonly_user \
  --source-password \
  --target-type mongodb \
  --target-host mongo-cluster.example.com \
  --target-database ecommerce_nosql \
  --tables "users,products,orders"
```

### Configuration File Migration

```yaml
# migration-config.yaml
source:
  type: "mysql"
  connection:
    host: "old-server.example.com"
    database: "legacy_db"
    username: "migration_user"
    password: "secure_password"

target:
  type: "mongodb"
  connection:
    host: "new-cluster.example.com"
    port: 27017
    database: "modern_db"

migration:
  batch_size: 2000
  rate_limit_delay: 0.05
  tables: ["users", "products", "orders", "analytics"]
```

```bash
database-swap migrate --config migration-config.yaml
```

## Advanced Features

### Rate Limiting

Database Swap includes adaptive rate limiting to prevent overwhelming target databases:

- **Fixed Rate Limiting**: Configurable delay between operations
- **Adaptive Rate Limiting**: Automatically adjusts based on error rates
- **Batch Rate Limiting**: Controls operations per time window

### Data Validation

Comprehensive validation ensures data integrity:

- **Type Validation**: Checks data types compatibility
- **Schema Validation**: Validates schema compatibility between databases
- **SQL Injection Protection**: Detects potentially dangerous patterns
- **Null Value Validation**: Ensures required fields are not null

### Error Handling

Robust error handling with retry mechanisms:

- **Automatic Retries**: Configurable retry attempts for failed operations
- **Exponential Backoff**: Increases delay between retries
- **Error Logging**: Detailed error reporting and logging
- **Partial Recovery**: Continues migration even if some records fail

### Performance Optimization

- **Batch Processing**: Processes data in configurable batch sizes
- **Connection Pooling**: Efficient database connection management
- **Memory Management**: Handles large datasets without memory issues
- **Progress Tracking**: Real-time progress updates

## Extending Database Support

Database Swap is designed to be easily extensible. To add support for a new database:

1. Create a new adapter in `database_swap/adapters/`
2. Inherit from `DatabaseAdapter` base class
3. Implement required methods
4. Update the adapter factory in `__init__.py`

Example:

```python
from .base import DatabaseAdapter

class PostgreSQLAdapter(DatabaseAdapter):
    def connect(self):
        # Implementation
        pass
    
    def read_data(self, table_name, batch_size, offset):
        # Implementation
        pass
    
    # ... other required methods
```

## Troubleshooting

### Common Issues

1. **Connection Failures**
   - Verify database credentials
   - Check network connectivity
   - Ensure database server is running

2. **Permission Errors**
   - Verify user has read access to source database
   - Verify user has write access to target database
   - Check table-level permissions

3. **Data Type Conflicts**
   - Review validation errors in logs
   - Consider using less strict validation
   - Manual data type conversion may be needed

4. **Performance Issues**
   - Reduce batch size
   - Increase rate limit delay
   - Check database server resources

### Logging

Enable debug logging for detailed troubleshooting:

```bash
database-swap migrate --verbose
```

Or in configuration:

```yaml
logging:
  level: "DEBUG"
  file: "debug.log"
  console: true
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

- GitHub Issues: Report bugs and feature requests
- Documentation: See docs/ directory for detailed documentation
- Examples: See examples/ directory for usage examples