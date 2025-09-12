# Database Swap Examples

This directory contains example configurations and usage scenarios for Database Swap.

## Example 1: SQLite to SQLite Migration

```bash
# Create source database
database-swap init-config --output sqlite-to-sqlite.yaml

# Edit configuration to set source and target
# Then run migration
database-swap migrate --config sqlite-to-sqlite.yaml
```

## Example 2: MySQL to MongoDB Migration

```yaml
# mysql-to-mongo.yaml
source:
  type: "mysql"
  connection:
    host: "mysql-server.example.com"
    port: 3306
    database: "ecommerce"
    username: "readonly_user"
    password: "secure_password"

target:
  type: "mongodb"
  connection:
    host: "mongo-cluster.example.com"
    port: 27017
    database: "ecommerce_nosql"

migration:
  batch_size: 1000
  rate_limit_delay: 0.1
  tables: ["users", "products", "orders"]
  
logging:
  level: "INFO"
  file: "migration.log"
```

```bash
database-swap migrate --config mysql-to-mongo.yaml
```

## Example 3: Selective Table Migration

```bash
# Migrate only specific tables
database-swap migrate \
  --source-type sqlite \
  --source-database app.db \
  --target-type mysql \
  --target-host localhost \
  --target-database migrated_app \
  --target-username root \
  --tables "users,orders,products"
```

## Example 4: Performance Tuning

```bash
# High-performance migration with larger batches
database-swap migrate \
  --source-type mysql \
  --source-host source-db \
  --source-database large_db \
  --target-type mongodb \
  --target-host target-mongo \
  --target-database large_nosql \
  --batch-size 5000 \
  --rate-limit-delay 0.05
```

## Example 5: Testing and Validation

```bash
# Always test connection first
database-swap test-connection --db-type mysql --host localhost --database mydb --username user

# Analyze database structure
database-swap analyze --db-type sqlite --database app.db

# Perform dry run before actual migration
database-swap migrate --dry-run --config migration-config.yaml

# Run actual migration
database-swap migrate --config migration-config.yaml
```