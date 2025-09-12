# Getting started

This guide helps you install Database Swap and run your first migration.

## Prerequisites

- Python 3.8+
- For MySQL: mysql-connector-python (installed via requirements)
- For MongoDB: pymongo (installed via requirements)

## Install

From source:

```powershell
pip install -r requirements.txt
pip install -e .
```

Optional: use a virtual environment.

## Create a config

```powershell
database-swap init-config -o config.yaml
```

Open `config.yaml` and set your source and target. Example for SQLite -> SQLite:

```yaml
source:
  type: sqlite
  connection:
    database: ./source.db

target:
  type: sqlite
  connection:
    database: ./target.db

migration:
  batch_size: 1000
  rate_limit_delay: 0.1
```

## Test connections

SQLite typically works if the file path is valid; for other engines:

```powershell
# MySQL
database-swap test-connection --db-type mysql --host localhost --database mydb --username root --password

# MongoDB
database-swap test-connection --db-type mongodb --host localhost --database mydb
```

## Analyze a database

```powershell
database-swap analyze --db-type sqlite --database .\source.db
```

## Dry run (recommended)

```powershell
database-swap migrate --dry-run `
  --source-type sqlite --source-database .\source.db `
  --target-type sqlite --target-database .\target.db
```

## Run the migration

```powershell
database-swap migrate `
  --source-type sqlite --source-database .\source.db `
  --target-type sqlite --target-database .\target.db
```

That’s it—you’ve moved data between databases with validation and rate limiting.

---

Next: Read the CLI guide (docs/cli.md) or the configuration reference (docs/configuration.md).