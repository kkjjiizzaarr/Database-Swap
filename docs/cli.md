# CLI reference

The `database-swap` CLI exposes four main commands defined in `database_swap/cli/interface.py`.

- init-config: Create a starter configuration file
- test-connection: Check that you can connect to a database
- analyze: Inspect tables/collections and record counts
- migrate: Migrate data between source and target

All commands accept a global `--config` (-c) to load settings from YAML and `--verbose` (-v) for DEBUG logging.

## init-config

Create a `config.yaml` in the current directory (or a custom output path).

```powershell
database-swap init-config -o config.yaml
```

## test-connection

Test connectivity to one database.

```powershell
# SQLite
database-swap test-connection --db-type sqlite --database .\source.db

# MySQL
database-swap test-connection --db-type mysql --host localhost --database mydb --username root --password

# MongoDB
database-swap test-connection --db-type mongodb --host localhost --database mydb
```

Options

- --db-type: sqlite | mysql | mongodb (required)
- --host, --port
- --database (required)
- --username, --password

## analyze

Summarize tables/collections, schemas, and counts.

```powershell
database-swap analyze --db-type sqlite --database .\source.db
```

Options

- --db-type: sqlite | mysql | mongodb (required)
- --host, --port
- --database (required)
- --username, --password
- --table: analyze one table/collection only

## migrate

Perform the migration. You can pass connection details via flags or a YAML config.

```powershell
# Dry run (no writes)
database-swap migrate --dry-run `
  --source-type sqlite --source-database .\source.db `
  --target-type mysql --target-host localhost --target-database target_db --target-username root

# Actual run
database-swap migrate `
  --source-type sqlite --source-database .\source.db `
  --target-type mysql --target-host localhost --target-database target_db --target-username root
```

Options

- Source: --source-type, --source-host, --source-port, --source-database, --source-username, --source-password
- Target: --target-type, --target-host, --target-port, --target-database, --target-username, --target-password
- Scope: --tables "a,b,c" (optional)
- Performance: --batch-size, --rate-limit-delay, --max-retries
- Safety: --dry-run

## Using a config file

All flags can be provided through YAML and overridden on the CLI.

```powershell
database-swap migrate --config .\config.yaml
```

Global flags can precede any subcommand:

```powershell
database-swap -c .\config.yaml -v migrate
```

See docs/configuration.md for the YAML schema.
