# Learn Database Swap

A guided path to learn and use Database Swap effectively. This page links to focused docs so you can ramp up quickly and deepen as needed.

- What it is: A Python tool to migrate data between SQLite, MySQL, and MongoDB with validation, rate limiting, and progress tracking.
- Who it’s for: Developers and data engineers moving datasets across database engines reliably.

## Start here

1) Getting started (install + first migration)
- Read: docs/getting-started.md

2) Understand the configuration
- Read: docs/configuration.md

3) Use the CLI efficiently
- Read: docs/cli.md

4) Know your database adapters
- Read: docs/adapters.md

5) How it works under the hood
- Read: docs/architecture.md

6) Extend to new databases
- Read: docs/extending.md

7) Troubleshoot common issues
- Read: docs/troubleshooting.md

8) Recipes and examples
- Read: docs/recipes.md

9) FAQ
- Read: docs/faq.md

## Quick cheat sheet

Installation (from source)

```powershell
pip install -r requirements.txt
pip install -e .
```

Available CLI commands

```powershell
# Initialize a config file in the current folder
database-swap init-config -o config.yaml

# Test a connection
database-swap test-connection --db-type sqlite --database .\source.db

# Analyze a database (structure + counts)
database-swap analyze --db-type sqlite --database .\source.db

# Migrate from SQLite to MySQL (dry run first)
database-swap migrate --dry-run `
  --source-type sqlite --source-database .\source.db `
  --target-type mysql --target-host localhost --target-database target_db --target-username root

# Run the actual migration
database-swap migrate `
  --source-type sqlite --source-database .\source.db `
  --target-type mysql --target-host localhost --target-database target_db --target-username root
```

Configuration keys you’ll use most

- source.* and target.*: database types and connection info
- migration.batch_size: records per write
- migration.rate_limit_delay: seconds to wait between batches
- migration.tables: subset of tables to migrate
- validation.strict_mode: fail fast on invalid data

See docs/configuration.md for a full reference.

## Examples in repo

- examples/sqlite-to-sqlite.yaml
- examples/mysql-to-mongodb.yaml

These are great starting points for your own config files.

## Contributing

- Open issues and PRs are welcome.
- See docs/extending.md to add new database adapters.
