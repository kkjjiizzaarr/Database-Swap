# Configuration reference

Database Swap loads YAML from `--config` or searches: `config.yaml`, `config.yml`, `%USERPROFILE%\\.database_swap.yaml`, `/etc/database_swap.yaml`.

The default shape (see `database_swap/config/settings.py`):

```yaml
source:
  type: sqlite | mysql | mongodb
  connection:
    host: localhost
    port: null
    database: source.db
    username: null
    password: null

target:
  type: sqlite | mysql | mongodb
  connection:
    host: localhost
    port: null
    database: target.db
    username: null
    password: null

migration:
  batch_size: 1000
  rate_limit_delay: 0.1
  max_retries: 3
  timeout: 30
  tables: null   # null = all tables/collections

validation:
  strict_mode: true
  data_type_validation: true
  foreign_key_validation: false

logging:
  level: INFO
  file: database_swap.log
  console: true
```

Notes

- For SQLite, only `connection.database` is required.
- For MySQL, required: host, database, username; password typically too.
- For MongoDB, required: host, database. Username/password optional.
- `tables`: set a list like ["users", "orders"] to migrate a subset.
- `rate_limit_delay`: seconds to wait between batch writes (adaptive during run).

## Override with CLI

Any key can be overridden via flags used by `Config.update_from_args`:

- Source: --source-type, --source-host, --source-port, --source-database, --source-username, --source-password
- Target: --target-type, --target-host, --target-port, --target-database, --target-username, --target-password
- Migration: --batch-size, --rate-limit-delay, --max-retries, --timeout, --tables
- Logging: implied via `-v` (DEBUG) or by editing YAML

Example

```powershell
database-swap migrate -c .\config.yaml --batch-size 500 --tables "users,orders"
```
