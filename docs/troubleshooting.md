# Troubleshooting

## Connection issues

- Ensure the server is reachable and the service is running
- Verify host/port, database name, and credentials
- For MySQL on localhost, check socket vs TCP settings
- For MongoDB, ensure you can `ping` the server and the database exists

## Missing packages

- MySQL: install `mysql-connector-python`
- MongoDB: install `pymongo`

```powershell
pip install -r requirements.txt
```

## Permission errors

- Source: needs SELECT/read permissions
- Target: needs CREATE/INSERT permissions

## Data type mismatches

- Enable DEBUG logs (`-v`) to see validation warnings
- Consider adjusting `validation.*` in YAML
- Use smaller `batch_size` to identify problematic rows

## Performance tuning

- Increase `batch_size` gradually (e.g., 500 -> 2000)
- Adjust `rate_limit_delay` (start at 0.1s)
- Ensure target DB has adequate indexes and resources

## Logging

- Logs go to `database_swap.log` (by default) and colored console output
- Set `logging.level: DEBUG` in YAML or pass `-v`
