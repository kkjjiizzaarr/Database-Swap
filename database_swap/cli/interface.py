"""Command-line interface for database swap operations."""

import click
import sys
import os
from typing import Optional, Dict, Any
from ..config import Config, load_config
from ..core import create_migrator, MigrationError
from ..utils import setup_logging, get_logger, validate_connection_config
from ..adapters import get_adapter


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), 
              help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, 
              help='Enable verbose logging')
@click.pass_context
def cli(ctx, config: Optional[str], verbose: bool):
    """Database Swap - Migrate data between different database systems."""
    # Ensure context object is dict
    ctx.ensure_object(dict)
    
    # Load configuration
    try:
        ctx.obj['config'] = load_config(config)
        
        # Set log level based on verbose flag
        if verbose:
            ctx.obj['config'].set('logging.level', 'DEBUG')
        
        # Setup logging
        logging_config = ctx.obj['config'].get_logging_config()
        ctx.obj['logger'] = setup_logging(logging_config)
        
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--source-type', type=click.Choice(['sqlite', 'mysql', 'mongodb']),
              help='Source database type')
@click.option('--source-host', help='Source database host')
@click.option('--source-port', type=int, help='Source database port')
@click.option('--source-database', help='Source database name')
@click.option('--source-username', help='Source database username')
@click.option('--source-password', help='Source database password', hide_input=True)
@click.option('--target-type', type=click.Choice(['sqlite', 'mysql', 'mongodb']),
              help='Target database type')
@click.option('--target-host', help='Target database host')
@click.option('--target-port', type=int, help='Target database port')
@click.option('--target-database', help='Target database name')
@click.option('--target-username', help='Target database username')
@click.option('--target-password', help='Target database password', hide_input=True)
@click.option('--tables', help='Comma-separated list of tables to migrate')
@click.option('--batch-size', type=int, help='Number of records per batch')
@click.option('--rate-limit-delay', type=float, help='Delay between batches in seconds')
@click.option('--max-retries', type=int, help='Maximum number of retries for failed operations')
@click.option('--dry-run', is_flag=True, help='Perform a dry run without actual migration')
@click.pass_context
def migrate(ctx, **kwargs):
    """Migrate data from source to target database."""
    config = ctx.obj['config']
    logger = ctx.obj['logger']
    
    try:
        # Update configuration with command line arguments
        args = {k: v for k, v in kwargs.items() if v is not None}
        
        # Handle tables list
        if args.get('tables'):
            args['tables'] = [t.strip() for t in args['tables'].split(',')]
        
        config.update_from_args(args)
        
        # Validate configuration
        _validate_migration_config(config)
        
        if kwargs.get('dry_run'):
            logger.info("Performing dry run - no data will be migrated")
            _perform_dry_run(config)
        else:
            # Perform actual migration
            migrator = create_migrator(config)
            stats = migrator.migrate()
            
            # Display results
            _display_migration_results(stats)
    
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--db-type', type=click.Choice(['sqlite', 'mysql', 'mongodb']),
              required=True, help='Database type to test')
@click.option('--host', help='Database host')
@click.option('--port', type=int, help='Database port')
@click.option('--database', required=True, help='Database name')
@click.option('--username', help='Database username')
@click.option('--password', help='Database password', hide_input=True)
@click.pass_context
def test_connection(ctx, db_type, host, port, database, username, password):
    """Test database connection."""
    logger = ctx.obj['logger']
    
    try:
        # Build connection config
        connection_config = {
            'host': host or 'localhost',
            'database': database,
            'username': username,
            'password': password
        }
        
        if port:
            connection_config['port'] = port
        
        # Validate configuration
        errors = validate_connection_config({'connection': connection_config}, db_type)
        if errors:
            for error in errors:
                logger.error(error)
            sys.exit(1)
        
        # Test connection
        adapter = get_adapter(db_type, connection_config)
        
        click.echo(f"Testing connection to {db_type} database...")
        
        if adapter.connect():
            if adapter.test_connection():
                click.echo(click.style("✓ Connection successful!", fg='green'))
                
                # Show additional info
                tables = adapter.get_tables()
                click.echo(f"Found {len(tables)} tables/collections")
                if tables:
                    click.echo("Tables/Collections:")
                    for table in tables[:10]:  # Show first 10
                        count = adapter.get_table_count(table)
                        click.echo(f"  - {table} ({count} records)")
                    if len(tables) > 10:
                        click.echo(f"  ... and {len(tables) - 10} more")
            else:
                click.echo(click.style("✗ Connection test failed", fg='red'))
                sys.exit(1)
        else:
            click.echo(click.style("✗ Failed to connect", fg='red'))
            sys.exit(1)
        
        adapter.disconnect()
    
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', type=click.Path(), 
              help='Output file for configuration')
@click.pass_context
def init_config(ctx, output):
    """Initialize a new configuration file."""
    try:
        config = Config()
        
        if not output:
            output = 'config.yaml'
        
        config.save_config(output)
        click.echo(f"Configuration file created: {output}")
        click.echo("Edit the file to customize your database settings.")
    
    except Exception as e:
        click.echo(f"Failed to create configuration: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--db-type', type=click.Choice(['sqlite', 'mysql', 'mongodb']),
              required=True, help='Database type')
@click.option('--host', help='Database host')
@click.option('--port', type=int, help='Database port')
@click.option('--database', required=True, help='Database name')
@click.option('--username', help='Database username')
@click.option('--password', help='Database password', hide_input=True)
@click.option('--table', help='Specific table to analyze')
@click.pass_context
def analyze(ctx, db_type, host, port, database, username, password, table):
    """Analyze database structure and data."""
    logger = ctx.obj['logger']
    
    try:
        # Build connection config
        connection_config = {
            'host': host or 'localhost',
            'database': database,
            'username': username,
            'password': password
        }
        
        if port:
            connection_config['port'] = port
        
        # Connect to database
        adapter = get_adapter(db_type, connection_config)
        
        if not adapter.connect():
            click.echo("Failed to connect to database", err=True)
            sys.exit(1)
        
        click.echo(f"Analyzing {db_type} database: {database}")
        click.echo("=" * 50)
        
        # Get tables
        tables = adapter.get_tables()
        
        if table:
            if table in tables:
                tables = [table]
            else:
                click.echo(f"Table '{table}' not found", err=True)
                sys.exit(1)
        
        total_records = 0
        
        for table_name in tables:
            count = adapter.get_table_count(table_name)
            schema = adapter.get_table_schema(table_name)
            
            click.echo(f"\nTable: {table_name}")
            click.echo(f"Records: {count:,}")
            
            total_records += count
            
            if schema.get('columns'):
                click.echo("Columns:")
                for col_name, col_info in schema['columns'].items():
                    col_type = col_info.get('type', 'unknown')
                    nullable = "NULL" if col_info.get('nullable', True) else "NOT NULL"
                    click.echo(f"  - {col_name}: {col_type} {nullable}")
        
        click.echo(f"\nTotal tables: {len(tables)}")
        click.echo(f"Total records: {total_records:,}")
        
        adapter.disconnect()
    
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        sys.exit(1)


def _validate_migration_config(config: Config) -> None:
    """Validate migration configuration."""
    # Validate source configuration
    source_config = config.get_source_config()
    errors = validate_connection_config(source_config, source_config['type'])
    if errors:
        raise ValueError(f"Source database configuration errors: {', '.join(errors)}")
    
    # Validate target configuration
    target_config = config.get_target_config()
    errors = validate_connection_config(target_config, target_config['type'])
    if errors:
        raise ValueError(f"Target database configuration errors: {', '.join(errors)}")
    
    # Validate migration settings
    migration_config = config.get_migration_config()
    batch_size = migration_config.get('batch_size', 1000)
    if batch_size <= 0:
        raise ValueError("Batch size must be greater than 0")


def _perform_dry_run(config: Config) -> None:
    """Perform a dry run to validate configuration and connectivity."""
    logger = get_logger('dry_run')
    
    source_config = config.get_source_config()
    target_config = config.get_target_config()
    
    # Test source connection
    logger.info("Testing source database connection...")
    source_adapter = get_adapter(source_config['type'], source_config['connection'])
    
    if not source_adapter.connect():
        raise Exception("Failed to connect to source database")
    
    if not source_adapter.test_connection():
        raise Exception("Source database connection test failed")
    
    # Get source tables
    tables = source_adapter.get_tables()
    logger.info(f"Source database has {len(tables)} tables: {tables}")
    
    # Test target connection
    logger.info("Testing target database connection...")
    target_adapter = get_adapter(target_config['type'], target_config['connection'])
    
    if not target_adapter.connect():
        raise Exception("Failed to connect to target database")
    
    if not target_adapter.test_connection():
        raise Exception("Target database connection test failed")
    
    # Check table compatibility
    migration_config = config.get_migration_config()
    tables_to_migrate = migration_config.get('tables') or tables
    
    total_records = 0
    for table in tables_to_migrate:
        if table in tables:
            count = source_adapter.get_table_count(table)
            total_records += count
            logger.info(f"Table '{table}': {count:,} records")
        else:
            logger.warning(f"Table '{table}' not found in source database")
    
    logger.info(f"Total records to migrate: {total_records:,}")
    
    # Estimate migration time
    from ..utils import estimate_migration_time, format_duration
    batch_size = migration_config.get('batch_size', 1000)
    rate_limit_delay = migration_config.get('rate_limit_delay', 0.1)
    
    estimated_time = estimate_migration_time(total_records, batch_size, rate_limit_delay)
    logger.info(f"Estimated migration time: {format_duration(estimated_time)}")
    
    # Clean up
    source_adapter.disconnect()
    target_adapter.disconnect()
    
    logger.info("Dry run completed successfully!")


def _display_migration_results(stats) -> None:
    """Display migration results in a formatted way."""
    from ..utils import format_duration
    
    click.echo("\n" + "=" * 60)
    click.echo("MIGRATION RESULTS")
    click.echo("=" * 60)
    
    stats_dict = stats.to_dict()
    
    # Success indicator
    if stats_dict['success_rate'] >= 100:
        click.echo(click.style("✓ Migration completed successfully!", fg='green'))
    elif stats_dict['success_rate'] >= 90:
        click.echo(click.style("⚠ Migration completed with warnings", fg='yellow'))
    else:
        click.echo(click.style("✗ Migration completed with errors", fg='red'))
    
    # Statistics
    click.echo(f"\nDuration: {format_duration(stats_dict['duration'])}")
    click.echo(f"Tables processed: {stats_dict['tables_processed']}")
    click.echo(f"Tables failed: {stats_dict['tables_failed']}")
    click.echo(f"Total records: {stats_dict['total_records']:,}")
    click.echo(f"Records migrated: {stats_dict['records_migrated']:,}")
    click.echo(f"Records failed: {stats_dict['records_failed']:,}")
    click.echo(f"Success rate: {stats_dict['success_rate']:.1f}%")
    
    # Errors
    if stats_dict['errors']:
        click.echo(f"\nErrors ({len(stats_dict['errors'])}):")
        for error in stats_dict['errors'][:5]:
            click.echo(f"  - {error}")
        if len(stats_dict['errors']) > 5:
            click.echo(f"  ... and {len(stats_dict['errors']) - 5} more")
    
    click.echo("=" * 60)


if __name__ == '__main__':
    cli()