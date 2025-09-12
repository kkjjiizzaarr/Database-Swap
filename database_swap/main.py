"""Main entry point for the database swap application."""

import sys
from .cli import cli


def main():
    """Main entry point for the database-swap command."""
    try:
        cli()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()