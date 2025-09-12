"""Logging utilities for database swap operations."""

import logging
import sys
from typing import Optional, Dict, Any
import colorama
from colorama import Fore, Style


class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to log messages."""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA
    }
    
    def format(self, record):
        log_color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{log_color}{record.levelname}{Style.RESET_ALL}"
        return super().format(record)


def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """Set up logging based on configuration."""
    # Initialize colorama for cross-platform colored output
    colorama.init()
    
    level = config.get('level', 'INFO').upper()
    log_file = config.get('file', 'database_swap.log')
    console_enabled = config.get('console', True)
    
    # Create logger
    logger = logging.getLogger('database_swap')
    logger.setLevel(getattr(logging, level))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = ColoredFormatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Add file handler if log file is specified
    if log_file:
        try:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(getattr(logging, level))
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            print(f"Warning: Could not create log file {log_file}: {e}")
    
    # Add console handler if enabled
    if console_enabled:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level))
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(f'database_swap.{name}')


class ProgressLogger:
    """Logger for tracking migration progress."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.total_records = 0
        self.processed_records = 0
        self.failed_records = 0
        self.current_table = ""
    
    def start_table(self, table_name: str, total_records: int):
        """Start processing a new table."""
        self.current_table = table_name
        self.total_records = total_records
        self.processed_records = 0
        self.failed_records = 0
        self.logger.info(f"Starting migration of table '{table_name}' ({total_records} records)")
    
    def update_progress(self, processed: int, failed: int = 0):
        """Update progress for current table."""
        self.processed_records = processed
        self.failed_records += failed
        
        percentage = (processed / self.total_records * 100) if self.total_records > 0 else 0
        
        self.logger.info(
            f"Table '{self.current_table}': {processed}/{self.total_records} "
            f"({percentage:.1f}%) - Failed: {self.failed_records}"
        )
    
    def finish_table(self, success: bool):
        """Finish processing current table."""
        if success:
            self.logger.info(
                f"Completed migration of table '{self.current_table}': "
                f"{self.processed_records} records processed, {self.failed_records} failed"
            )
        else:
            self.logger.error(f"Failed to migrate table '{self.current_table}'")
    
    def log_error(self, message: str, exception: Optional[Exception] = None):
        """Log an error message."""
        if exception:
            self.logger.error(f"{message}: {exception}")
        else:
            self.logger.error(message)