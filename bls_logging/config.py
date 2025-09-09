#!/usr/bin/env python3
"""
Logging configuration for BLS Data Repository.

This module provides centralized logging configuration with proper formatting,
file rotation, and function-level error tracking.
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from .formatters import BLSFormatter


class BLSLoggingConfig:
    """Configuration class for BLS logging system."""

    def __init__(
        self,
        log_level: str = "INFO",
        log_dir: Optional[str] = None,
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        console_output: bool = True,
        file_output: bool = True,
    ):
        """
        Initialize logging configuration.

        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directory for log files (defaults to ./logs)
            max_file_size: Maximum size of log files before rotation
            backup_count: Number of backup files to keep
            console_output: Whether to output logs to console
            file_output: Whether to output logs to files
        """
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.log_dir = Path(log_dir) if log_dir else Path("logs")
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.console_output = console_output
        self.file_output = file_output

        # Create log directory if it doesn't exist
        if self.file_output:
            self.log_dir.mkdir(parents=True, exist_ok=True)

    def setup_logger(
        self, name: str, log_file: Optional[str] = None, propagate: bool = False
    ) -> logging.Logger:
        """
        Setup a logger with the specified configuration.

        Args:
            name: Logger name (typically __name__)
            log_file: Specific log file name (defaults to {name}.log)
            propagate: Whether to propagate messages to parent loggers

        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(self.log_level)
        logger.propagate = propagate

        # Clear existing handlers
        logger.handlers.clear()

        # Create formatter
        formatter = BLSFormatter()

        # Console handler
        if self.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(self.log_level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # File handler
        if self.file_output:
            if log_file is None:
                log_file = f"{name.replace('.', '_')}.log"

            log_path = self.log_dir / log_file

            # Use rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=self.max_file_size,
                backupCount=self.backup_count,
                encoding="utf-8",
            )
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger


# Global logging configuration instance
_logging_config: Optional[BLSLoggingConfig] = None


def setup_logging(
    log_level: str = "INFO",
    log_dir: Optional[str] = None,
    max_file_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    console_output: bool = True,
    file_output: bool = True,
    **kwargs,
) -> BLSLoggingConfig:
    """
    Setup global logging configuration.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_dir: Directory for log files
        max_file_size: Maximum size of log files before rotation
        backup_count: Number of backup files to keep
        console_output: Whether to output logs to console
        file_output: Whether to output logs to files
        **kwargs: Additional configuration options

    Returns:
        Configured BLSLoggingConfig instance
    """
    global _logging_config

    _logging_config = BLSLoggingConfig(
        log_level=log_level,
        log_dir=log_dir,
        max_file_size=max_file_size,
        backup_count=backup_count,
        console_output=console_output,
        file_output=file_output,
        **kwargs,
    )

    # Setup root logger
    root_logger = _logging_config.setup_logger("bls_data", "bls_data.log")

    # Configure third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)

    return _logging_config


def get_logger(
    name: str, log_file: Optional[str] = None, propagate: bool = False
) -> logging.Logger:
    """
    Get a logger instance with the global configuration.

    Args:
        name: Logger name (typically __name__)
        log_file: Specific log file name
        propagate: Whether to propagate messages to parent loggers

    Returns:
        Configured logger instance
    """
    global _logging_config

    if _logging_config is None:
        # Setup default configuration if not already configured
        _logging_config = setup_logging()

    return _logging_config.setup_logger(name, log_file, propagate)


def get_logging_config() -> Optional[BLSLoggingConfig]:
    """Get the current logging configuration."""
    return _logging_config


def log_function_call(func):
    """
    Decorator to automatically log function calls with parameters and results.

    Usage:
        @log_function_call
        def my_function(param1, param2):
            return result
    """

    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)

        # Log function entry
        logger.debug(f"Calling {func.__name__} with args={args}, kwargs={kwargs}")

        try:
            result = func(*args, **kwargs)
            logger.debug(f"{func.__name__} completed successfully")
            return result
        except Exception as e:
            logger.error(f"{func.__name__} failed with error: {e}", exc_info=True)
            raise

    return wrapper


def log_performance(func):
    """
    Decorator to log function execution time.

    Usage:
        @log_performance
        def my_function():
            # function body
    """

    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        start_time = datetime.now()

        try:
            result = func(*args, **kwargs)
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"{func.__name__} executed in {execution_time:.3f} seconds")
            return result
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(
                f"{func.__name__} failed after {execution_time:.3f} seconds: {e}",
                exc_info=True,
            )
            raise

    return wrapper
