#!/usr/bin/env python3
"""
Test script to demonstrate the new logging system.

This script shows the logging functionality with timestamps, function names,
and proper error tracking.
"""

import sys
from pathlib import Path

# Add the parent directory to the Python path
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from bls_logging.config import (
    get_logger,
    log_function_call,
    log_performance,
    setup_logging,
)

# Setup logging
setup_logging(log_level="DEBUG", log_dir="logs", console_output=True, file_output=True)
log = get_logger(__name__)


@log_function_call
def test_function_with_parameters(param1: str, param2: int = 42):
    """Test function that will be logged automatically."""
    log.info(f"Processing {param1} with value {param2}")
    return f"Result: {param1}_{param2}"


@log_performance
def test_performance_function():
    """Test function that will log execution time."""
    import time

    time.sleep(0.1)  # Simulate some work
    log.info("Performance test completed")
    return "success"


def test_error_logging():
    """Test function that demonstrates error logging with function context."""
    try:
        log.info("About to cause an error for demonstration")
        # Intentionally cause an error
        raise ZeroDivisionError("Intentional error for demonstration")  # noqa: B018
    except Exception as e:
        log.error(f"Error occurred in test_error_logging: {e}", exc_info=True)


def test_different_log_levels():
    """Test different log levels."""
    log.debug("This is a debug message")
    log.info("This is an info message")
    log.warning("This is a warning message")
    log.error("This is an error message")
    log.critical("This is a critical message")


def main():
    """Run all logging tests."""
    log.info("=" * 60)
    log.info("LOGGING SYSTEM TEST")
    log.info("=" * 60)

    # Test basic logging
    log.info("Testing basic logging functionality...")

    # Test function call logging
    log.info("Testing function call logging...")
    result = test_function_with_parameters("test_param", 123)
    log.info(f"Function returned: {result}")

    # Test performance logging
    log.info("Testing performance logging...")
    test_performance_function()

    # Test different log levels
    log.info("Testing different log levels...")
    test_different_log_levels()

    # Test error logging
    log.info("Testing error logging...")
    test_error_logging()

    log.info("=" * 60)
    log.info("LOGGING TEST COMPLETED")
    log.info("=" * 60)
    log.info(
        "Check the 'logs' directory for log files with timestamps and function names"
    )


if __name__ == "__main__":
    main()
