# BLS Data Repository Logging System

This directory contains a comprehensive logging system for the BLS Data Repository with proper timestamps, function-level error tracking, and structured output.

## Features

### 🕒 **Timestamps**
- All log messages include precise timestamps
- Format: `YYYY-MM-DD HH:MM:SS`
- Consistent across all log levels

### 🔍 **Function-Level Error Tracking**
- Automatic function name detection in log messages
- Line number information for debugging
- Detailed stack traces for errors
- Context-aware error reporting

### 🎨 **Colored Console Output**
- Color-coded log levels for easy reading
- Debug: Cyan
- Info: Green  
- Warning: Yellow
- Error: Red
- Critical: Magenta

### 📁 **File Rotation**
- Automatic log file rotation (10MB default)
- Configurable backup count (5 files default)
- Separate log files per module
- UTF-8 encoding support

### 📊 **Structured Logging**
- JSON formatter for machine-readable logs
- Performance logging with execution times
- Custom formatters for different use cases

## Quick Start

### Basic Usage

```python
from bls_logging.config import setup_logging, get_logger

# Setup logging (call once at application start)
setup_logging(log_level="INFO", log_dir="logs")

# Get a logger for your module
log = get_logger(__name__)

# Use the logger
log.info("Application started")
log.error("Something went wrong", exc_info=True)
```

### Advanced Usage

```python
from bls_logging.config import log_function_call, log_performance

# Automatically log function calls
@log_function_call
def my_function(param1, param2):
    return result

# Automatically log execution time
@log_performance  
def slow_operation():
    # Your code here
    return result
```

## Configuration Options

### Setup Parameters

```python
setup_logging(
    log_level="INFO",           # DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_dir="logs",             # Directory for log files
    max_file_size=10*1024*1024, # 10MB file rotation
    backup_count=5,             # Number of backup files
    console_output=True,        # Output to console
    file_output=True           # Output to files
)
```

### Log Levels

- **DEBUG**: Detailed information for debugging
- **INFO**: General information about program execution
- **WARNING**: Something unexpected happened
- **ERROR**: A serious problem occurred
- **CRITICAL**: A very serious error occurred

## File Structure

```
bls_data/bls_logging/
├── __init__.py          # Package initialization
├── config.py            # Logging configuration and setup
├── formatters.py        # Custom log formatters
└── README.md           # This file
```

## Log Files

Log files are created in the specified directory with the following naming convention:

- `bls_data.log` - Main application log
- `{module_name}.log` - Module-specific logs
- `{module_name}.log.1`, `{module_name}.log.2`, etc. - Rotated backups

## Example Output

### Console Output
```
2024-01-15 14:30:25 [INFO] main: Application started
2024-01-15 14:30:26 [INFO] bls_client: Fetching data for series CUUR0000SA0
2024-01-15 14:30:27 [ERROR] data_parser: Failed to parse data in parse_results_to_df: Invalid JSON format
Traceback (most recent call last):
  File "data_parser.py", line 45, in parse_results_to_df
    data = json.loads(response)
  File "json/__init__.py", line 357, in loads
    return _default_decoder.decode(s)
json.JSONDecodeError: Invalid JSON format
```

### File Output
```
2024-01-15 14:30:25 [INFO] main: Application started
2024-01-15 14:30:26 [INFO] bls_client: Fetching data for series CUUR0000SA0
2024-01-15 14:30:27 [ERROR] data_parser: Failed to parse data in parse_results_to_df: Invalid JSON format
```

## Integration with Examples

The logging system is integrated into all example scripts:

- `database/example.py` - Database operations logging
- `data_extraction/example.py` - Data extraction logging
- `test_logging.py` - Logging system demonstration

## Best Practices

### 1. **Use Appropriate Log Levels**
```python
log.debug("Variable value: %s", variable)  # Detailed debugging
log.info("Processing %d records", count)   # General information
log.warning("API rate limit approaching")  # Potential issues
log.error("Database connection failed")    # Errors
log.critical("System out of memory")       # Critical failures
```

### 2. **Include Context in Messages**
```python
# Good
log.info("Fetching data for series %s from %d to %d", series_id, start_year, end_year)

# Avoid
log.info("Fetching data")
```

### 3. **Use Exception Information**
```python
try:
    risky_operation()
except Exception as e:
    log.error("Operation failed: %s", e, exc_info=True)
```

### 4. **Performance Logging**
```python
@log_performance
def expensive_operation():
    # Your code here
    return result
```

## Troubleshooting

### Common Issues

1. **Log files not created**
   - Check directory permissions
   - Ensure `log_dir` path exists or is writable

2. **No console output**
   - Verify `console_output=True` in setup
   - Check if logging level is appropriate

3. **Missing function names**
   - Ensure you're using `get_logger(__name__)`
   - Check if the formatter is properly configured

### Debug Mode

Enable debug logging to see detailed information:

```python
setup_logging(log_level="DEBUG")
```

## Migration from Print Statements

The logging system replaces all `print()` statements with proper logging:

```python
# Old way
print("Processing data...")
print(f"Found {count} records")

# New way  
log.info("Processing data...")
log.info("Found %d records", count)
```

## Performance Considerations

- Logging has minimal performance impact
- File rotation prevents disk space issues
- Colored output only affects console (not files)
- Structured logging adds slight overhead

## Security Notes

- Log files may contain sensitive data
- Ensure proper file permissions
- Consider log file encryption for production
- Rotate logs regularly to prevent accumulation
