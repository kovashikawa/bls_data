#!/usr/bin/env python3
"""
Custom log formatters for BLS Data Repository.

This module provides specialized log formatters with timestamps, function names,
and structured error information.
"""

import logging
import traceback
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class BLSFormatter(logging.Formatter):
    """
    Custom formatter for BLS logging with timestamps and function-level error tracking.
    """
    
    # Color codes for different log levels
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def __init__(self, 
                 use_colors: bool = True,
                 include_function: bool = True,
                 include_line_number: bool = True,
                 detailed_errors: bool = True):
        """
        Initialize the BLS formatter.
        
        Args:
            use_colors: Whether to use colored output for console
            include_function: Whether to include function name in log format
            include_line_number: Whether to include line number in log format
            detailed_errors: Whether to include detailed error information
        """
        self.use_colors = use_colors
        self.include_function = include_function
        self.include_line_number = include_line_number
        self.detailed_errors = detailed_errors
        
        # Build format string
        format_parts = [
            '%(asctime)s',
            '[%(levelname)s]',
        ]
        
        if include_function:
            format_parts.append('%(funcName)s:')
        
        if include_line_number:
            format_parts.append('%(lineno)d:')
        
        format_parts.append('%(message)s')
        
        self.base_format = ' '.join(format_parts)
        
        super().__init__(self.base_format, datefmt='%Y-%m-%d %H:%M:%S')
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record with custom formatting.
        
        Args:
            record: Log record to format
            
        Returns:
            Formatted log message
        """
        # Add function context if not already present
        if not hasattr(record, 'funcName') or record.funcName == '<module>':
            record.funcName = self._get_caller_function()
        
        # Format the base message
        formatted = super().format(record)
        
        # Add colors for console output
        if self.use_colors and hasattr(record, 'levelname'):
            color = self.COLORS.get(record.levelname, '')
            reset = self.COLORS['RESET']
            formatted = f"{color}{formatted}{reset}"
        
        # Add detailed error information for errors and critical messages
        if (record.levelno >= logging.ERROR and 
            record.exc_info and 
            self.detailed_errors):
            formatted += self._format_exception_details(record)
        
        return formatted
    
    def _get_caller_function(self) -> str:
        """
        Get the name of the function that called the logger.
        
        Returns:
            Function name or 'unknown'
        """
        try:
            # Get the current frame and walk up the stack
            frame = sys._getframe()
            
            # Walk up the stack to find the actual caller
            for _ in range(5):  # Limit search depth
                frame = frame.f_back
                if frame is None:
                    break
                
                # Skip logging-related frames
                if (frame.f_code.co_filename.endswith('logging/') or
                    'logging' in frame.f_code.co_filename):
                    continue
                
                # Return the function name
                return frame.f_code.co_name
            
            return 'unknown'
        except Exception:
            return 'unknown'
    
    def _format_exception_details(self, record: logging.LogRecord) -> str:
        """
        Format detailed exception information.
        
        Args:
            record: Log record with exception info
            
        Returns:
            Formatted exception details
        """
        if not record.exc_info:
            return ""
        
        try:
            exc_type, exc_value, exc_traceback = record.exc_info
            
            # Get the traceback
            tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            
            # Find the relevant part of the traceback (skip logging frames)
            relevant_lines = []
            for line in tb_lines:
                if ('logging/' not in line and 
                    'logging' not in line and
                    'traceback' not in line.lower()):
                    relevant_lines.append(line)
            
            if relevant_lines:
                return "\n" + "".join(relevant_lines)
            else:
                return "\n" + "".join(tb_lines)
                
        except Exception:
            return ""


class StructuredFormatter(logging.Formatter):
    """
    JSON-structured formatter for machine-readable logs.
    """
    
    def __init__(self):
        """Initialize the structured formatter."""
        super().__init__()
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as structured JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON-formatted log message
        """
        import json
        
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'function': getattr(record, 'funcName', 'unknown'),
            'line': getattr(record, 'lineno', 0),
            'message': record.getMessage(),
            'module': record.module,
            'pathname': record.pathname
        }
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in log_entry and not key.startswith('_'):
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str, ensure_ascii=False)


class PerformanceFormatter(logging.Formatter):
    """
    Specialized formatter for performance-related logs.
    """
    
    def __init__(self):
        """Initialize the performance formatter."""
        super().__init__(
            '%(asctime)s [PERF] %(funcName)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format performance log records.
        
        Args:
            record: Log record to format
            
        Returns:
            Formatted performance log message
        """
        # Add performance-specific formatting
        if hasattr(record, 'execution_time'):
            record.msg = f"{record.msg} (execution_time: {record.execution_time:.3f}s)"
        
        if hasattr(record, 'memory_usage'):
            record.msg = f"{record.msg} (memory: {record.memory_usage}MB)"
        
        return super().format(record)
