#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Logging configuration module for Telegram bot reporting system.
Provides centralized logging setup with file rotation and appropriate formatting.
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Optional
from pathlib import Path

# Default log directory
DEFAULT_LOG_DIR = "logs"
DEFAULT_LOG_LEVEL = logging.INFO
MAX_LOG_FILE_SIZE = 10 * 1024 * 1024  # 10MB
BACKUP_COUNT = 5


class UTCFormatter(logging.Formatter):
    """
    Custom formatter that uses UTC time for log messages.
    """
    
    def formatTime(self, record, datefmt=None):
        """
        Format time using UTC timezone.
        
        Args:
            record: Log record
            datefmt: Date format string
            
        Returns:
            str: Formatted timestamp
        """
        dt = datetime.utcfromtimestamp(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.strftime('%Y-%m-%d %H:%M:%S UTC')


def setup_logging(
    log_level: int = DEFAULT_LOG_LEVEL,
    log_dir: str = DEFAULT_LOG_DIR,
    app_name: str = "telegram_bot",
    console_output: bool = True,
    file_output: bool = True
) -> logging.Logger:
    """
    Setup centralized logging configuration.
    
    Args:
        log_level: Logging level (default: INFO)
        log_dir: Directory for log files (default: "logs")
        app_name: Application name for log files
        console_output: Enable console output (default: True)
        file_output: Enable file output (default: True)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    if file_output:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # Get root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = UTCFormatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S UTC'
    )
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler with rotation
    if file_output:
        log_file_path = os.path.join(log_dir, f"{app_name}.log")
        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path,
            maxBytes=MAX_LOG_FILE_SIZE,
            backupCount=BACKUP_COUNT,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Set specific loggers levels
    # Reduce aiogram verbosity
    logging.getLogger('aiogram').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    
    # Ensure our app logger exists
    app_logger = logging.getLogger(app_name)
    app_logger.setLevel(log_level)
    
    return app_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)


def log_function_call(func_name: str, args: tuple = (), kwargs: Optional[dict] = None) -> None:
    """
    Log function call with arguments.
    
    Args:
        func_name: Function name
        args: Positional arguments
        kwargs: Keyword arguments
    """
    logger = logging.getLogger('function_calls')
    
    args_str = ', '.join(repr(arg) for arg in args) if args else ''
    kwargs_str = ', '.join(f'{k}={repr(v)}' for k, v in (kwargs if kwargs is not None else {}).items())
    
    all_args = ', '.join(filter(None, [args_str, kwargs_str]))
    logger.debug(f"Calling {func_name}({all_args})")


def log_database_operation(operation: str, table: str, **kwargs) -> None:
    """
    Log database operation.
    
    Args:
        operation: Operation type (INSERT, UPDATE, DELETE, SELECT)
        table: Table name
        **kwargs: Additional operation details
    """
    logger = logging.getLogger('database')
    
    details = ', '.join(f'{k}={v}' for k, v in kwargs.items()) if kwargs else ''
    logger.debug(f"DB {operation} on {table}: {details}")


def log_telegram_event(event_type: str, user_id: int, username: Optional[str] = None, **kwargs) -> None:
    """
    Log Telegram bot event.
    
    Args:
        event_type: Type of event (message, callback, etc.)
        user_id: Telegram user ID
        username: Telegram username (optional)
        **kwargs: Additional event details
    """
    logger = logging.getLogger('telegram_events')
    
    user_info = f"user_id={user_id}"
    if username:
        user_info += f", username={username}"
    
    details = ', '.join(f'{k}={v}' for k, v in kwargs.items()) if kwargs else ''
    log_message = f"Telegram {event_type}: {user_info}"
    if details:
        log_message += f", {details}"
    
    logger.info(log_message)


def log_report_generation(report_type: str, period: str, status: str, **kwargs) -> None:
    """
    Log report generation event.
    
    Args:
        report_type: Type of report (daily, weekly, etc.)
        period: Report period
        status: Generation status (started, completed, failed)
        **kwargs: Additional details
    """
    logger = logging.getLogger('reports')
    
    details = ', '.join(f'{k}={v}' for k, v in kwargs.items()) if kwargs else ''
    log_message = f"Report {status}: {report_type} for {period}"
    if details:
        log_message += f", {details}"
    
    if status == 'failed':
        logger.error(log_message)
    else:
        logger.info(log_message)


def log_scheduler_event(task_name: str, status: str, next_run: Optional[str] = None, **kwargs) -> None:
    """
    Log scheduler event.
    
    Args:
        task_name: Name of scheduled task
        status: Task status (scheduled, running, completed, failed)
        next_run: Next run time (optional)
        **kwargs: Additional details
    """
    logger = logging.getLogger('scheduler')
    
    details = ', '.join(f'{k}={v}' for k, v in kwargs.items()) if kwargs else ''
    log_message = f"Task {task_name} {status}"
    
    if next_run:
        log_message += f", next run: {next_run}"
    
    if details:
        log_message += f", {details}"
    
    if status == 'failed':
        logger.error(log_message)
    else:
        logger.info(log_message)


def setup_development_logging() -> logging.Logger:
    """
    Setup logging configuration for development environment.
    
    Returns:
        logging.Logger: Configured logger
    """
    return setup_logging(
        log_level=logging.DEBUG,
        log_dir="logs",
        app_name="telegram_bot_dev",
        console_output=True,
        file_output=True
    )


def setup_production_logging() -> logging.Logger:
    """
    Setup logging configuration for production environment.
    Falls back to relative directory if system log directory is not accessible.
    
    Returns:
        logging.Logger: Configured logger
    """
    log_dir = "/var/log/telegram_bot"
    
    # Test if we can write to the system log directory
    try:
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        # Test write permissions
        test_file = os.path.join(log_dir, "test_write.tmp")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
    except (PermissionError, OSError):
        # Fall back to relative logs directory
        log_dir = os.getenv('LOG_DIR', DEFAULT_LOG_DIR)
        print(f"Warning: Cannot write to /var/log/telegram_bot, using {log_dir} instead", file=sys.stderr)
    
    return setup_logging(
        log_level=logging.INFO,
        log_dir=log_dir,
        app_name="telegram_bot",
        console_output=False,  # No console output in production
        file_output=True
    )


def configure_logging_from_env() -> logging.Logger:
    """
    Configure logging based on environment variables.
    
    Environment variables:
        LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR)
        LOG_DIR: Directory for log files
        APP_ENV: Application environment (development, production)
        
    Returns:
        logging.Logger: Configured logger
    """
    # Get environment variables
    log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_dir = os.getenv('LOG_DIR', DEFAULT_LOG_DIR)
    app_env = os.getenv('APP_ENV', 'development').lower()
    
    # Convert log level string to constant
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    log_level = log_level_map.get(log_level_str, logging.INFO)
    
    # Configure based on environment
    if app_env == 'production':
        return setup_logging(
            log_level=log_level,
            log_dir=log_dir,
            app_name="telegram_bot",
            console_output=False,
            file_output=True
        )
    else:
        return setup_logging(
            log_level=log_level,
            log_dir=log_dir,
            app_name="telegram_bot_dev",
            console_output=True,
            file_output=True
        )


# Convenience function for quick setup
def init_logging() -> logging.Logger:
    """
    Initialize logging with default configuration.
    This is the main function to call in application startup.
    
    Returns:
        logging.Logger: Configured logger
    """
    return configure_logging_from_env()


if __name__ == "__main__":
    # Test logging configuration
    logger = init_logging()
    
    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")
    
    # Test specialized logging functions
    log_telegram_event("message", 12345, "testuser", text="Test message")
    log_database_operation("INSERT", "users", user_id=12345, username="testuser")
    log_report_generation("daily", "2023-12-01", "completed", rows=100)
    log_scheduler_event("daily_report", "completed", "2023-12-02 09:00:00")