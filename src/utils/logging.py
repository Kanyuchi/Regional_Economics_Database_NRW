"""
Logging Configuration Module
Regional Economics Database for NRW

Sets up and configures logging using loguru.
"""

import sys
from pathlib import Path
from loguru import logger
from typing import Optional


class LoggerManager:
    """Manages application logging configuration."""

    def __init__(self, log_dir: Optional[str] = None):
        """
        Initialize the logger manager.

        Args:
            log_dir: Directory for log files. Defaults to project_root/logs
        """
        # Determine log directory
        if log_dir is None:
            project_root = Path(__file__).parent.parent.parent
            self.log_dir = project_root / "logs"
        else:
            self.log_dir = Path(log_dir)

        # Create log directory if it doesn't exist
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Flag to track if logger is configured
        self._configured = False

    def setup(self, level: str = "INFO", console: bool = True) -> None:
        """
        Set up logging configuration.

        Args:
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            console: Whether to log to console
        """
        if self._configured:
            return

        # Remove default handler
        logger.remove()

        # Console handler
        if console:
            logger.add(
                sys.stdout,
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                       "<level>{level: <8}</level> | "
                       "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
                       "<level>{message}</level>",
                level=level,
                colorize=True
            )

        # Main log file
        logger.add(
            self.log_dir / "app_{time:YYYY-MM-DD}.log",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
            level="DEBUG",
            rotation="100 MB",
            retention="30 days",
            compression="zip",
            encoding="utf-8"
        )

        # Error log file
        logger.add(
            self.log_dir / "error_{time:YYYY-MM-DD}.log",
            format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {name}:{function}:{line} | {message}",
            level="ERROR",
            rotation="100 MB",
            retention="90 days",
            compression="zip",
            encoding="utf-8"
        )

        self._configured = True
        logger.info("Logging configured successfully")

    def get_logger(self, name: Optional[str] = None):
        """
        Get a logger instance.

        Args:
            name: Optional logger name (usually __name__)

        Returns:
            Logger instance
        """
        if not self._configured:
            self.setup()

        if name:
            return logger.bind(name=name)
        return logger


# Global logger manager instance
_logger_manager = None


def setup_logging(level: str = "INFO", console: bool = True) -> None:
    """
    Set up application logging (convenience function).

    Args:
        level: Logging level
        console: Whether to log to console
    """
    global _logger_manager

    if _logger_manager is None:
        _logger_manager = LoggerManager()

    _logger_manager.setup(level=level, console=console)


def get_logger(name: Optional[str] = None):
    """
    Get a logger instance (convenience function).

    Args:
        name: Optional logger name (usually __name__)

    Returns:
        Logger instance
    """
    global _logger_manager

    if _logger_manager is None:
        _logger_manager = LoggerManager()

    return _logger_manager.get_logger(name)


# Create module-level logger
log = get_logger(__name__)
