import logging
import logging.handlers
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str = "nyc_taxi_platform",
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    log_dir: str = "logs",
) -> logging.Logger:
    """
    Set up and configure a logger for the application.

    Args:
        name: Logger name (default: "nyc_taxi_platform")
        level: Logging level (default: logging.INFO)
        log_file: Optional log file name. If provided, logs will be written to file.
        log_dir: Directory to store log files (default: "logs")

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers if logger already configured
    if logger.hasHandlers():
        return logger

    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (always enabled)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    if log_file:
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_path / log_file,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(name: str = "nyc_taxi_platform") -> logging.Logger:
    """
    Get an existing logger instance.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return setup_logger(name)
