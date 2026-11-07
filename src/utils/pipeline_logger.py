
import logging
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_FOLDER = PROJECT_ROOT / "logs"
LOG_FOLDER.mkdir(parents=True, exist_ok=True)

log_file = LOG_FOLDER / (
    f"retail_pipeline_{datetime.now():%Y%m%d}.log"
)

def get_logger(name):

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if not logger.handlers:

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        file_handler = logging.FileHandler(
            log_file,
            encoding="utf-8"
        )

        console_handler = logging.StreamHandler()

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
