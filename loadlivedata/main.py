from src.services.consume_and_process_messages import consume_and_process_messages
from src.config import get_config
import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "loadlivedata.log"

# In Airflow just remove this logging configuration block
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Rotation: 5MB per file, keeping the last 5 files
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),  # Also keeps console output
    ],
)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    config = get_config()
    consume_and_process_messages(config)
