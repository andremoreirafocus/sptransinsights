from dotenv import dotenv_values
from src.infra.message_broker import start_consumer
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
    config = dotenv_values(".env")
    broker = config.get("KAFKA_BROKER")
    topic = config.get("KAFKA_TOPIC")

    start_consumer(
        broker=config.get("KAFKA_BROKER"),
        topic=config.get("KAFKA_TOPIC"),
        bucket_name=config.get("RAW_BUCKET_NAME"),
        app_folder=config.get("APP_FOLDER"),
    )
