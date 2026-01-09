from dotenv import dotenv_values
from gtfstransform.src.services.transforms import (
    transform_calendar,
    transform_frequencies,
    transform_routes,
    transform_stop_times,
    transform_stops,
    transform_trips,
)
import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "gtfstransform.log"

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


def main():
    config = dotenv_values(".env")
    source_bucket = config["SOURCE_BUCKET"]
    dest_bucket = config["DEST_BUCKET"]
    app_folder = config["APP_FOLDER"]
    transform_routes(source_bucket, dest_bucket, app_folder)
    transform_trips(source_bucket, dest_bucket, app_folder)
    transform_stop_times(source_bucket, dest_bucket, app_folder)
    transform_stops(source_bucket, dest_bucket, app_folder)
    transform_calendar(source_bucket, dest_bucket, app_folder)
    transform_frequencies(source_bucket, dest_bucket, app_folder)

    if __name__ == "__main__":
        main()
