from src.services.load_positions_for_line_and_vehicle import (
    load_positions_for_line_and_vehicle,
)
from src.services.extract_trips_from_positions import (
    extract_raw_trips_metadata,
    filter_healthy_trips,
    generate_trips_table,
)
from src.services.save_trips_to_db import save_trips_to_db
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_per_line_per_vehicle(config, year, month, day, linha_lt, veiculo_id):
    position_records = load_positions_for_line_and_vehicle(
        config, year, month, day, linha_lt, veiculo_id
    )
    if not position_records:
        logger.error(
            f"No positions retrieved for line {linha_lt} and vehicle {veiculo_id}"
        )
    logger.info("Extracting raw trips metadata from position records...")
    raw_trips_metadata = extract_raw_trips_metadata(position_records)
    logger.info(
        f"Extracted {len(raw_trips_metadata)} raw trips metadata from position records for line {linha_lt} and vehicle {veiculo_id}."
    )
    # return raw_trips_metadata
    clean_trips_metadata = filter_healthy_trips(raw_trips_metadata, position_records)
    logger.info("Generating records for trips...")
    clean_trips = generate_trips_table(
        position_records, clean_trips_metadata, linha_lt, veiculo_id
    )
    logger.info(f"Generated {len(clean_trips)} records for trips.")

    logger.info(f"Saving {len(clean_trips)} trips to database...")
    save_trips_to_db(config, clean_trips)
    logger.info("Saved trips to database.")
