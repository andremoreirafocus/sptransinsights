from src.services.load_positions import load_positions
from src.services.get_positions_table_from_raw import get_positions_table_from_raw
from src.services.save_positions_to_db import save_positions_to_db
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_position(config, year, month, day, hour, minute):
    logger.info("Transforming position...")
    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logger.error("No position data found to transform.")
        return
    positions_table = get_positions_table_from_raw(raw_positions)
    if not positions_table:
        logger.error("No valid position records found after transformation.")
        return
    try:
        save_positions_to_db(config, positions_table)
    except Exception as e:
        logger.error(f"Error saving positions to DB: {e}")
        return
    logger.info("Positions transformed successfully.")
