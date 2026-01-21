from src.extract_trips_per_line_per_vehicle_pandas import (
    extract_trips_per_line_per_vehicle,
)
from src.infra.db import fetch_data_from_db_as_df
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def generate_trips_for_all_Lines_and_vehicles(config, all_lines_and_vehicles):
    year = "2026"
    month = "01"
    day = "15"

    total_records = len(all_lines_and_vehicles)
    num_processed = 0
    for record in all_lines_and_vehicles:
        logger.info(f"{record}")
        linha_lt = record["linha_lt"]
        veiculo_id = record["veiculo_id"]
        logger.info(f"Line: {linha_lt}, vehicle: {veiculo_id}")
        extract_trips_per_line_per_vehicle(
            config, year, month, day, linha_lt, veiculo_id
        )
        num_processed += 1
        logger.info(f"Record {num_processed}/{total_records} processed.")
    return total_records


def extract_trips_for_all_Lines_and_vehicles(config):
    logger.info("Bulk loading last 3 hours of positions for all vehicles...")

    # 1. Fetch ALL data for the last 3 hours in one single operation
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""
        SELECT 
            veiculo_ts, linha_lt, veiculo_id, linha_sentido, 
            distance_to_first_stop, distance_to_last_stop, 
            is_circular, lt_origem, lt_destino
        FROM {table_name}
        WHERE veiculo_ts >= NOW() - INTERVAL '3 hours'
        ORDER BY veiculo_ts ASC;
    """
    df_all_positions = fetch_data_from_db_as_df(config, sql)

    if df_all_positions.empty:
        logger.warning("No position data found for the last 3 hours.")
        return

    # 2. Group by Line and Vehicle to process them efficiently
    grouped = df_all_positions.groupby(["linha_lt", "veiculo_id"])
    total_groups = len(grouped)
    logger.info(
        f"Processing {total_groups} unique line/vehicle combinations in memory."
    )

    num_processed = 0
    for (linha_lt, veiculo_id), df_group in grouped:
        # 3. Call the modified extraction function passing the pre-loaded data
        extract_trips_per_line_per_vehicle(
            config, None, None, None, linha_lt, veiculo_id, df_group
        )

        num_processed += 1
        if num_processed % 500 == 0:
            logger.info(f"Progress: {num_processed}/{total_groups} processed.")


def extract_trips_for_all_Lines_and_vehicles_db(config):
    # # date params below kept for compatibility during development
    # year = "2026"
    # month = "01"
    # day = "15"
    logger.info("Loading all lines and vehicles...")
    # all_lines_and_vehicles = load_all_lines_and_vehicles(config)
    all_lines_and_vehicles = load_all_lines_and_vehicles_last_3_hours(config)
    total_records = len(all_lines_and_vehicles)
    logger.info(f"Loaded {total_records} records for lines and vehicles")

    generate_trips_for_all_Lines_and_vehicles(config, all_lines_and_vehicles)

    # num_processed = 0
    # for record in all_lines_and_vehicles:
    #     logger.info(f"{record}")
    #     linha_lt = record["linha_lt"]
    #     veiculo_id = record["veiculo_id"]
    #     logger.info(f"Line: {linha_lt}, vehicle: {veiculo_id}")
    #     extract_trips_per_line_per_vehicle(
    #         config, year, month, day, linha_lt, veiculo_id
    #     )
    #     num_processed += 1
    #     logger.info(f"Record {num_processed}/{total_records} processed.")


def load_all_lines_and_vehicles_last_3_hours(config):
    """
    Retrieves a distinct list of all line/vehicle combinations
    that have reported positions within the last 3 hours.
    """
    table_name = config["POSITIONS_TABLE_NAME"]

    # SQL query with a 3-hour time filter and group by logic
    sql = f"""
        SELECT 
            linha_lt, 
            veiculo_id 
        FROM {table_name}
        WHERE veiculo_ts >= NOW() - INTERVAL '3 hours'
        GROUP BY linha_lt, veiculo_id
        ORDER BY linha_lt, veiculo_id;
    """

    # Fetch data using the existing utility function
    df_raw = fetch_data_from_db_as_df(config, sql)

    # Convert the unique combinations into a list of dictionaries
    position_records = df_raw.to_dict("records")

    return position_records


def load_all_lines_and_vehicles(config):
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""
        select linha_lt, veiculo_id from {table_name}
	    group by linha_lt, veiculo_id
	    order by linha_lt, veiculo_id
    """

    df_raw = fetch_data_from_db_as_df(config, sql)
    position_records = df_raw.to_dict("records")

    return position_records


def extract_trips_for_a_test_Line_and_vehicle(config):
    year = "2026"
    month = "01"
    day = "15"
    linha_lt = "2290-10"
    veiculo_id = "41539"
    extract_trips_per_line_per_vehicle(config, year, month, day, linha_lt, veiculo_id)
