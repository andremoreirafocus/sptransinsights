from src.extract_trips_per_line_per_vehicle import extract_trips_per_line_per_vehicle
from src.infra.db import fetch_data_from_db_as_df
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_for_all_Lines_and_vehicles(config):
    year = "2026"
    month = "01"
    day = "15"
    linha_lt = "2290-10"
    veiculo_id = "41539"
    extract_trips_per_line_per_vehicle(config, year, month, day, linha_lt, veiculo_id)

    # logger.info("Loading all lines and vehicles...")
    # all_lines_and_vehicles = load_all_lines_and_vehicles(config)
    # logger.info(f"Loaded {len(all_lines_and_vehicles)} records for lines and vehicles")
    # for record in all_lines_and_vehicles:
    #     print(f"{record}")
    #     linha_lt = record["linha_lt"]
    #     veiculo_id = record["veiculo_id"]
    #     print(f"Line: {linha_lt}, vehicle: {veiculo_id}")
    #     extract_trips_per_line_per_vehicle(
    #         config, year, month, day, linha_lt, veiculo_id
    #     )


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
