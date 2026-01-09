from src.infra.db import db_cursor
from src.infra.minio_functions import read_file_from_minio
from src.infra.get_minio_connection_data import get_minio_connection_data
import logging
import json

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(source_bucket, app_folder):
    """
    Load position data from source bucket and app folder.
    :param source_bucket: Source bucket name
    :param app_folder: Application folder path
    :return: Loaded data
    """
    logger.info(
        f"Loading position data from bucket: {source_bucket}, folder: {app_folder}"
    )
    # Add your logic to load position data here
    # Example: read files from MinIO, parse them, and return as a list of records
    year = 2026
    month = "01"
    day = "09"
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
    hour_minute = "0947"
    base_file_name = "posicoes_onibus"
    connection_data = get_minio_connection_data()
    object_name = f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
    datastr = read_file_from_minio(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # print(data)
    data = json.loads(datastr)
    payload = data["payload"]
    return payload


def get_positions_table_from_raw(raw_positions):
    positions_table = []
    if "hr" not in raw_positions:
        logger.error("No 'hr' field found in raw positions data.")
        return None
    if "l" not in raw_positions:
        logger.error("No 'l' field found in raw positions data.")
        return None
    for line in raw_positions["l"]:
        # print(line)
        number_of_vehicles = 0
        for vehicle in line.get("vs", []):
            print(vehicle)
            vehicle_record = {
                "vehicle_id": vehicle.get("id"),
                "hora": raw_positions.get("hr"),
                "linha_lt": line.get("c"),
                "linha_code": line.get("cl"),
                "linha_sentido": line.get("sl"),
                "lt_destino": line.get("lt0"),
                "lt_origem": line.get("lt1"),
                "veiculo_prefixo": vehicle.get("p"),
                "veiculo_acessivel": vehicle.get("a"),
                "veiculo_ts": vehicle.get("ta"),
                "veiculo_lat": vehicle.get("py"),
                "veiculo_long": vehicle.get("px"),
            }
            number_of_vehicles += 1
            positions_table.append(vehicle_record)
        if number_of_vehicles != int(line.get("qv")):
            logger.warning(
                f"Expected {line.get('q', 0)} vehicles for line {line.get('qv')}, but found {number_of_vehicles}."
            )
        else:
            logger.info(
                f"Processed {number_of_vehicles} vehicles for line {line.get('qv')}."
            )
    return positions_table


def transform_position(source_bucket, app_folder, table_name):
    logger.info("Transforming position...")
    raw_positions = load_positions(source_bucket, app_folder)
    if not raw_positions:
        logger.error("No position data found to transform.")
        return
    positions_table = get_positions_table_from_raw(raw_positions)
    return
    TRANSFORMATION_SQL = """
    INSERT INTO trusted.my_fact_table (col1, col2, col3)
    SELECT
        col1,
        col2,
        col3
    FROM staging.my_raw_table
    WHERE ingestion_date = %(ingestion_date)s;
    """
    ingestion_date = "2024-01-01"  # Example ingestion date
    params = {"ingestion_date": ingestion_date}
    with db_cursor() as cur:
        cur.execute(TRANSFORMATION_SQL, params)  # execute arbitrary SQL via psycopg2.

    print("Position transformed successfully.")
