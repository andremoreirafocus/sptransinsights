from src.infra.get_minio_connection_data import get_minio_connection_data
from src.infra.minio_functions import read_file_from_minio
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_raw_csv(source_bucket, app_folder, file_name):
    """
    Load position data from source bucket and app folder.
    :param source_bucket: Source bucket name
    :param app_folder: Application folder path
    :return: Loaded data
    """
    logger.info(
        f"Loading {file_name} csv data from bucket: {source_bucket}, folder: {app_folder}"
    )
    # Add your logic to load position data here
    # Example: read files from MinIO, parse them, and return as a list of records
    prefix = f"{app_folder}/"
    connection_data = get_minio_connection_data()
    object_name = f"{prefix}{file_name}/{file_name}.txt"
    print(f"Reading object: {object_name} from bucket: {source_bucket} ...")
    datastr = read_file_from_minio(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # logger.info(data)
    print(datastr)

    return datastr


def transform_routes(config):
    SOURCE_BUCKET = config["SOURCE_BUCKET"]
    APP_FOLDER = config["APP_FOLDER"]
    BASE_TABLE_NAME = config["BASE_TABLE_NAME"]
    DB_HOST = config["DB_HOST"]
    DB_PORT = config["DB_PORT"]
    DB_DATABASE = config["DB_DATABASE"]
    DB_USER = config["DB_USER"]
    DB_PASSWORD = config["DB_PASSWORD"]
    DB_SSLMODE = config["DB_SSLMODE"]
    print("Transforming routes...")
    load_raw_csv(SOURCE_BUCKET, APP_FOLDER, "routes")
    # Add your logic to transform routes here
    print("Routes transformed successfully.")


def transform_trips(config):
    print("Transforming trips...")
    # Add your logic to transform trips here
    print("Trips transformed successfully.")


def transform_stop_times(config):
    print("Transforming stop times...")
    # Add your logic to transform stop times here
    print("Stop times transformed successfully.")


def transform_stops(config):
    print("Transforming stops...")
    # Add your logic to transform stops here
    print("Stops transformed successfully.")


def transform_calendar(config):
    print("Transforming calendar...")
    # Add your logic to transform calendar here
    print("Calendar transformed successfully.")


def transform_frequencies(config):
    print("Transforming frequencies...")
    # Add your logic to transform frequencies here
    print("Frequencies transformed successfully.")
