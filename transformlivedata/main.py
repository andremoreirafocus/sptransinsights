from dotenv import dotenv_values
from src.services.transforms import (
    transform_position,
)


def main():
    config = dotenv_values(".env")
    source_bucket = config["SOURCE_BUCKET"]
    app_folder = config["APP_FOLDER"]
    db_connection = None
    table_name = config["TABLE_NAME"]
    transform_position(source_bucket, app_folder, db_connection, table_name)

    if __name__ == "__main__":
        main()
