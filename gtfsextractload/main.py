from dotenv import dotenv_values
from src.services.extract_gtfs_files import extract_gtfs_files
from src.services.load_files_to_raw import load_files_to_raw


def main():
    config = dotenv_values(".env")
    files_list = extract_gtfs_files(
        url=config.get("GTFS_URL"),
        login=config.get("LOGIN"),
        password=config.get("PASSWORD"),
        downloads_folder=config.get("LOCAL_DOWNLOADS_FOLDER"),
    )
    load_files_to_raw(
        folder=config.get("LOCAL_DOWNLOADS_FOLDER"),
        files_list=files_list,
        bucket_name=config.get("RAW_BUCKET_NAME"),
        app_folder=config.get("APP_FOLDER"),
    )


if __name__ == "__main__":
    main()
