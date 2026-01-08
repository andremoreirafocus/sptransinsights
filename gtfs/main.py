from dotenv import dotenv_values
from src.services.extract_gtfs_files import extract_gtfs_files


def main():
    config = dotenv_values(".env")
    extract_gtfs_files(
        url=config.get("GTFS_URL"),
        login=config.get("LOGIN"),
        password=config.get("PASSWORD"),
        downloads_folder=config.get("DOWNLOADS_FOLDER"),
    )


if __name__ == "__main__":
    main()
