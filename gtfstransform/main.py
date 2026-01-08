from dotenv import dotenv_values
from gtfstransform.src.services.transforms import (
    transform_calendar,
    transform_frequencies,
    transform_routes,
    transform_stop_times,
    transform_stops,
    transform_trips,
)


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
