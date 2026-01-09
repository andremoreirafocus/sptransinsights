import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_routes(source_bucket, dest_bucket, app_folder):
    print("Transforming routes...")
    # Add your logic to transform routes here
    print("Routes transformed successfully.")


def transform_trips(source_bucket, dest_bucket, app_folder):
    print("Transforming trips...")
    # Add your logic to transform trips here
    print("Trips transformed successfully.")


def transform_stop_times(source_bucket, dest_bucket, app_folder):
    print("Transforming stop times...")
    # Add your logic to transform stop times here
    print("Stop times transformed successfully.")


def transform_stops(source_bucket, dest_bucket, app_folder):
    print("Transforming stops...")
    # Add your logic to transform stops here
    print("Stops transformed successfully.")


def transform_calendar(source_bucket, dest_bucket, app_folder):
    print("Transforming calendar...")
    # Add your logic to transform calendar here
    print("Calendar transformed successfully.")


def transform_frequencies(source_bucket, dest_bucket, app_folder):
    print("Transforming frequencies...")
    # Add your logic to transform frequencies here
    print("Frequencies transformed successfully.")
