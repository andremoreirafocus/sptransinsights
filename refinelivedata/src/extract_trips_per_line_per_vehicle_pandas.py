from src.services.load_positions_for_line_and_vehicle import (
    load_positions_for_line_and_vehicle,
    load_positions_for_line_and_vehicle_last_3_hous,
)
from src.services.extract_trips_from_positions import (
    extract_raw_trips_metadata,
    filter_healthy_trips,
    generate_trips_table,
)
from src.services.save_trips_to_db import save_trips_to_db
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_per_line_per_vehicle(
    config, year, month, day, linha_lt, veiculo_id, df_preloaded=None
):
    try:
        # CHANGE: Use preloaded slice if available, otherwise fetch from DB
        if df_preloaded is not None:
            # df_preloaded is already filtered by the caller, just convert to dicts
            position_records = df_preloaded.to_dict("records")
        else:
            # Fallback for old implementation (sequential DB calls)
            position_records = load_positions_for_line_and_vehicle_last_3_hous(
                config, year, month, day, linha_lt, veiculo_id
            )

        if not position_records:
            # Reduced log level to debug so logs aren't flooded with empty vehicle slots
            logger.debug(f"No positions for line {linha_lt} vehicle {veiculo_id}")
            return

        # --- Remaining logic stays EXACTLY as it was ---
        raw_trips_metadata = extract_raw_trips_metadata(position_records)

        if not raw_trips_metadata:
            return

        clean_trips_metadata = filter_healthy_trips(
            raw_trips_metadata, position_records
        )

        if not clean_trips_metadata:
            return

        finished_trips = generate_trips_table(
            position_records, clean_trips_metadata, linha_lt, veiculo_id
        )
        if finished_trips:
            # 4. DATA ADAPTATION: Convert to Tuples for Psycopg2
            # We perform the conversion here into a separate variable
            processed_trips_tuples = []

            for trip in finished_trips:
                # trip is still a DICT here, so ['key'] works perfectly
                vehicle_id = int(trip["vehicle_id"].item())
                print(vehicle_id, type(vehicle_id))
                row = (
                    str(trip["trip_id"]),
                    vehicle_id,
                    trip["trip_start_time"].to_pydatetime()
                    if hasattr(trip["trip_start_time"], "to_pydatetime")
                    else trip["trip_start_time"],
                    trip["trip_end_time"].to_pydatetime()
                    if hasattr(trip["trip_end_time"], "to_pydatetime")
                    else trip["trip_end_time"],
                    str(trip["duration"]),  # Fix Timedelta to string
                    bool(trip["is_circular"]),  # Fix numpy.bool
                    float(trip["average_speed"]),  # Fix numpy.float64
                )
                print(f"row: {row}")
                processed_trips_tuples.append(row)

            # 5. Database Save
            # Pass the TUPLES to the DB function
            logger.info(
                f"Saving {len(processed_trips_tuples)} trips for {linha_lt}/{veiculo_id}..."
            )
            save_trips_to_db(config, processed_trips_tuples)
            # save_trips_to_db(config, finished_trips)

    except Exception as e:
        logger.error(f"Error processing {linha_lt}/{veiculo_id}: {e}")


def extract_trips_per_line_per_vehicle_db(
    config, year, month, day, linha_lt, veiculo_id
):
    try:
        # position_records = load_positions_for_line_and_vehicle(
        position_records = load_positions_for_line_and_vehicle_last_3_hous(
            config, year, month, day, linha_lt, veiculo_id
        )
        if not position_records:
            logger.error(
                f"No positions retrieved for line {linha_lt} and vehicle {veiculo_id}"
            )
        logger.info("Extracting raw trips metadata from position records...")
        raw_trips_metadata = extract_raw_trips_metadata(position_records)
        logger.info(
            f"Extracted {len(raw_trips_metadata)} raw trips metadata from position records for line {linha_lt} and vehicle {veiculo_id}."
        )
        if not raw_trips_metadata:
            logger.warning(
                f"No trips found for line {linha_lt} and vehicle {veiculo_id}."
            )
            return
        clean_trips_metadata = filter_healthy_trips(
            raw_trips_metadata, position_records
        )
        if not clean_trips_metadata:
            logger.warning(
                f"No clean trips found for line {linha_lt} and vehicle {veiculo_id}."
            )
            return
        logger.info("Generating records for trips...")
        finished_trips = generate_trips_table(
            position_records, clean_trips_metadata, linha_lt, veiculo_id
        )
        logger.info(f"Generated {len(finished_trips)} records for trips.")

        logger.info(f"Saving {len(finished_trips)} trips to database...")
        save_trips_to_db(config, finished_trips)
        logger.info("Saved trips to database.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
