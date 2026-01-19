from src.infra.db import fetch_data_from_db_as_df
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions_for_line_and_vehicle(config, year, month, day, linha_lt, veiculo_id):
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""
        SELECT veiculo_ts, linha_sentido, distance_to_first_stop, distance_to_last_stop, is_circular, lt_origem, lt_destino
        FROM {table_name}
        WHERE linha_lt = '{linha_lt}' AND veiculo_id = {veiculo_id}
        ORDER BY veiculo_ts ASC;
    """

    df_raw = fetch_data_from_db_as_df(config, sql)
    position_records = df_raw.to_dict("records")

    return position_records
