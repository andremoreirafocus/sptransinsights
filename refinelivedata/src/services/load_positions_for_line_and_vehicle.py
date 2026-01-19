# algoritmo de busca das trips a partir das posições
# extrai as posições de um veículo em uma linha específica e um veiculo específico
# checa se tem descontinuidades para dividir em segmentos
# identifica as trips em um segmento com base apenas no sentido, salvando start record e end record
# Para cada trip
#   valida se a trip está completa (
#       checa se começa no ponto inicial e se acaba no ponto final
#           se nao for valida
#               adiciona a lista de trips em quarentena para análise posterior
#               vai para a próxima trip
#           se for valida
#               extrai os trechos de parada nos terminais
#               adiciona a trip na lista de trips válidas
#

from zoneinfo import ZoneInfo
import logging

from src.infra.db import fetch_data_from_db_as_df
from src.services.save_trips_to_db import save_trips_to_db
# from datetime import timedelta

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


# def load_positions_for_line_and_vehicle(config, year, month, day, linha_lt, veiculo_id):
#     table_name = config["POSITIONS_TABLE_NAME"]
#     sql = f"""select veiculo_ts, veiculo_id,
#        distance_to_first_stop, distance_to_last_stop,
#        is_circular, linha_sentido,
# 	   lt_origem, lt_destino
#        from  {table_name}
#        where linha_lt = '{linha_lt}' and veiculo_id = {veiculo_id}
#        order by veiculo_ts asc;"""

#     logger.info(f"Loading position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
#     logger.debug(f"SQL Query: {sql}")
#     df_filtered_positions = fetch_data_from_db_as_df(config, sql)
#     logger.info(f"Loaded position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
#     logger.info(df_filtered_positions.head(5))
#     return df_filtered_positions


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



