Este projeto faz uso de um monorepo com diferentes subprojetos que compõe o SPTransInsights

Olhe o README de cada subprojeto para mais informações

Requisitos para o funcionamento do projeto:
 docker compose up -d kafka-broker akhq
 docker compose up -d minio
 docker compose up -d postgres
 docker compose up -d postgres_airflow webserver scheduler
docker compose up -d metabase

 AKHQ (Kafka): 
 http://localhost:28080/ui/

 Minio:
 http://localhost:9001/login

 Airflow:
 http://localhost:8080/


# #############################################################
TO DO:

Em gtfstransform:
    criar a tabela trusted.trips_extended com os campos e computar para consulta pelo livedatatransform:
        trip_id (from trips)
        is_circular (from trips)
        tp_stop_id (from stop_times)
        tp_stop_name (from stops)
        tp_lat (from stops)
        tp_lon (from stops)
        ts_stop_id (from stop_times) (null quando is_circular)
        tp_stop_name (from stops)
        ts_lat (from stops) (null quando is_circular)
        ts_lon (from stops) (null quando is_circular)
        <!-- trip_distance_tp_ts (from  )
        trip_distance_ts_tp (null quando is_circular) -->
        trip_linear_distance (calculated from tp_lat, tp_lon, ts_lat, ts_lon in this table)


Em livedatatransform:
    Na trusted.positions incluir os campos e computar os campos
        trip_id 
        tp_current_distance
        ts_current_distance


Em um novo processo refinelivedata:
Criar as tabelas refined.finished_trips e ongoing_trips
    trip_id
    vehicle_id
    trip_start_time
    trip_end_time
    duration
    average_speed
    is_circular

Quando a distancia ao tp (origem) for superior ao limite de tolerancia (exemplo 30 metros) a viagem começa
Quando a distancia ao ts (destino) for inferior ao limite de tolerancia (exemplo 30 metros) a viagem termina

Se a viagens for circular a dist

Adicionar docker-compose.yaml e config de serviços
Dockerizar algum processo que esteja sendo em Python sem airflow



