Este projeto consome dados de um tópico Kafka e salva em um bucket no Minio 

Configurações necessárias no arquivo .env do projeto:
INTERVALO = 120  # 2 minutos em segundos
KAFKA_BROKER = <kafka_broker>
KAFKA_TOPIC = <kafka_topic>
MINIO_ENDPOINT= <hostname:porta>
ACCESS_KEY= <key>
SECRET_KEY= <secret>
RAW_BUCKET_NAME = <bucket_name>
APP_FOLDER = <folder>

Para instalar os requisitos:
- cd <diretorio deste subprojeto>
- python3 -m venv .env
- source .venv/bin/activate
- pip install -r requirements.txt

Para executar: 
python ./main.py

Instruções adicionais:
Kafka:
    Foi necessário mudar a porta do akhq para 28080 no docker-compose-yaml para parar de conflitar com outros componentes
    docker compose up -d kafka-broker zookeeper akhq

    Para criar o tópico:
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;







