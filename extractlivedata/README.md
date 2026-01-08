Este projeto extrai a informação de posição dos ônibus a partir da API da SPTRANS e envia o payload comprimido para um tópico Kafka.

Configurações necessárias no arquivo .env do projeto:
API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
TOKEN =  <insira o seu token>
INTERVALO = 120  # 2 minutos em segundos
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "bus_positions"

Para instalar os requisitos:
pip install -r requirements.txt

Kafka:
    Foi necessário mudar a porta do akhq para 28080 no docker-compose-yaml para parar de conflitar com outros componentes
    docker compose up -d kafka-broker zookeeper akhq

    Para criar o tópico:
    docker exec -it kafka-broker /bin/bash
    kafka-topics --bootstrap-server localhost:9092 --create --partitions 1 --replication-factor 1 --topic sptrans-positions;


Para executar: 
python ./main.py


