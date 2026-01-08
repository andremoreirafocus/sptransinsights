from dotenv import dotenv_values
from src.infra.message_broker import start_consumer

if __name__ == "__main__":
    config = dotenv_values(".env")
    broker = config.get("KAFKA_BROKER")
    topic = config.get("KAFKA_TOPIC")

    start_consumer(broker, topic)
