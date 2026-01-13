from kafka import KafkaConsumer
import logging
import json

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_Kafka_consumer(broker, topic):
    logger.info(f"[*] Connecting to {broker}...")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=broker,
        # Automatically handle JSON decoding
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        # Start from the earliest messages if no offset is stored
        auto_offset_reset="earliest",
        # Group ID allows multiple consumers to work together
        group_id="bus_monitor_group",
    )
    return consumer
