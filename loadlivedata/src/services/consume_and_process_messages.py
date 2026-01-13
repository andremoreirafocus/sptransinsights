from src.services.load_data_to_raw import load_data_to_raw
from src.infra.message_broker import get_Kafka_consumer
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def consume_and_process_messages(config):
    def get_config(config):
        topic = config["KAFKA_TOPIC"]
        broker = config["KAFKA_BROKER"]
        return broker, topic

    broker, topic = get_config(config)
    num_read_messages = 0
    num_invalid_messages = 0
    consumer = get_Kafka_consumer(broker, topic)
    logger.info(f"[*] Waiting for messages on topic: {topic}. To exit press CTRL+C")
    logger.info(f"Total messages read: {num_read_messages}\n")
    try:
        for message in consumer:
            # 'message.value' is now a Python dictionary thanks to value_deserializer
            data_json = message.value
            logger.info(f"--- New Message Received at {message.timestamp} ---")
            data = json.loads(data_json)
            if not message_structure_is_valid(data):
                num_invalid_messages += 1
                logger.warning(f"Total invalid messages : {num_invalid_messages}\n")
                continue
            hour_minute, total_qv, total_bus_lines = get_payload_summary(data)
            logger.info(
                f"Received data for {total_qv} vehicles from {total_bus_lines} bus lines."
            )
            load_data_to_raw(
                config,
                data=data_json,
                hour_minute=hour_minute,
            )
            num_read_messages += 1
            logger.info(f"Total messages read: {num_read_messages}\n")
    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
    finally:
        consumer.close()


def get_payload_summary(data):
    hour_minute = data.get("payload").get("hr").replace(":", "")
    total_qv = 0
    payload = data.get("payload")
    for line in payload.get("l", []):
        # logger.info(f"Line: {line.get('qv')}")
        total_qv += int(line.get("qv", 0))
    total_bus_lines = len(data.get("l", []))
    return hour_minute, total_qv, total_bus_lines


def message_structure_is_valid(message):
    if not isinstance(message, dict):
        logger.error("Message does not have a valid structure.")
        return False
    required_fields = ["payload", "metadata"]
    for field in required_fields:
        if field not in message:
            logger.error(f"Missing required field: {field}")
            logger.error(f"Message content: {message}")
            return False
    if not isinstance(message.get("metadata"), dict):
        logger.error("Message metadata does not have a valid structure.")
        return False
    required_fields = ["source", "extracted_at", "total_vehicles"]
    for field in required_fields:
        if field not in message.get("metadata"):
            logger.error(f"Missing required metadata field: {field}")
            logger.error(f"Metadata content: {message.get('metadata')}")
            return False
    if not isinstance(message.get("payload"), dict):
        logger.error("Message payload does not have a valid structure.")
        logger.error(f"Payload content: {message.get('payload')}")
        logger.error(f"Metadata content: {message.get('metadata')}")
        return False
    required_fields = ["hr", "l"]
    for field in required_fields:
        if field not in message.get("payload"):
            logger.error(f"Missing required payload field: {field}")
            return False
    return True
