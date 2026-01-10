from kafka import KafkaConsumer
import json
from src.services.load_data_to_raw import load_data_to_raw
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def message_structure_is_valid(message):
    """
    Validate the structure of the incoming message.
    :param message: The message to validate
    :return: True if valid, False otherwise
    """
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


def start_consumer(broker, topic, bucket_name, app_folder):
    num_read_messages = 0
    num_invalid_messages = 0

    logger.info(f"[*] Connecting to {broker}...")

    # Initialize the Consumer
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
                # return
                continue
            hour_minute = data.get("payload").get("hr").replace(":", "")
            # e.g., "15:30" -> "1530"
            total_qv = 0
            payload = data.get("payload")
            for line in payload.get("l", []):
                # logger.info(f"Line: {line.get('qv')}")
                total_qv += int(line.get("qv", 0))
            logger.info(
                f"Received data for {total_qv} vehicles from {len(data.get('l', []))} bus lines."
            )
            load_data_to_raw(
                data=data_json,
                raw_bucket_name=bucket_name,
                app_folder=app_folder,
                hour_minute=hour_minute,
            )
            num_read_messages += 1
            logger.info(f"Total messages read: {num_read_messages}\n")
    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
    finally:
        consumer.close()
