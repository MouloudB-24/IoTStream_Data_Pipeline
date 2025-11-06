
import json
from kafka import KafkaProducer


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8")
    )
    
    
def produce_iot_message(topic, json_thing, deviceId, logger):
    
    """summary"""
    
    producer = create_kafka_producer()
    
    if type(json_thing) is not str:
        message = json.dumps(json_thing)
    else:
        message = json_thing
        
    try:
        producer.send(topic, key=deviceId, value=message)
        producer.flush()
    
    except Exception as e:
        logger.error("Error in the kafka_utils.produce_iot_message")
    
    logger.debug(f"kafka_utils.produce_iot_message - Divece ID {deviceId} - Sent message to topic: {topic}")