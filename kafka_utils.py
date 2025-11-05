import json
import os
import pprint
from kafka import KafkaProducer, KafkaConsumer

from connection import create_mongo_connection
    

def producer_sensor_data(topic_name, json_thing, deviceId, sensorId, logger):
    
    """summary"""
    
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8")
        )

    if type(json_thing) is not str:
        message = json.dumps(json_thing)
    else:
        message = json_thing
        
    try:
        producer.send(topic_name, key=deviceId, value=message)
    
    except Exception as e:
        logger.error("Error in the kafka_utils.producer_sensor_data")
    
    
    logger.debug(f"kafka_utils.producer_sensor_data - Sensor ID {sensorId} - Sent message to topic: {topic_name}")
    

def consumer_sensor_data(topic_name, device, logger):
    
    """summary"""
        
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest"
        )
    
    logger.info(f"kafka_utils.consumer_sensor_data Device ID {device["deviceId"]} - Consumption messages from topic {topic_name}")
     
    my_docs = []
    
    for msg in consumer:
        
        # Extract information from kafka
        message = json.loads(msg.value.decode("utf-8"))
        
        # Add docments into collection
        my_docs.append(message)
        
        if len(my_docs) == len(device["sensors"]):
            return my_docs
            
    
    
    
    