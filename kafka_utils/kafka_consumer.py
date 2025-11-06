
import json
from kafka import KafkaConsumer

from db.db_connection import insert_to_mongodb


def create_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        group_id="iot_consumer_group",
        enable_auto_commit=False,
        # consumer_timeout_ms=2000
    )
    

def consumer_iot_message(topic, my_collection, max_docs, logger):
    
    """summary"""
        
    logger.info(f"consumer.consumer_iot_message - Consumption from topic: {topic}")
    
    consumer = create_kafka_consumer(topic)
     
    my_docs = []
    
    for msg in consumer:
        
        # Extract information from kafka
        message = json.loads(msg.value.decode("utf-8"))
        
        # Add message to events
        my_docs.append(message)
        
        # break
        if len(my_docs) >= max_docs:
            insert_to_mongodb(my_collection, my_docs, logger)
            consumer.commit()
            
            logger.info(f"kafka_consumer.consumer_iot_message - Events saved in Mongodb")
            
            # reset
            my_docs = []
        
    consumer.close()
    return 