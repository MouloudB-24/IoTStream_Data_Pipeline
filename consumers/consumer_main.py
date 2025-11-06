
import os
from config import IoT_topic, NB_SONSORS
from db.db_connection import create_mongo_connection
from kafka_utils.kafka_consumer import consumer_iot_message


def main(params, logger):
    
    # MongoDB persistence
    mongodb_collection = create_mongo_connection(params, logger)

    if mongodb_collection == -1:
        logger.critical(f"simulate.generate_iot_events - EXITING")
        os._exit(1)
    
    # Loading data into Mongodb
    consumer_iot_message(IoT_topic, mongodb_collection, NB_SONSORS, logger)