#####################################################################
#
# Project       : IoT based TimesSeries Data via Python Application
#
# File          : simulate.py
#
# Description   : Data simulator
#
# Created       : 1 November 2025
#
# Author        : Mouloud BELLIL
#
# Email         : mouloud.bellil@outlook.fr
#
#######################################################################

import os
import random
import time
from datetime import datetime

from utils import pp_json
from connection import create_mongo_connection, insert_mongo
from kafka_utils import producer_sensor_data, consumer_sensor_data
from config import sensor_topic_name


def generate_value(sensor, method="normal"):
    
    """
    Generate a sensor value
    Scaling ecart_type(sd) based on stability_factor if the current time is whithin device start_time and end_time
    If start_time and end_time are not provided for the device, no scaling is applied
    """
    
    sd = sensor["sd"]
    mean = sensor["mean"]
             
    # Generate sensor value using normal or uniform distribution
    if method == "uniform":
        lowed_bound = max(0, mean - sd)
        upper_bound = mean + sd
        return round(random.uniform(lowed_bound, upper_bound), 4)
    
    return round(random.gauss(mean, sd), 4)
    

def generate_payload(sensor, device, site_id, current_time):
    
    """Function to generate payloads with timezone in the timestamp"""
    
    # Generate the timestamp
    timestamp = current_time.isoformat()
    
    # Generate the sonsor measurement
    measurement = generate_value(sensor, datetime.now())
    
    return {
        "timestamp": timestamp,
        "metadata": {
            "siteId": site_id,
            "deviceId": device["deviceId"],
            "sensorId": sensor["sensorId"],
            "unit": sensor["unit"]
        },
        "measurement": measurement
    }
    

def run_simulation(site, params, logger):
    
    """Function to run simulation for a specific site"""

    logger.debug(f"simulation.run_simulation - Site ID {site["siteId"]}: Printing Complete site record")
    pp_json(site, logger)
     
    # MongoDB persistence
    mongodb_collection = create_mongo_connection(params, site["siteId"], logger)
    
    if mongodb_collection == -1:
        logger.critical(f"simulate.run_simulation - Site ID {site["siteId"]} EXITING")
        os._exit(1)
        
    logger.info(f"simulate.run_simulation - Site ID {site["siteId"]}: Starding Simulation")
                
    while True:
        current_time = datetime.now()
        
        for device in site["devices"]:
            
            logger.info(f"kafka_utils.producer_sensor_data Device ID {device["deviceId"]} - Sending messages to topic {sensor_topic_name}")
            
            for sensor in device["sensors"]:
                payload = generate_payload(sensor, device, site["siteId"], current_time)
                
                # envoyer des données dans topic kafka
                producer_sensor_data(sensor_topic_name, payload, device["deviceId"], sensor["sensorId"], logger)
                        
                logger.debug(f"simulate.run_simulation Device ID {device["deviceId"]}: Payload {payload}")
            
                sensor["last_value"] = payload["measurement"]
            
            # Call a kafka consumer
            my_docs = consumer_sensor_data(sensor_topic_name, device, logger)
            
            # Loading data into Mongodb
            insert_mongo(mongodb_collection, site["siteId"], my_docs, logger)
            
            logger.info(f"simulation.run_simulation Device ID {device["deviceId"]}: Messages saved in Mongodb")
            logger.info("*")
            
            # reset my_docs
            my_docs = []

        time.sleep(site["sleeptime"] / 1000)     
        
        logger.info(f"simulation.run_simulation - Site ID {site["siteId"]}: IoT data generation is in progress...")
        logger.info("To stop press Ctrl+C")
        logger.info("")
    
    
    