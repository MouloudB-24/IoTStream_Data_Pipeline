#####################################################################
#
# Project       : Smart Irrigation Optimisation Platform
#
# File          : kafka_producer.py
#
# Description   : Implementation of the iot data producer kafka
# 
# Created       : 7 November 2025
#
# Author        : Mouloud BELLIL
#
# Email         : mouloud.bellil@outlook.fr
#
#######################################################################

from datetime import datetime
import os
import time

from conf.config import IoT_topic
from src.data_ingestion.iot_simulator import simulate_iot
from src.utils.json_utils import pp_json, read_input_data
from src.utils.kafka_utils import produce_message


def produce_iot(params, logger):
    
    """_summary_"""
    
    # Load data site
    sites = read_input_data(params["INPUT_DATA_FILE"], logger)
    
    # quit: problem in the input data file
    if sites == -1:
        os._exit(1)
        
    # Date source: CNPE Nogent-sur_seine => 4 devices = > 12 sensors
    site = sites[0]
    
    logger.debug(f"producer_iot.main - Site ID {site["siteId"]}: Printing Complete site record")
    pp_json(site, logger)
    
    # Start ESP: Kafka   
    logger.info(f"producer_iot.main - Site ID {site["siteId"]}: Starding ESP Kafka")
        
    while True:
        current_time = datetime.now()

        for device in site["devices"]:

            for sensor in device["sensors"]:
                # new event
                message = simulate_iot(site["name"], device, sensor, current_time, logger)            
        
                # send event to topic
                produce_message(IoT_topic, message, logger)
                
        logger.info(f"producer_iot.main - Site ID {site["siteId"]}: IoT data generation is in progress...")
        logger.info("To stop press Ctrl+C")
        logger.info("-----")
        
        # Define collection frequecy
        time.sleep(site["sleeptime"] / 1000)
        
    # logger.info(f"Completed run, logfile => {params["LOGGING_FILE"]}")      
    
    