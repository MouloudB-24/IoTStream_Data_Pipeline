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
from datetime import datetime, timedelta

import utils
import connection


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
        "matedata": {
            "siteId": site_id,
            "deviceId": device["deviceId"],
            "sensorId": sensor["sensorId"],
            "unit": sensor["unit"]
        },
        "measurement": measurement
    }
    

def run_simulation(site, current_time, params):
    
    """Function to run simulation for a specific site"""
    
    # Create new site specific logger's
    logger = utils.logger(params["LOGGING_FILE"] + "_" + str(site["siteId"]) + ".log", params["CONSOLE_DEBUG_LEVEL"], params["FILE_DEBUG_LEVEL"])
    
    logger.info(f"simulate.run_simulation - Site ID {site["siteId"]}: Starding Simulation")

    logger.debug(f"simulation.run_simulation - Site ID {site["siteId"]}: Printing Complete site record")
    utils.pp_json(site, logger)
    
    batch_flush_counter = 0
    total_record_counter = 0
     
    # MongoDB persistence
    mongodb_collection = connection.create_mongo_connection(params, site["siteId"], logger)
    
    if mongodb_collection == -1:
        logger.critical(f"simulate.run_simulation - Site ID {site["siteId"]} EXITING")
        os._exit(1)
            
    # Current Phase
    if site["reccap"] > 0:
        logger.info(f"simulate.run_simulation - Site ID {site["siteId"]}: Running current phase")
        
        my_docs = []
        
        for loop in range(site["reccap"]):
            current_loop_time = current_time + timedelta(milliseconds=site["sleeptime"] * loop)
            
            for device in site["devices"]:
                for sensor in device["sensors"]:
                    payload = generate_payload(sensor, device, site["siteId"], current_loop_time)
                      
                    batch_flush_counter += 1
                    total_record_counter += 1
                
                    my_docs.append(payload)
                    
                    if batch_flush_counter == site["flush_size"]:
                        connection.insert_mongo(mongodb_collection, site["siteId"], my_docs, logger)

                        logger.debug(f"Adding {batch_flush_counter} to {total_record_counter}")
                        
                        # Reset flush counter
                        batch_flush_counter = 0
                        my_docs = []    
                    
                    logger.debug(f"simulate.run_simulation SiteId {site["siteId"]} - Current Phase: Payload {payload}")
                    
                    sensor["last_value"] = payload["measurement"]

            time.sleep(site["sleeptime"] / 1000)
        
        logger.info(f"simulate.run_simulation - Site ID {site["siteId"]}: Completed current phase")
    
    logger.info(f"simulation.run_simulation - Site ID {site["siteId"]}: Completed simulation")
    
    
    