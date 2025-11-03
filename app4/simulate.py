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
from datetime import datetime, timedelta, timezone

import utils
import connection


def is_whithin_time_range(start_time_str, end_time_str, current_time=None):
    
    """Function to check if the current time is whithin the specified time range for the site"""

    start_time = datetime.strptime(start_time_str, "%H:%M")
    end_time = datetime.strptime(end_time_str, "%H:%M")
    
    if not current_time:
        current_time = datetime.now()
    
    return start_time <= current_time <= end_time


def progress_value(sensor, stability_factor, device, current_time, method="normal"):
    
    """
    Generate a sensor value
    Scaling ecart_type(sd) based on stability_factor if the current time is whithin device start_time and end_time
    If start_time and end_time are not provided for the device, no scaling is applied
    """
    
    sd = sensor["sd"]
    mean = sensor["mean"]
    
    # Check if device has start_time and end_time, and scale sd only they are procided
    if "start_time" in device and "end_time" in device:
        device_start_time = datetime.strptime(device["start_time"], "%H:%M")
        device_end_time  = datetime.strptime(device["end_time"], "%H:%M")
        current_time_local = current_time.time() # retreive only time portion
        
        if device_start_time.time() <= current_time_local <= device_end_time.time():
            # Scale standard deviation if whithin the specified time range
            sd = sd * (100 - stability_factor) / 100
            
    # Generate sensor value using normal or uniform distribution
    if method == "normal":
        return round(random.gauss(mean, sd), 4)
    
    elif method == "uniform":
        lowed_bound = max(0, mean - sd)
        upper_bound = mean + sd
        
        return round(random.uniform(lowed_bound, upper_bound), 4)
    

def generate_payload(sensor, device, site_id, current_time, time_zone_offset):
    
    """Function to generate payloads with timezone in the timestamp"""
    
    # Adjust the timestamp to include the site's local timezone offset
    offset_hours, offset_minutes = map(int, time_zone_offset.split(":"))
    tz_offset = timezone(timedelta(hours=offset_hours, minutes=offset_minutes))
    local_time_with_tz = current_time.replace(tzinfo=tz_offset)
    
    # Generate the timestamp with timezone included
    timestamp = local_time_with_tz.isoformat()
    
    # Generate the sonsor measurement
    measurement = progress_value(sensor, device["stabilityFactor"], device, datetime.now())
    
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
    

def run_simulation(site, current_time, config_params):
    
    """Function to run simulation for a specific site"""
    
    # Create new site specific logger's
    logger = utils.logger(config_params["LOGGINGFILE"] + "_" + str(site["siteId"]) + ".log", config_params["CONSOLE_DEBUGLEVEL"], config_params["FILE_DEBUGLEVEL"])
    
    logger.info(f"simulate.run_simulation - Site ID {site["siteId"]}: Starding Simulation")

    logger.debug(f"simulation.run_simulation - Site ID {site["siteId"]}: Printing Complete site record")
    utils.pp_json(site, logger)
    
    # Site's time zone offset
    site_time_zone = site.get("time_zone", 0)
        
    # Parse the start_datetime and begin simulation
    if "start_datetime" in site and site["start_datetime"]:
        oldest_time = datetime.strptime(site["start_datetime"], "%Y-%m-%dT%H:%M")
    else:
        oldest_time = current_time
    
    batch_flush_counter = 0
    total_record_counter = 0
     
    # MongoDB persistence
    if site["data_persistence"] == 2:
        flush_size = site["flush_size"]
        
        if flush_size > 1:
            my_docs = []
            mode = 2
        
        else:
            mode = 1
        
        mongodb_collection = connection.create_mongo_connection(config_params, site["siteId"], logger)
        
        if mongodb_collection == -1:
            logger.critical(f"simulate.run_simulation - Site ID {site["siteId"]} EXITING")
            os._exit(1)
         
    
    # Historical phase
    if "start_datetime" in site and site["start_datetime"]:
        logger.info(f"simulate.run_simulation - Site ID {site["siteId"]} Running historical phase starting from {site["start_datetime"]}")
        
        while oldest_time < current_time:
            for device in site["devices"]:
                for sensor in device["sensors"]:
                    payload = generate_payload(sensor, device, site["siteId"], oldest_time, site_time_zone)
                    
                    batch_flush_counter += 1
                    total_record_counter += 1
                    
                    if site["data_persistence"] == 2:
                        if mode == 1:
                            result = connection.insert_mongo(mongodb_collection, site["siteId"], mode, payload, logger)
                            batch_flush_counter = 0
                        
                        else: # mode =2
                            my_docs.append(payload)
                            
                            if batch_flush_counter == flush_size:
                                result = connection.insert_mongo(mongodb_collection, site["siteId"], mode, my_docs, logger)
                                logger.debug(f"Adding {batch_flush_counter} to {total_record_counter}")
                            
                            # Reset flush counter
                            batch_flush_counter = 0
                            my_docs = []    
                            
                    logger.debug(f"simulate.run_simulation SiteId {site["siteId"]} - Hist Ph: Payload {payload}")
                    
                    sensor["last_value"] = payload["measurement"]
                
            oldest_time += timedelta(milliseconds=site["sleeptime"])
        
        logger.info(f"simulation.run_simulation - Site ID {site["siteId"]}: Completed historical phase from {site["start_datetime"]}")
    
    
    # Current Phase
    if site["reccap"] > 0:
        logger.info(f"simulate.run_simulation - Site ID {site["siteId"]}: Running current phase")
        
        for loop in range(site["reccap"]):
            current_loop_time = oldest_time + timedelta(milliseconds=site["sleeptime"] * loop)
            
            for device in site["devices"]:
                for sensor in device["sensors"]:
                    payload = generate_payload(sensor, device, site["siteId"], current_loop_time, site_time_zone)
                      
                    batch_flush_counter += 1
                    total_record_counter += 1
                    
                    if site["data_persistence"] == 2:
                        if mode == 1:
                            result = connection.insert_mongo(mongodb_collection, site["siteId"], mode, payload, logger)
                            batch_flush_counter = 0
                        
                        else: # mode =2
                            my_docs.append(payload)
                            
                            if batch_flush_counter == flush_size:
                                result = connection.insert_mongo(mongodb_collection, site["siteId"], mode, my_docs, logger)

                                logger.debug(f"Adding {batch_flush_counter} to {total_record_counter}")
                                
                                # Reset flush counter
                                batch_flush_counter = 0
                                my_docs = []    
                    
                    logger.debug(f"simulate.run_simulation SiteId {site["siteId"]} - Current Phase: Payload {payload}")
                    
                    sensor["last_value"] = payload["measurement"]

            time.sleep(site["sleeptime"] / 1000)
        
        logger.info(f"simulate.run_simulation - Site ID {site["siteId"]}: Completed current phase")
    
    logger.info(f"simulation.run_simulation - Site ID {site["siteId"]}: Completed simulation")
    
    
    