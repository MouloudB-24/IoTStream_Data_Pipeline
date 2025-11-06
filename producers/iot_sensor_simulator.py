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

import random
from datetime import datetime


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
    

def simulate_iot_event(sensor, device, site_id, current_time, logger):
    
    """Function to generate payloads with timezone in the timestamp"""
    
    # Generate the timestamp
    timestamp = current_time.isoformat()
    
    # Generate the sonsor measure
    measure = generate_value(sensor, datetime.now())
    
    event = {
        "timestamp": timestamp,
        "metadata": {
            "siteId": site_id,
            "deviceId": device["deviceId"],
            "sensorId": sensor["sensorId"],
            "unit": sensor["unit"]
        },
        "measure": measure
    }
    
    logger.debug(f"iot_sensor_simulator.simulate_iot_event - Device ID {device["deviceId"]}: event {event}")
    
    return event
