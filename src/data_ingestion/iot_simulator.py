#####################################################################
#
# Project       : Smart Irrigation Optimization Platfom
#
# File          : iot_simulate.py
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


def generate_value(sensor: dict, method: str="normal") -> float:
    """function that generatsz a random value according to a probability distribution

    _summary_Args:
        sensor (dict): a dict descripting the sensor
        method (str, optional): probability distribution used to generate a random value. Defaults to "normal"

    Returns:
        float: generated measurement
    """
    
    sd = sensor["sd"]
    mean = sensor["mean"]
    
    # Generate sensor value using normal or uniform distribution
    if method == "uniform":
        lowed_bound = max(0, mean - sd)
        upper_bound = mean + sd
        return round(random.uniform(lowed_bound, upper_bound), 4)
    
    return round(random.gauss(mean, sd), 4)
    

def simulate_iot(site_name: str, device: dict, sensor: dict, current_time: datetime, logger) -> dict:
    """function fot generating random IoT data

    Args:
        sensor (dict): sensor information
        device (dict): device information
        site_name (str): site name
        current_time (datetime): current time
        logger (function): logging

    Returns:
        dict: randomly generated data
    """
    
    
    # Generate the timestamp
    timestamp = current_time.isoformat()
    
    # Generate the sonsor measure
    measure = generate_value(sensor)
    
    event = {
        "timestamp": timestamp,
        "metadata": {
            "siteId": site_name,
            "deviceType": device["deviceType"],
            "sensorType": sensor["sensorType"],
            "unit": sensor["unit"]
        },
        "measure": measure
    }
    
    logger.debug(f"iot_simulator.simulate_iot - Device Type {device["deviceType"]}: {event}")
    
    return event
