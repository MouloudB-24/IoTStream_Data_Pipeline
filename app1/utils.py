#####################################################################
#
# Project       : IoT based TimesSeries Data via Python Application
#
# File          : utils.py
#
# Description   : Some common utility functions
#
# Created       : 30 october 2025
#
# Author        : Mouloud BELLIL
# 
# Email         : mouloud.bellil@outlook.fr
# 
#######################################################################

import os
import logging


def configparams():
    
    config_params = {}
    
    # General
    config_params["DEBUGLEVEL"] = int(os.environ["DEBUGLEVEL"])
    config_params["ECHOCONFIG"] = int(os.environ["ECHOCONFIG"])
    config_params["ECHORECORDS"] = int(os.environ["ECHORECORDS"])
    config_params["LOGGINGFILE"] = os.environ["LOGGINGFILE"]
    
    return config_params


def logger(config_params):
    
    # Logging Handler
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # create formatter and add it to the handlers
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    # create console handler with higher log level
    ch = logging.StreamHandler() # display logs in the console during execution
    fh = logging.FileHandler(config_params["LOGGINGFILE"])
    
    if config_params["DEBUGLEVEL"] == 0:
        ch.setLevel(logging.DEBUG)
        fh.setLevel(logging.DEBUG)
    
    elif config_params["DEBUGLEVEL"] == 1:
        ch.setLevel(logging.INFO)
        fh.setLevel(logging.INFO)
    
    elif config_params["DEBUGLEVEL"] == 2:
        ch.setLevel(logging.WARNING)
        fh.setLevel(logging.WARNING)
    
    elif config_params["DEBUGLEVEL"] == 3:
        ch.setLevel(logging.ERROR)
        fh.setLevel(logging.ERROR)
    
    elif config_params["DEBUGLEVEL"] == 4:
        ch.setLevel(logging.CRITICAL)
        fh.setLevel(logging.CRITICAL)
    
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger


def echo_config(config_params, logger):
    logger.info("*"*50)
    logger.info("* ")
    logger.info("*      Python IoT Sensor data generator")
    logger.info("* ")
    logger.info("*"*50)
    logger.info("* General")
    logger.info("* DebugLevel       :"+ str(config_params["DEBUGLEVEL"]))
    logger.info("* EchoConfig       :"+ str(config_params["ECHOCONFIG"]))
    logger.info("* EchoRecords      :"+ str(config_params["ECHORECORDS"]))
    
    logger.info("* Logfile          :"+ str(config_params["LOGGINGFILE"]))
    
    logger.info("*"*50)
    logger.info("")
    