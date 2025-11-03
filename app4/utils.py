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
import json
import sys


def logger(filename, console_debuglevel, file_debuglevel):
    
    """Common generic logger stup, used to display information in the console and the logging file"""
    
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # Create a formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    # Create a console handler
    ch = logging.StreamHandler()
    
    # Set file log level
    if console_debuglevel == 0:
        ch.setLevel(logging.DEBUG)
    
    elif console_debuglevel == 1:
        ch.setLevel(logging.INFO)
    
    elif console_debuglevel == 2:
        ch.setLevel(logging.WARNING)
        
    elif console_debuglevel == 3:
        ch.setLevel(logging.ERROR)
        
    elif console_debuglevel == 4:
        ch.setLevel(logging.CRITICAL)
    
    ch.setFormatter(formatter)
    logger.addHandler(ch) 
    
    # Create log file
    fh = logging.FileHandler(filename)
    
    # Set file log level
    if file_debuglevel == 0:
        fh.setLevel(logging.DEBUG)
    
    elif file_debuglevel == 1:
        fh.setLevel(logging.INFO)
        
    elif file_debuglevel == 2:
        fh.setLevel(logging.WARNING)
    
    elif file_debuglevel == 3:
        fh.setLevel(logging.ERROR)
    
    elif file_debuglevel == 4:
        fh.setLevel(logging.CRITICAL)
    
    else:
        fh.setLevel(logging.INFO) # Default log leve if undefined
    
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
    
    
def configparams():
    
    """Retreive Common Config Pamameters in a dictionary format"""
    
    config_params = {}
    
    # General parameters
    config_params["CONSOLE_DEBUGLEVEL"] = int(os.environ["CONSOLE_DEBUGLEVEL"])
    config_params["FILE_DEBUGLEVEL"] = int(os.environ["FILE_DEBUGLEVEL"])
    
    config_params["LOGGINGFILE"] = os.environ["LOGGINGFILE"]
    config_params["SEEDFILE"] = os.environ["SEEDFILE"]
    config_params["SITEIDS"] = os.environ["SITEIDS"].split(",")
    
    
    # Mongo parameters
    config_params["MONGO_HOST"] = os.environ["MONGO_HOST"]
    config_params["MONGO_PORT"] = os.environ["MONGO_PORT"]
    config_params["MONGO_DIRECT"] = os.environ["MONGO_DIRECT"]
    config_params["MONGO_USERNAME"] = os.environ["MONGO_USERNAME"]
    config_params["MONGO_PASSWORD"] = os.environ["MONGO_PASSWORD"]
    config_params["MONGO_DATASTORE"] = os.environ["MONGO_DATASTORE"]
    config_params["MONGO_COLLECTION"] = os.environ["MONGO_COLLECTION"]
    
    return config_params


def echo_config(config_params, logger):
    logger.info("*"*65)
    logger.info("* ")
    logger.info("*          Python IoT Sensor data generator")
    logger.info("* ")
    logger.info("*"*65)
    logger.info("* General")
    
    logger.info("* Console Debuglevel       : "+ str(config_params["CONSOLE_DEBUGLEVEL"])) 
    logger.info("* File Debuglevel          : "+ str(config_params["FILE_DEBUGLEVEL"]))
        
    logger.info("* Logfile                  : "+ config_params["LOGGINGFILE"])
    logger.info("* Seedfile                 : "+ config_params["SEEDFILE"])
    logger.info("* SiteId's                 : "+ str(config_params["SITEIDS"]))
    
    logger.info("* Mongo Host               : " + config_params["MONGO_HOST"])
    logger.info("* Mongo Port               : " + config_params["MONGO_PORT"])
    logger.info("* Mongo DataStore          : " + config_params["MONGO_DATASTORE"])
    logger.info("* Mongo Collection         : " + config_params["MONGO_COLLECTION"])
    
    logger.info("*"*65)     
    logger.info("")


def pp_json(json_thing, logger, sort=False, indents=4):
    
    """Console print: display site data only when logging level = debug"""
    
    if type(json_thing) is str:
        logger.debug(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    
    else:
        logger.debug(json.dumps(json_thing, sort_keys=sort, indent=indents))


def read_seed_file(filename, logger):
    
    """Lets read entire seed file in"""
    
    my_seedfile = []

    logger.info("utils.read_seed_file Called ")

    logger.info(f"utils.read_seed_file Loading file: {filename}")

    try:
        with open(filename, "r") as f:
            my_seedfile = json.load(f)

    except IOError as e:
        logger.critical(f"utils.read_seed_file I/O error: {filename}, {e.errno}, {e.strerror}")
        return -1

    except:  # handle other exceptions such as attribute errors
        logger.critical(f"utils.read_file Unexpected error: {filename}, {sys.exc_info()[1]}")
        return -1

    finally:
        logger.debug("utils.read_seed_file Printing Seed file")
        
        # pp_json only prints diring debug level due to the volume of information
        pp_json(my_seedfile, logger)

        logger.info("utils.read_seed_file Completed ")

    return my_seedfile


def find_site(my_seedfile, siteId, logger):
    
    """Find the specific site in array of sites based on siteId"""
    
    logger.info(f"utils.read_site Called, SiteId: {siteId}")
    
    site = None
    found = False
    
    for site in my_seedfile:
        if site["siteId"] == siteId:
            
            logger.info(f"utils.find_site Retreived, SiteId: {siteId}")
            
            return site
    
    if not found:
        
        logger.critical(f"utils.find_site Completed, SiteId: {siteId} NOT FOUND")
        
        return -1
    