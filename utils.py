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
from dotenv import load_dotenv

import config

# Load environment variables
load_dotenv()


def logger(filename, console_debug_level, file_debug_level):
    
    """Common generic logger stup, used to display information in the console and the logging file"""
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # Create a formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    # Create a console handler
    ch = logging.StreamHandler()
    
    # Set file log level
    if console_debug_level == 0:
        ch.setLevel(logging.DEBUG)
    
    elif console_debug_level == 1:
        ch.setLevel(logging.INFO)
    
    elif console_debug_level == 2:
        ch.setLevel(logging.WARNING)
        
    elif console_debug_level == 3:
        ch.setLevel(logging.ERROR)
        
    elif console_debug_level == 4:
        ch.setLevel(logging.CRITICAL)
    
    else:
        ch.setLevel(logging.INFO) # Default log leve if undefined
    
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    # Create log file
    fh = logging.FileHandler(filename)
    
    # Set file log level
    if file_debug_level == 0:
        fh.setLevel(logging.DEBUG)
    
    elif file_debug_level == 1:
        fh.setLevel(logging.INFO)
        
    elif file_debug_level == 2:
        fh.setLevel(logging.WARNING)
    
    elif file_debug_level == 3:
        fh.setLevel(logging.ERROR)
    
    elif file_debug_level == 4:
        fh.setLevel(logging.CRITICAL)
    
    else:
        fh.setLevel(logging.INFO) # Default log leve if undefined
    
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
    
    
def config_params():
    
    """Retreive Common Config Pamameters in a dictionary format"""
    
    return {
        # General parameters
        "CONSOLE_DEBUG_LEVEL": config.CONSOLE_DEBUG_LEVEL,
        "FILE_DEBUG_LEVEL": config.FILE_DEBUG_LEVEL,
        
        "LOGGING_FILE": config.LOGGING_FILE,
        "INPUT_DATA_FILE": config.INPUT_DATA_FILE,
        "SITE_IDS": config.SITE_IDS,
        
        # Mongo parameters
        "MONGO_USERNAME": os.environ["MONGO_USERNAME"],
        "MONGO_PASSWORD": os.environ["MONGO_PASSWORD"],
        "MONGO_DATABASE": os.environ["MONGO_DATABASE"],
        "MONGO_COLLECTION": os.environ["MONGO_COLLECTION"]
    }
   

def echo_config(params, logger):
    logger.info("*"*70)
    logger.info("* ")
    logger.info("*          IoT Stream Data Pipeline    ")
    logger.info("* ")
    logger.info("*"*70)
    logger.info("* General")
    
    logger.info("* Console Debuglevel       : "+ str(params["CONSOLE_DEBUG_LEVEL"])) 
    logger.info("* File Debuglevel          : "+ str(params["FILE_DEBUG_LEVEL"]))
        
    logger.info("* Logging file             : "+ params["LOGGING_FILE"])
    logger.info("* Input data file          : "+ params["INPUT_DATA_FILE"])
    logger.info("* SiteId's                 : "+ str(params["SITE_IDS"]))
    
    logger.info("* Mongo Database          : " + params["MONGO_DATABASE"])
    logger.info("* Mongo Collection         : " + params["MONGO_COLLECTION"])
    
    logger.info("*"*70)     
    logger.info("")


def pp_json(json_thing, logger, sort=False, indents=4):
    
    """Console print: display site data only when logging level = debug"""
    
    if type(json_thing) is str:
        logger.debug(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    
    else:
        logger.debug(json.dumps(json_thing, sort_keys=sort, indent=indents))


def read_input_data(filename, logger):
    
    """Lets read entire seed file in"""
    
    sites = []

    logger.info(f"utils.read_input_data Loading file: {filename}")

    try:
        with open(filename, "r") as f:
            sites = json.load(f)

    except IOError as e:
        logger.critical(f"utils.read_input_data I/O error: {filename}: {e}")
        return -1

    except:  # handle other exceptions such as attribute errors
        logger.critical(f"utils.read_file Unexpected error: {filename}, {sys.exc_info()[1]}")
        return -1

    return sites


def find_site(sites, siteId, logger):
    
    """Find the specific site in array of sites based on siteId"""
    
    logger.info(f"utils.read_site Called, SiteId: {siteId}")
    
    site = None
    found = False
    
    for site in sites:
        if site["siteId"] == siteId:
            
            logger.info(f"utils.find_site Retreived, SiteId: {siteId}")
            
            return site
    
    if not found:
        
        logger.critical(f"utils.find_site Completed, SiteId: {siteId} NOT FOUND")
        
        return -1
    