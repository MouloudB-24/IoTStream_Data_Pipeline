#####################################################################
#
# Project       : Smart Irrigation Optimization platform
#
# File          : logger.py
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

import logging
import os
from conf import config


def logger(filepath: str, console_debug_level: int, file_debug_level: int):
    
    """function allowing to configure the logger"""
        
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
    fh = logging.FileHandler(filepath)
    
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
   