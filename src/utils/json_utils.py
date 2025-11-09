#####################################################################
#
# Project       : Smart Irrigation Optimization Platfom
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

import json
import sys


def pp_json(json_thing, logger, sort=False, indents=4):
    
    """Console print: display site data only when logging level = debug"""
    
    if type(json_thing) is str:
        logger.debug(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))
    
    else:
        logger.debug(json.dumps(json_thing, sort_keys=sort, indent=indents))


def read_input_data(filepath, logger):
    
    """Lets read entire seed file in"""
    
    sites = []

    logger.info(f"utils.read_input_data Loading file: {filepath}")

    try:
        with open(filepath, "r") as f:
            sites = json.load(f)

    except IOError as e:
        logger.critical(f"utils.read_input_data I/O error: {filepath}: {e}")
        return -1

    except:  # handle other exceptions such as attribute errors
        logger.critical(f"utils.read_file Unexpected error: {filepath}, {sys.exc_info()[1]}")
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
    