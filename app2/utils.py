#####################################################################
#
# Project       : IoT based TimesSeries Data via Python Application
#
# File          : utils.py
#
# Description   : Some common utility functions
#
# Created       : 31 october 2025
#
# Author        : Mouloud BELLIL
#
# Email         : mouloud.bellil@outlook.fr
#
#######################################################################

import os
import sys
import json
import logging


def logger(config_params):

    # Logging handler
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # create the formatter and add it to the handlers
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # create console handler with higher level
    ch = logging.StreamHandler()
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


def configparams():

    config_params = {}

    config_params["DEBUGLEVEL"] = int(os.environ["DEBUGLEVEL"])
    config_params["ECHOCONFIG"] = int(os.environ["ECHOCONFIG"])
    config_params["ECHORECORDS"] = int(os.environ["ECHORECORDS"])
    config_params["ECHOSEEDFILE"] = int(os.environ["ECHOSEEDFILE"])
    config_params["ECHOSITEFILE"] = int(os.environ["ECHOSITEFILE"])

    config_params["LOGGINGFILE"] = os.environ["LOGGINGFILE"]
    config_params["SEEDFILE"] = os.environ["SEEDFILE"]
    config_params["SITEIDS"] = os.environ["SITEIDS"].split(",")

    return config_params


def echo_config(config_params, logger):

    logger.info("*" * 50)
    logger.info("* ")
    logger.info("*          Python IoT Sensor data generator")
    logger.info("* ")
    logger.info("*" * 50)
    logger.info("* General")

    logger.info("* Debuglevel                : " + str(config_params["DEBUGLEVEL"]))
    logger.info("* EchoConfig                : " + str(config_params["ECHOCONFIG"]))
    logger.info("* EchoRecords               : " + str(config_params["ECHORECORDS"]))
    logger.info("* Echo Seedfile JSON        : " + str(config_params["ECHOSEEDFILE"]))
    logger.info("* Echo Site definition JSON : " + str(config_params["ECHOSITEFILE"]))

    logger.info("* Logfile                   : " + str(config_params["LOGGINGFILE"]))
    logger.info("* Seedfile                  : " + str(config_params["SEEDFILE"]))
    logger.info("* SiteId's                  : " + str(config_params["SITEIDS"]))

    logger.info("*" * 50)
    logger.info("")


def pp_json(json_thing, sort=False, indents=4):
    """Display sites ordered from the json file"""

    if type(json_thing) is str:
        print(json.dumps(json.loads(json_thing), sort_keys=sort, indent=indents))

    else:
        print(json.dumps(json_thing, sort_keys=sort, indent=indents))

    return None


def read_seed_file(filename, logger, config_params):
    """Retreive the site into a list form the json file"""

    my_seedfile = []

    if config_params["DEBUGLEVEL"] >= 1:

        logger.info("utils.read_seed_file Called ")

        logger.info(f"utils.read_seed_file Loading file: {filename}")

    try:
        with open(filename, "r") as f:
            my_seedfile = json.load(f)

    except IOError as e:
        logger.error(
            f"utils.read_seed_file I/O error: {filename}, {e.errno}, {e.strerror}"
        )
        return -1

    except:  # handle other exceptions such as attribute errors
        logger.error(
            f"utils.read_file Unexpected error: {filename}, {sys.exc_info()[1]}"
        )
        return -1

    finally:
        if config_params["ECHOSEEDFILE"] == 1:
            logger.info("utils.read_seed_file Printing Seed file")

            print("-" * 30, "     Seedfile     ", "-" * 30)

            pp_json(my_seedfile)

            print("-" * 80)

        if config_params["DEBUGLEVEL"] >= 1:
            logger.info("utils.read_seed_file Completed ")

    return my_seedfile


def find_site(my_seedfile, siteId, logger, config_params):

    if config_params["DEBUGLEVEL"] >= 1:
        logger.info("utils.find_site Called")

    site = None
    found = False

    for site in my_seedfile:
        if site["siteId"] == siteId:

            if config_params["ECHOSITEFILE"] == 1:
                logger.info(f"utils.find_site Printing, SideId: {siteId}")

                pp_json(site)

            if config_params["DEBUGLEVEL"] >= 1:
                logger.info(f"utils.find_site Completed, SideId: {siteId}")
                print("\n")

            return site

    if not found:

        if config_params["DEBUGLEVEL"] >= 1:
            logger.info(f"utils.find_site Completed, SiteId: {siteId} NOT FOUND")

        return -1
