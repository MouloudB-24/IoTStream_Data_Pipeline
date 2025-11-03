#####################################################################
#
# Project       : IoT based TimesSeries Data via Python Application
#
# File          : main.py
#
# Description   : Implementation of the sensor data generation process
#
# Created       : 1 November 2025
#
# Author        : Mouloud BELLIL
#
# Email         : mouloud.bellil@outlook.fr
#
#######################################################################

import os
import utils
import simulate
from datetime import datetime
import multiprocessing


def main():
    
    """_summary_"""
    
    runTime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    config_params = utils.configparams()
    config_params["LOGGINGFILE"] = config_params["LOGGINGFILE"] + "_" + runTime
    logger = utils.logger(config_params["LOGGINGFILE"] + "_common.log",
                          config_params["CONSOLE_DEBUGLEVEL"],
                          config_params["FILE_DEBUGLEVEL"])
    
    logger.info(f"Starding run, logfile => {config_params["LOGGINGFILE"]}")
    
    utils.echo_config(config_params, logger)
    
    # Load the entire SeedFile
    my_seedfile = utils.read_seed_file(config_params["SEEDFILE"], logger)
    if my_seedfile == -1:
        os._exit(1) # quit: problem in the seed file
    
    # Load/filtred out the site data based on SITEIDS environment variable
    my_sites = []
    for siteId in config_params["SITEIDS"]:
        cur_site = utils.find_site(my_seedfile, int(siteId), logger)
        if cur_site == -1:
            os._exit(1) # quit: siteId was not found
        
        my_sites.append(cur_site)


    current_time = datetime.now()
    
    # Create processs for each site
    processes = []
    for site in my_sites:
        logger.info(f"Calling simulate.run_simulation for SiteId: {site["siteId"]}")
        
        p = multiprocessing.Process(target=simulate.run_simulation, args=(site, current_time, config_params))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    
    logger.info(f"Completed run, logfile => {config_params["LOGGINGFILE"]}")
   
    
if __name__ == "__main__":
    main()