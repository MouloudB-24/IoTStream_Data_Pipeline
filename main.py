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
    
    run_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    params = utils.config_params()
    params["LOGGING_FILE"] = params["LOGGING_FILE"] + "_" + run_time
    logger = utils.logger(params["LOGGING_FILE"] + "_common.log", params["CONSOLE_DEBUG_LEVEL"], params["FILE_DEBUG_LEVEL"])
    
    logger.info(f"Starding run, logfile => {params["LOGGING_FILE"]}")
    
    utils.echo_config(params, logger)
    
    # Load data site
    sites = utils.read_input_data(params["INPUT_DATA_FILE"], logger)
    if sites == -1:
        os._exit(1) # quit: problem in the input data file

    current_time = datetime.now()
    
    # Create processs for each site
    processes = []
    
    try:
        for site in sites:
            logger.info(f"Calling simulate.run_simulation for SiteId: {site["siteId"]}")
            
            p = multiprocessing.Process(target=simulate.run_simulation, args=(site, current_time, params))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
            
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt detected! Terminating all processes...")
        
        # Terminate all processes if Ctrl+C si pressed
        for p in processes:
            p.terminate()
            p.join()
        
        logger.info("All processes terminated. Exiting gracefully...")
        
    
    logger.info(f"Completed run, logfile => {params["LOGGING_FILE"]}")
   
    
if __name__ == "__main__":
    main()