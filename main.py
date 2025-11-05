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
from datetime import datetime
from multiprocessing import Process

from simulate import run_simulation
from utils import config_params, logger, echo_config, read_input_data


def main():
    
    """_summary_"""
    
    run_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    params = config_params()
    params["LOGGING_FILE"] = params["LOGGING_FILE"] + "_" + str(params["SITE_IDS"])+ "_" + run_time
    logger_ = logger(params["LOGGING_FILE"] + ".log", params["CONSOLE_DEBUG_LEVEL"], params["FILE_DEBUG_LEVEL"])
    
    logger_.info(f"Starding run, logfile => {params["LOGGING_FILE"]}")
    
    echo_config(params, logger_)
    
    # Load data site
    sites = read_input_data(params["INPUT_DATA_FILE"], logger_)
    
    if sites == -1:
        os._exit(1) # quit: problem in the input data file

    processes = []
    
    try:
        for site in sites:
            logger_.info(f"Calling simulate.run_simulation for SiteId: {site["siteId"]}")
            
            p = Process(target=run_simulation, args=(site, params, logger_))
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
            
    except KeyboardInterrupt:
        logger_.warning("KeyboardInterrupt detected! Terminating all processes...")
        
        # Terminate all processes if Ctrl+C si pressed
        for p in processes:
            p.terminate()
            p.join()
        
        logger_.info("All processes terminated. Exiting gracefully...")
        
    
    logger_.info(f"Completed run, logfile => {params["LOGGING_FILE"]}")      
     
    
if __name__ == "__main__":
    main()