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

from datetime import datetime
from multiprocessing import Process

from producers import producer_iot
from consumers import consumer_main
from utils import config_params, logger, echo_config


def main():
    
    """_summary_"""
    
    run_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    params = config_params()
    params["LOGGING_FILE"] = params["LOGGING_FILE"] + "_" + str(params["SITE_IDS"])+ "_" + run_time
    
    logger_ = logger(params["LOGGING_FILE"] + ".log", params["CONSOLE_DEBUG_LEVEL"], params["FILE_DEBUG_LEVEL"])
    logger_.info(f"Starding run, logfile => {params["LOGGING_FILE"]}")
    
    echo_config(params, logger_)
    
    try:
        p1 = Process(target=producer_iot.main, args=(params, logger_))
        p2 = Process(target=consumer_main.main, args=(params, logger_))

        # Start process
        p1.start()
        p2.start()

        # Wait process to finish
        p1.join()
        p2.join()
    
    except KeyboardInterrupt:
        logger_.warning("KeyboardInterrupt detected! Terminating all processes...")
        
        # Termnate all processes is Ctrl+C is pressed
        p1.terminate()
        p1.join()
        p2.terminate()
        p2.join()
        
        logger_.info("All processes terminated. Exiting gracefuly!")
     
    
if __name__ == "__main__":
    main()