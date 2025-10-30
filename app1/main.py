#####################################################################
#
# Project       : IoT based TimesSeries Data via Python Application
#
# File          : main.py
#
# Description   : Program showing how to set configuration variables, use the console, and manage log file output
#
# Created       : 30 october 2025
#
# Author        : Mouloud BELLIL
# 
# Email         : mouloud.bellil@outlook.fr
# 
#######################################################################

import utils


config_params = utils.configparams()
logger = utils.logger(config_params)


def main():
    
    if config_params["ECHOCONFIG"] == 1:
        utils.echo_config(config_params, logger)
    
    logger.debug("Hello, This a Debug (level=0) message with no variables added")
    
    # Quick example of how we can print a info message to the console, no added variables
    logger.info("Hello, This is Info (level=1) message with no variables added")

    # same as above but we now add a variable to include/append to our text
    logger.info(f"Hello, This is a Info (level=1) message with a single variable: {config_params['LOGGINGFILE']}")

    # Quick example of how we can print a warning message to console
    logger.warning(f"Hello, this is a Warning (level=2) message with a single variable : {config_params['LOGGINGFILE']}")
    
    # Quick example of how we can print a error message to console
    logger.error(f"Hello, this is a Error (level=3) message with a single variable : {config_params['LOGGINGFILE']}")
    
    # Quick example of how we can print a critical message to console
    logger.critical(f"Hello, this is a Critical (level=4) message with a single variable : {config_params['LOGGINGFILE']}")
    
    
if __name__ == "__main__":
    main()