#####################################################################
#
# Project       : IoT based TimesSeries Data via Python Application
#
# File          : main.py
#
# Description   : Program allowing reading site data from conf/Full2.json file
#
# Created       : 31 october 2025
#
# Author        : Mouloud BELLIL
#
# Email         : mouloud.bellil@outlook.fr
#
#######################################################################

import os
import utils


config_params = utils.configparams()
logger = utils.logger(config_params)


def main():

    if config_params["ECHOCONFIG"] == 1:
        utils.echo_config(config_params, logger)

    # Load the entire SeedFile
    my_seedfile = utils.read_seed_file(config_params["SEEDFILE"], logger, config_params)
    if my_seedfile == -1:
        os._exit(1)

    # Load/filter out the site data based on SITEIDS environment variable
    my_sites = []
    for siteId in config_params["SITEIDS"]:

        cur_site = utils.find_site(my_seedfile, int(siteId), logger, config_params)
        if cur_site == -1:
            os._exit(1)

        my_sites.append(cur_site)


if __name__ == "__main__":
    main()
