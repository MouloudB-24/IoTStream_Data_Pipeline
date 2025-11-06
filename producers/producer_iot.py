
from datetime import datetime
import os
import time

from kafka_utils.kafka_producer import produce_iot_message
from producers.iot_sensor_simulator import simulate_iot_event
from utils import pp_json, read_input_data

from config import IoT_topic


def main(params, logger):
    
    """_summary_"""
    
    # Load data site
    sites = read_input_data(params["INPUT_DATA_FILE"], logger)
    
    # quit: problem in the input data file
    if sites == -1:
        os._exit(1)
        
    # Date source: CNPE Nogent-sur_seine => 4 devices = > 12 sensors
    site = sites[0]
    
    logger.debug(f"producer_iot.main - Site ID {site["siteId"]}: Printing Complete site record")
    pp_json(site, logger)
    
    # Start ESP: Kafka   
    logger.info(f"producer_iot.main - Site ID {site["siteId"]}: Starding ESP Kafka")
        
    while True:
        current_time = datetime.now()

        for device in site["devices"]:

            for sensor in device["sensors"]:
                # new event
                event = simulate_iot_event(sensor, device, site["siteId"], current_time, logger)            
        
                # send event to topic
                produce_iot_message(IoT_topic, event, device["deviceId"], logger)
                
        logger.info(f"producer_iot.main - Site ID {site["siteId"]}: IoT data generation is in progress...")
        logger.info("To stop press Ctrl+C")
        logger.info("-----")
        
        # Define collection frequecy
        time.sleep(site["sleeptime"] / 1000)
        
    # logger.info(f"Completed run, logfile => {params["LOGGING_FILE"]}")      
    

if __name__ == "__main__":
    main()
    