#####################################################################
#
# Project       : IoT based TimesSeries Data via Python Application
#
# File          : connection_db.py
#
# Description   : Function for use to persist the data to file and MongoDB
# 
#                 Options:
#                       - site["data_persistence"] = 0 => None
#                       - site["data_persistence"] = 1 => File
#                       - site["data_persistence"] = 2 => MongoDB
#
# Created       : 3 November 2025
#
# Author        : Mouloud BELLIL
#
# Email         : mouloud.bellil@outlook.fr
#
#######################################################################

import pymongo


def create_mongo_connection(config_params, siteId, logger):
    
    """Connect to the MongoDB database 'IoTSensor_db' """
    
    try:
        if config_params["MONGO_USERNAME"] != "":
            config_params["MONGO_URI"] = f'mongodb://{config_params["MONGO_USERNAME"]}:{config_params["MONGO_PASSWORD"]}@{config_params["MONGO_HOST"]}:{int(config_params["MONGO_PORT"])}/?directConnection=true'
        else:
            config_params["MONGO_URI"] = f'mongodb://{config_params["MONGO_HOST"]}:{int(config_params["MONGO_PORT"])}/?{config_params["MONGO_DIRECT"]}'
    
        logger.debug(f"connection_db.create_mongo_connection - Site {siteId} - URL: {config_params['MONGO_URI']}")

        try:
            connection = pymongo.MongoClient(config_params["MONGO_URI"])
            connection.admin.command("ping")
            
        except pymongo.errors.ServerSelectionTimeoutError as err:
            logger.critical(f"connection_db.create_mongo_connection - Site {siteId} - FAILED error: {err}")
            return -1
        
        my_db = connection[config_params["MONGO_DATASTORE"]]
        my_collection = my_db[config_params["MONGO_COLLECTION"]]
        
        logger.info(f"connection_db.create_mongo_connection - Site {siteId} - CONNECTED")
        
        return my_collection
    
    except Exception as e:
        logger.critical(f"connection_db.create_mongo_connection - Site {siteId} - FAILED error: {e}")
        return -1 
        

def insert_mongo(my_collection, siteId, mode, payload, logger):
    
    """Insert one or many document into the MongoDB collection"""
    
    try:
        if mode == 1:
            result = my_collection.insert_one(payload)
        
        else:
            result = my_collection.insert_many(payload)
    
        return result
    
    except pymongo.errors.PyMongoError as e:
        logger.error(f"connection.insertOne - Site ID {siteId}, insertOne - FAILED: {e} ")
        return -1

    