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


def create_mongo_connection(params, logger):
    
    """Connect to the MongoDB database 'IoTSensor_db' """
    
    try:
        if params["MONGO_USERNAME"] != "":
            MONGO_URI = f"mongodb://{params["MONGO_USERNAME"]}:{params["MONGO_PASSWORD"]}@localhost:27017/?directConnection=true"
        else:
            MONGO_URI = "mongodb://localhost:27017/?directConnection=true"
    
        logger.debug(f"connection_db.create_mongo_connection - URI: {MONGO_URI}")

        try:
            connection = pymongo.MongoClient(MONGO_URI)
            connection.admin.command("ping")
            
        except pymongo.errors.ServerSelectionTimeoutError as err:
            logger.critical(f"connection_db.create_mongo_connection - FAILED error: {err}")
            return -1
        
        my_database = connection[params["MONGO_DATABASE"]]
        my_collection = my_database[params["MONGO_COLLECTION"]]
        
        logger.info(f"connection_db.create_mongo_connection - CONNECTED")
        
        return my_collection
    
    except Exception as e:
        logger.critical(f"connection_db.create_mongo_connection - FAILED error: {e}")
        return -1 
        

def insert_to_mongodb(my_collection, events, logger):
    
    """Insert one or many document into the MongoDB collection"""
    
    try:  
        return my_collection.insert_many(events)
    
    except pymongo.errors.PyMongoError as e:
        logger.error(f"connection.insertOne - insert_many - FAILED: {e} ")
        return -1

    