import os
import requests
import time


def query_weather_api(url, current_time, params, logger):
    
    """summary"""
    
    response = requests.get(url, params)
    
    if response.status_code != 200:
        logger.info(f"weather.query_weather_api - Problem in the API request: {response.status_code}")
        return -1
    
    raw_data = response.json()
    
    return {
        "time":current_time.isoformat(),
        "city": raw_data["name"],
        "temperature": raw_data["main"]["temp_max"],
        "pressure": raw_data["main"]["pressure"],
        "humidity": raw_data["main"]["humidity"]
        }


def get_weather_data(url, current_time, params, logger, api_quota):
    
    """summary"""
    
    weather_data = []
    
    for _ in range(api_quota):
        current_data = query_weather_api(url, current_time, params, logger)
        if current_data == -1:
            os._exit(1)
                
        weather_data.append(current_data)
        
        # break
        time.sleep(5)       
        
    return weather_data
