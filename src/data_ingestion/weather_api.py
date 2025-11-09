import os
import requests
import time


def query_weather_api(url, api_params, logger):
    
    """summary"""
    
    response = requests.get(url, api_params)
    
    if response.status_code != 200:
        logger.info(f"weather.query_weather_api - Problem in the API request: {response.status_code}")
        return -1
    
    return  response.json()


def main():
    
    


if __name__ == "__main__":
    from pprint import pprint
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    api_params = {
        "q": "Nogent-sur-Seine, FR",
        "appid": "01a86db818cffaff596ecb521c7d1228",
        "units": "metric",
        "lang": "fr"
    }
    pprint(query_weather_api(BASE_URL, api_params))
