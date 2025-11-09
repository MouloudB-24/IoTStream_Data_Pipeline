import os
from dotenv import load_dotenv

# Load API KEY
load_dotenv()
API_KEY = os.environ["API_KEY"]

# Debug Levels:
"""
0 Debug       -> Allot of information will be printed, including printing site configuration to logfile.
1 Info        -> We just printing that processes is starting/ending
2 Warning     -> Will decide
3 Error       -> used in any try/except block
4 Critical    -> used when we going to kill the programm
"""

# Console Handler
CONSOLE_DEBUG_LEVEL = 1

# FILE Handler
FILE_DEBUG_LEVEL = 0

# Data
LOGGING_FILE = "logs/logger"
INPUT_DATA_FILE = "conf/beauce.json"

# Data site
SITE_IDS = 101
NB_SONSORS = 12

# Kafka
IoT_topic = "IoT_topic"
weather_topic = "weather_topic"

# Weather API
QUOTA = 1000
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
API_PARAMS = {
    "q": "Nogent-sur-seine, FR",
    "appid": API_KEY,
    "units": "metric",
    "lang": "fr"
}