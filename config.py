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

LOGGING_FILE = "logging/logger"

INPUT_DATA_FILE = "resources/sites.json"

SITE_IDS = [(101, "CNPE-Nogent")]

