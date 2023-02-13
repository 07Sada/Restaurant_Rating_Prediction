# Import the required libraries
import logging 
from datetime import datetime 
import os 

# Define the name for the log file
# The name is created by combining the current date and time with the .log extension
LOG_FILE_NAME = f"{datetime.now().strftime('%m%d%Y__%H%M%S')}.log"

# Define the path to the log file directory
LOG_FILE_DIR = os.path.join(os.getcwd(),'logs')

# Create the log file directory if it does not already exist
os.makedirs(LOG_FILE_DIR, exist_ok=True)

# Define the full path to the log file by combining the directory and name
LOG_FILE_PATH = os.path.join(LOG_FILE_DIR, LOG_FILE_NAME)

# Configure the logging
# The log file location is specified with the filename parameter
# The format parameter defines the format of the log entries, including the date and time, line number, logger name, level, and message
# The level parameter is set to logging.INFO to log informational messages
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
