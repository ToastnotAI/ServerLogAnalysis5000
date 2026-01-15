import pandas as pd
import os
import logging
import re
logger = logging.getLogger(__name__)





class DataContainer:
    """Class to interpret apache access log data as initial formatting is unsuitable for analysis.
    Attributes:
        raw (pd.DataFrame): The raw data loaded from the log file.
        processed (pd.DataFrame): The processed data after interpretation.
    """

    raw = None #raw data is in form //ip -- [DD/MON/YYYY:HH:MM:SS +ZZZZ] "REQUEST" STATUS SIZE "REFERRER" "USER_AGENT"//
    processed = None #processed data is in a dataframe with columns: IP, DateTime, Request, Status, Size, Referrer, User_Agent
    regex_patterns = {
        "ip": r'(^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
        "datetime": r'([(.*?)])',
        "request_type": r'"(GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH)"',
        "request": r'" GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH (.*? HTTP/[\d.]+")',
        "status": r'" (\d{3}) ',
        "size": r' (\d+|-) "',
        "referrer": r'" (http[s]?://.*?) "',
        "user_agent": r'" "([^"]+)"$'
        }


    def __init__(self, file_location):
        """Initializes the DataContainer by loading and processing the data from the given file location.
        Args:
            file_location (str): The path to the apache access log file.
        """

        logger.info("Initializing DataContainer with file location: %s", file_location)
        if( not os.path.exists(file_location)):
            logger.error("File not found at location: %s", file_location)
            raise FileNotFoundError(f"The file at {file_location} was not found.")
        
        self.raw = pd.read_csv(file_location, header=None, names=['log_entry'])
        if len(self.raw) == 0:
            logger.error("Loaded data is empty from file: %s", file_location)
            raise ValueError("The loaded data is empty.")

        logger.debug("Raw data loaded with %d entries", len(self.raw))

    def process_data(self):
        """Processes the raw log data into a structured DataFrame."""
        logger.debug("Processing raw data into structured format with regex")
        self.processed = pd.DataFrame()
        for key, pattern in self.regex_patterns.items():
            self.processed[key] = self.raw['log_entry'].str.extract(pattern, expand=False)
            logger.debug("Extracted %s data", key)
        logger.info("Data processing complete with %d entries", len(self.processed))
        
        
        
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)# Set logging level to DEBUG for detailed output
    logger.addHandler(logging.StreamHandler())# Add a stream handler to output logs to console
    logger.debug("Starting DataContainer module as main program")
    
    # set file location relative to this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, '..', 'AccessLogs')
    file_location = os.path.join(data_dir, 'apache_access.log.log')
    interpreter = DataContainer(file_location)
    interpreter.process_data()
    logger.debug("DataContainer initialized and data processed successfully")
    logger.debug(interpreter.processed.head())