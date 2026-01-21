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
    Methods:
        __init__(self, file_location): Initializes the DataContainer by loading data from the given file location.
        process_data(self): Processes the raw log data into a structured DataFrame.
    """

    raw = None #raw data is in form //ip -- [DD/MON/YYYY:HH:MM:SS +ZZZZ] "REQUEST" STATUS SIZE "REFERRER" "USER_AGENT"//
    processed = None #processed data is in a dataframe with columns: ip, datetime, request_type, request, status, size, referrer, user_agent
    regex_patterns = {
        "ip": r'^(\d{1,3}(?:\.\d{1,3}){3}) ',
        "datetime": r'\[(.*?)\]',
        "request_type": r'"(GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH)"',
        "request": r'"(?:GET|POST|HEAD|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH)\s+(.+?)\s+HTTP/[\d.]+"',
        "status": r'" (\d{3}) ',
        "size": r' (\d+|-) "',
        "referrer": r'" (https?://\S+|-) "',
        "user_agent": r'"([^"]+)"$'
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
        
        self.raw = pd.read_table(file_location, header=None, names=['log_entry'], dtype=str)
        if len(self.raw) == 0:
            logger.error("Loaded data is empty from file: %s", file_location)
            raise ValueError("The loaded data is empty.")

        logger.debug("Raw data loaded with %d entries", len(self.raw))
        logger.debug(self.raw.iloc[0])

    def process_data(self):
        """Processes the raw log data into a structured DataFrame."""
        logger.debug("Processing raw data into structured format with regex")
        logger.debug("Sample log entry: %s", self.raw['log_entry'].iloc[0])
        self.processed = pd.DataFrame()
        for column, pattern in self.regex_patterns.items():
            self.processed[column] = self.raw['log_entry'].str.extract(pattern, expand=False)
            logger.debug("Extracted column: %s with pattern: %s", column, pattern)
            logger.debug("Sample extracted value: %s", self.processed[column].iloc[0] if len(self.processed[column]) > 0 else "None")

        logger.info("Data processing complete with %d entries", len(self.processed))


        
if __name__ == "__main__":
    os.makedirs('SelfLogs/DataProcessingLogs', exist_ok=True)
    logging.basicConfig(filename='SelfLogs/DataProcessingLogs/datacontainer.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.addHandler(logging.StreamHandler())# Add a stream handler to output logs to console
    logger.debug("Starting DataContainer module as main program")
    # set file location relative to this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    logger.debug("Current directory: %s", current_dir)
    data_dir = os.path.join(current_dir, '..', 'AccessLogs')
    logger.debug("Data directory: %s", data_dir)
    file_location = os.path.join(data_dir, 'apache_access.log.log')
    logger.debug("File location set to: %s", file_location)
    interpreter = DataContainer(file_location)
    logger.debug("DataContainer instance created")
    interpreter.process_data()
    logger.debug("DataContainer initialized and data processed successfully")
    logger.debug(interpreter.processed.head())
    logger.debug(interpreter.processed.iloc[0]['ip'])
