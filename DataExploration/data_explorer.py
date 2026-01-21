import pandas as pd 
import logging
import os
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    from sys import path
    os.makedirs('SelfLogs/DataExplorationLogs', exist_ok=True)
    logging.basicConfig(filename='SelfLogs/DataExplorationLogs/data_explorer.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    logger.addHandler(logging.StreamHandler())# Add a stream handler to output logs to console
    logger.debug("Starting DataExplorer module as main program")
    # the file is located in a sibling directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, '..', 'AccessLogs')
    file_location = os.path.join(data_dir, 'apache_access.log.log')
    processor_directory = os.path.join(current_dir, '..', 'DataProcessing')
    path.append(processor_directory) # Add DataProcessing directory to sys.path to allow import
    from DataLoader import DataContainer 
    logger.debug("DataLoader imported successfully")



else:
    f
    file_location = 'AnalysisSuite/AccessLogs/apache_access.log.log'

data_container = DataContainer(file_location)
logger.debug("DataContainer instance created in DataExplorer")
data_container.process_data()
logger.info("Data processed in DataExplorer")

# get all unique values in all columns
for column in data_container.processed.columns:
    unique_values = data_container.processed[column].unique()
    logger.info("Column '%s' has %d unique values", column, len(unique_values))
    if len(unique_values) <= 10:
        logger.debug("Unique values in column '%s': %s", column, unique_values)
    else:
        logger.debug("First 10 unique values in column '%s': %s", column, unique_values[:10])
    