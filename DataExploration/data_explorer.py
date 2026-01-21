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
    processor_directory = os.path.join(current_dir, '..', 'DataProcessing')
    path.append(processor_directory) # Add DataProcessing directory to sys.path to allow import
    from DataLoader import DataContainer 
    logger.debug("DataLoader imported successfully")



else:
    #import data loader assuming the program is run from parent directory
    from DataProcessing.DataLoader import DataContainer


data_container = DataContainer()
logger.debug("DataContainer instance created in DataExplorer")
data_container.process_data()
logger.info("Data processed in DataExplorer")

# get all unique values in all columns and count occurences
for column in data_container.processed.columns:
    value_counts = data_container.processed[column].value_counts()
    logger.info("Top 5 most common values in column '%s':\n%s", column, value_counts.head(5))
    unique_values = data_container.processed[column].unique()
    logger.info("Column '%s' has %d unique values", column, len(unique_values))
    if len(unique_values) <= 10:
        logger.debug("Unique values in column '%s': %s", column, unique_values)
    else:
        logger.debug("First 10 unique values in column '%s': %s", column, unique_values[:10])
    
