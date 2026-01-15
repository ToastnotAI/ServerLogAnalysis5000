import pandas as pd 

if __name__ == "__main__":
    import os
    # the file is located in a sibling directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(current_dir, '..', 'AccessLogs')
    file_location = os.path.join(data_dir, 'apache_access.log.log')
else:
    file_location = 'AnalysisSuite/AccessLogs/apache_access.log.log'

