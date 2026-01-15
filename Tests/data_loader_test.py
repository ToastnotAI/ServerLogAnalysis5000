import unittest
from DataProcessing.DataLoader import DataContainer
class TestDataContainer(unittest.TestCase):
    """Unit tests for the DataContainer class in DataLoader module."""
    def setUp(self):
        """Set up test environment."""
        # Set up a sample log file for testing
        self.test_file = 'test_apache_access.log'
    
    def insert_test_data(self, amount = 2):
        """Helper method to insert test data into the test log file."""
        test_data = [
            '114.119.128.158 - - [06/Aug/2024:00:00:15 +0000] "GET /pfaf/BlagdonWilderness.php HTTP/1.1" 200 9473 "https://singsurf.org/index.html" "Mozilla/5.0 (Linux; Android 7.0;) AppleWebKit/537.36 (KHTML, like Gecko) Mobile Safari/537.36 (compatible; PetalBot;+https://webmaster.petalsearch.com/site/petalbot)"',
            '207.46.13.125 - - [06/Aug/2024:00:00:31 +0000] "GET /wallpaper/wallpaper.php?FILENAME=06-14-12(184120).png HTTP/2.0" 200 8383 "-" "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm) Chrome/116.0.1938.76 Safari/537.36"'
        ]
        with open(self.test_file, 'w') as f:
            for _ in range(amount):
                f.write('\n'.join(test_data) + '\n')
    
    def test_load_raw_data(self):
        """Test loading raw data from the log file."""
        self.insert_test_data()
        data_container = DataContainer(self.test_file)
        self.assertIsNotNone(data_container.raw)
        self.assertEqual(len(data_container.raw), 2)

    def test_load_no_data(self):
        """Test loading from an empty log file."""
        with open(self.test_file, 'w') as f:
            pass  # Create an empty file
        with self.assertRaises(ValueError):
            DataContainer(self.test_file)
        
    def test_invalid_file_path(self):
        """Test loading from an invalid file path."""
        with self.assertRaises(FileNotFoundError):
            DataContainer('non_existent_file.log')
    
    def test_process_data(self):
        """Test processing one row of raw data into structured format."""
        self.insert_test_data(1)
        data_container = DataContainer(self.test_file)
        data_container.process_data()
        self.assertIsNotNone(data_container.processed)
        self.assertIn('ip', data_container.processed.columns)
        self.assertIn('datetime', data_container.processed.columns)
        self.assertIn('request_type', data_container.processed.columns)
        self.assertIn('request', data_container.processed.columns)
        self.assertIn('status', data_container.processed.columns)
        self.assertIn('size', data_container.processed.columns)
        self.assertIn('referrer', data_container.processed.columns)
        self.assertIn('user_agent', data_container.processed.columns)
        self.assertEqual(len(data_container.processed), 2)