import unittest
import pandas as pd
from io import StringIO
from unittest.mock import patch, mock_open
from src.loader import NewsDataLoader  # Replace 'your_module' with the actual module name

class TestNewsDataLoader(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data="col1,col2\n1,2\n3,4")
    @patch('pandas.read_csv')
    def test_load_data_first_time(self, mock_read_csv, mock_file):
        # Create a mock DataFrame to return when pd.read_csv is called
        mock_df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        mock_read_csv.return_value = mock_df

        # Instantiate NewsDataLoader
        loader = NewsDataLoader()

        # Call load_data and check the returned DataFrame
        df = loader.load_data('fake_path.csv')
        mock_read_csv.assert_called_once_with('fake_path.csv')
        pd.testing.assert_frame_equal(df, mock_df)

        # Check that the data is stored in the loader's data attribute
        self.assertIn('fake_path.csv', loader.data)
        pd.testing.assert_frame_equal(loader.data['fake_path.csv'], mock_df)

    @patch('builtins.open', new_callable=mock_open, read_data="col1,col2\n1,2\n3,4")
    @patch('pandas.read_csv')
    def test_load_data_cached(self, mock_read_csv, mock_file):
        # Create a mock DataFrame to return when pd.read_csv is called
        mock_df = pd.DataFrame({"col1": [1, 3], "col2": [2, 4]})
        mock_read_csv.return_value = mock_df

        # Instantiate NewsDataLoader and preload the data
        loader = NewsDataLoader()
        loader.data['fake_path.csv'] = mock_df

        # Call load_data and check that read_csv is not called again
        df = loader.load_data('fake_path.csv')
        mock_read_csv.assert_not_called()  # Ensure read_csv wasn't called
        pd.testing.assert_frame_equal(df, mock_df)

if __name__ == '__main__':
    unittest.main()
