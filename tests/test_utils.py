import unittest
import pandas as pd
from src.utils import website_sentiment_distribution  # Ensure correct import path

class TestUtils(unittest.TestCase):
    def test_website_sentiment_distribution(self):
        data = pd.DataFrame({
            'source_name': ['site1', 'site2', 'site1', 'site2'],
            'title_sentiment': ['Positive', 'Neutral', 'Negative', 'Positive']
        })
        print(data)
        result = website_sentiment_distribution(data)

        expected_data = pd.DataFrame({
            
            'Negative': [1, 0],
            'Neutral': [0, 1],
            'Positive': [1, 1],
            'Total': [2, 2],
            'Mean': [0.666667 ,0.666667 ],
            'Median': [1.0, 1.0]
        }, index=['site1', 'site2'])
        # Reset index in result for comparison if necessary
        result_reset = result.reset_index(drop=True)
        expected_reset = expected_data.reset_index(drop=True)

        # Compare without index names
        pd.testing.assert_frame_equal(result_reset, expected_reset, check_index_type=False)

if __name__ == '__main__':
    unittest.main()
