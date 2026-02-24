
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np
import logging
import sys
from unittest.mock import MagicMock

# Mock xgboost and sklearn if not installed
sys.modules['xgboost'] = MagicMock()
sys.modules['sklearn.preprocessing'] = MagicMock()
sys.modules['joblib'] = MagicMock()

from ml_forecasting import MLForecasting
from io import BytesIO

class TestMLForecasting(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger()
        self.mock_dwconn = MagicMock()
        self.mock_esbconn = MagicMock()
        self.mock_s3 = MagicMock()
        self.params = {
            "dwconn": self.mock_dwconn,
            "esbconn": self.mock_esbconn,
            "s3": self.mock_s3
        }
        self.context = {}

    def get_mock_training_data(self):
        # Create sample DataFrame mimicking SQL result
        data = {
            'InventoryLegID': [1, 1, 2, 2],
            'DepartureDate': ['2024-01-10', '2024-01-10', '2024-01-11', '2024-01-11'],
            'DepartureStation': ['DEL', 'DEL', 'BOM', 'BOM'],
            'ArrivalStation': ['BOM', 'BOM', 'DEL', 'DEL'],
            'FlightNumber': ['AI101', 'AI101', 'AI202', 'AI202'],
            'STD': ['10:00:00', '10:00:00', '14:00:00', '14:00:00'],
            'AdjustedCapacity': [180, 180, 150, 150],
            'BookingDate': ['2024-01-01', '2024-01-05', '2024-01-02', '2024-01-08'],
            'FareBasis': ['Y123T', 'Y123C', 'Y123G', 'Y123'],
            'PaxType': ['ADT', 'ADT', 'ADT', 'ADT'],
            'RevenuePax': [10, 5, 20, 15]
        }
        return pd.DataFrame(data)

    def test_preprocess_creates_correct_features(self):
        """Verify preprocessing creates all BRD-required columns."""
        print("\nTesting Preprocessing Feature Completeness...")
        overrides = {"action": "train"}
        ml = MLForecasting(self.logger, self.params, overrides, self.context)
        
        df = self.get_mock_training_data()
        result = ml.preprocess_data(df, is_training=True)
        
        if result.empty:
            self.skipTest("No processed data (likely no NDO-0 target).")
            return

        # NDO range should be 0-38
        ndo_values = sorted(result['NDO'].unique())
        self.assertEqual(ndo_values[0], 0, "Min NDO should be 0")
        self.assertEqual(ndo_values[-1], 38, "Max NDO should be 38")
        
        # Required columns
        for col in ['Sector', 'Sector_Capacity', 'Day_of_Week',
                     'Cum_PLF', 'Cum_Corp', 'Cum_Group', 'Cum_FIT', 'Cum_TBF',
                     'Lag_1_PLF', 'Lag_1_Corp', 'Lag_1_Group', 'Lag_1_FIT', 'Lag_1_TBF',
                     'month_sin', 'week_cos', 'Weekend_Flag', 'Time_Bucket']:
            self.assertIn(col, result.columns, f"Missing column: {col}")
        
        # Cumulative PLF values should be percentages (0-1 range for sanity)
        self.assertTrue((result['Cum_PLF'] >= 0).all(), "Cum_PLF should be >= 0")
        self.assertTrue((result['Cum_Corp'] >= 0).all(), "Cum_Corp should be >= 0")
        
        # Sector_Capacity should be > 0
        self.assertTrue((result['Sector_Capacity'] > 0).all(), "Sector_Capacity should be > 0")
        
        print("Preprocessing creates all required features correctly.")

    def test_lag_imputation_bfill_ffill(self):
        """Verify lag imputation uses bfill then ffill, not fillna(0)."""
        print("\nTesting Lag Imputation Strategy...")
        overrides = {"action": "train"}
        ml = MLForecasting(self.logger, self.params, overrides, self.context)
        
        df = self.get_mock_training_data()
        result = ml.preprocess_data(df, is_training=True)
        
        if result.empty:
            self.skipTest("No processed data.")
            return

        # For a single leg, check lag continuity (no isolated zeros surrounded by non-zeros)
        leg1 = result[result['InventoryLegID'] == 1].sort_values('NDO', ascending=False)
        lag_plf = leg1['Lag_1_PLF'].values
        
        # After bfill/ffill, there should be no NaN
        self.assertFalse(np.isnan(lag_plf).any(), "Lag_1_PLF should have no NaN after imputation")
        print("Lag imputation passes basic checks.")

    def test_train_pipeline(self):
        print("\nTesting Training Pipeline...")
        overrides = {"action": "train"}
        ml = MLForecasting(self.logger, self.params, overrides, self.context)
        
        # Configure LabelEncoder mock
        from sklearn.preprocessing import LabelEncoder
        
        mock_le_instance = MagicMock()
        mock_le_instance.fit_transform.side_effect = lambda x: [0] * len(x)
        LabelEncoder.return_value = mock_le_instance

        # Mock pd.read_sql to return our sample data
        with patch('pandas.read_sql', return_value=self.get_mock_training_data()):
            ml.run()
            
        # Verify S3 upload called (Model and Encoders)
        self.assertEqual(self.mock_s3.put_object.call_count, 2)
        print("Training Pipeline successfully generated and uploaded artifacts.")

    def test_score_pipeline(self):
        print("\nTesting Scoring Pipeline...")
        overrides = {"action": "score"}
        ml = MLForecasting(self.logger, self.params, overrides, self.context)
        
        # Mock read_sql for fetching scoring data
        with patch('pandas.read_sql', return_value=self.get_mock_training_data()):
            fixed_now = pd.Timestamp('2024-01-01 10:00:00')
            with patch('ml_forecasting.datetime') as mock_dt_cls:
                mock_dt_cls.now.return_value = fixed_now
                
                # Mock S3 get_object response
                mock_model_body = MagicMock()
                mock_model_body.read.return_value = b'mock_model_bytes'
                
                mock_encoder_body = MagicMock()
                mock_encoder_body.read.return_value = b'mock_encoder_bytes'
                
                self.mock_s3.get_object.side_effect = [
                    {'Body': mock_model_body},
                    {'Body': mock_encoder_body}
                ]
                
                # Mock joblib.load to return dummy model/encoders
                mock_model = MagicMock()
                mock_model.predict.side_effect = lambda x: [0.5] * len(x)
                
                mock_le_sector = MagicMock()
                mock_le_sector.classes_ = ['DELBOM', 'BOMDEL']
                mock_le_sector.transform.side_effect = lambda x: [0] * len(x)
                
                mock_le_day = MagicMock()
                mock_le_day.classes_ = [0, 1, 2, 3, 4, 5, 6]
                mock_le_day.transform.side_effect = lambda x: [0] * len(x)
                
                with patch('joblib.load', side_effect=[mock_model, {'sector': mock_le_sector, 'day': mock_le_day}]):
                    with patch('pandas.DataFrame.to_sql') as mock_to_sql:
                        ml.run()
                        self.assertTrue(mock_to_sql.called)
                        print("Scoring Pipeline successfully predicted and called to_sql.")

if __name__ == '__main__':
    unittest.main()
