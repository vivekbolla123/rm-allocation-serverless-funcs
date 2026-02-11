
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

    def test_train_pipeline(self):
        print("\nTesting Training Pipeline...")
        overrides = {"action": "train"}
        ml = MLForecasting(self.logger, self.params, overrides, self.context)
        
        # Configure LabelEncoder mock to return correct length
        # We need to access the mocked module
        from sklearn.preprocessing import LabelEncoder
        # Since LabelEncoder is a class in the mocked module
        # LabelEncoder() creates an instance.
        # We need to set fit_transform on ANY instance or a specific one if we could capture it.
        # But here MLForecasting instantiates it.
        # So we patch the class in the module.
        
        mock_le_instance = MagicMock()
        mock_le_instance.fit_transform.side_effect = lambda x: [0] * len(x)
        
        # We need to make sure LabelEncoder() returns this instance
        # Access the dictionary of the mocked module (sys.modules['sklearn.preprocessing'])
        # But we already imported it as a MagicMock. 
        # Actually in `test_ml_forecasting_local.py` imports happen at top level.
        # `from sklearn.preprocessing import LabelEncoder`
        # `LabelEncoder` IS a MagicMock.
        # So `LabelEncoder()` returns a NEW MagicMock by default.
        # We need `LabelEncoder.return_value` to be our configured instance.
        LabelEncoder.return_value = mock_le_instance

        # Mock pd.read_sql to return our sample data
        # We need to return data when called inside the loop
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
            # Mock datetime in ml_forecasting to control "now" and ensure filtering works
            # We set now to match one of the BookingDates in mock data (2024-01-01)
            # Departure is 2024-01-10. So NDO = 9.
            # Mock data Row 1: Dep 10, Book 01 -> NDO 9.
            # Target NDO (Dep 10 - Now 01) -> 9.
            # So Row 1 should be kept.
            
            fixed_now = pd.Timestamp('2024-01-01 10:00:00')
            with patch('ml_forecasting.datetime') as mock_dt_cls:
                mock_dt_cls.now.return_value = fixed_now
                # Ensure side_effect for other attributes isn't blocking, but we mainly need now()
                
                # Mock S3 get_object response
                mock_model_body = MagicMock()
                mock_model_body.read.return_value = b'mock_model_bytes'
                
                mock_encoder_body = MagicMock()
                mock_encoder_body.read.return_value = b'mock_encoder_bytes'
                
                self.mock_s3.get_object.side_effect = [
                    {'Body': mock_model_body}, # First call for model
                    {'Body': mock_encoder_body} # Second call for encoders
                ]
                
                # Mock joblib.load to return dummy model/encoders
                mock_model = MagicMock()
                # Predict should return list of length equal to input rows
                # We expect filtering to reduce rows.
                # Row 1 (Book 2024-01-01) matches NDO 9.
                # Row 2 (Book 2024-01-05) NDO 5. Target 9. Drop.
                # Row 3 (Dep 11, Book 02) NDO 9. Target (11-1) 10. Drop.
                # Row 4 (Dep 11, Book 08) NDO 3. Target 10. Drop.
                # So we expect 1 row? 
                # Wait, let's check Row 3: Dep 11. Now 01. Target NDO = 10.
                # Book 02. NDO = 9. 9 != 10. Drop.
                # So only Row 1 remains?
                # Actually, filtering logic: df['NDO'] == df['Target_NDO'].
                # So dynamic prediction length.
                mock_model.predict.side_effect = lambda x: [0.5] * len(x)
                
                mock_le = MagicMock()
                mock_le.classes_ = ['DELBOM', 'BOMDEL']
                mock_le.transform.side_effect = lambda x: [0] * len(x)
                
                # We need to patch joblib.load used inside MLForecasting
                with patch('joblib.load', side_effect=[mock_model, {'sector': mock_le}]):
                    # Mock to_sql to verify write
                    with patch('pandas.DataFrame.to_sql') as mock_to_sql:
                        ml.run()
                        # Verify to_sql called
                        self.assertTrue(mock_to_sql.called)
                        # verify arguments (optional)
                        # args, _ = mock_to_sql.call_args
                        # self.assertEqual(args[0], 'Forecasting_Results_ML')
                        print("Scoring Pipeline successfully predicted and called to_sql.")

if __name__ == '__main__':
    unittest.main()
