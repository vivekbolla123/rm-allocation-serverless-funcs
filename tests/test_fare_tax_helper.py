import pytest
import sys
import os

# Add rm-allocation-process-inputs to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rm-allocation-process-inputs'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'rm-allocation-configurations'))

from common.fare_tax_helper import FareTaxHelper


class MockMongoMasterDb:
    def __init__(self):
        self.tax_mast_dom_data = {
            'BOM': {
                'Code': 'BOM',
                'GST': 5.0,
                'RCS': 50.0,
                'CUTE': 50.0,
                'ASF': 236.0,
                'PSF': 0.0,
                'UDF_DF': 207.0
            },
            'DEL': {
                'Code': 'DEL',
                'GST': 5.0,
                'RCS': 50.0,
                'CUTE': 50.0,
                'ASF': 236.0,
                'PSF': 0.0,
                'UDF_DF': 152.0
            },
            'IXB': {
                'Code': 'IXB',
                'GST': 0.0,
                'RCS': 0.0,
                'CUTE': 50.0,
                'ASF': 236.0,
                'PSF': 0.0,
                'UDF_DF': 0.0
            }
        }
        self.tax_mast_intl_data = {
            'BOMDOH': {
                'Sectors': 'BOMDOH',
                'GST': 5.0,
                'RCS': 0.0,
                'CUTE': 50.0,
                'ASF': 1174.0,
                'PSF_ADF': 0.0,
                'UDF': 0.0,
                'PSC_PZ': 0.0,
                'Tax1': 100.0,
                'Tax2': 50.0,
                'Tax3': 0.0,
                'Tax4': 0.0,
                'Tax5': 0.0
            }
        }

    def __getitem__(self, item):
        class MockCollection:
            def __init__(self, data):
                self.data = data

            def find(self, query=None, projection=None):
                return list(self.data.values())

            def find_one(self, query, projection=None):
                if 'Code' in query:
                    return self.data.get(query['Code'])
                if 'Sectors' in query:
                    return self.data.get(query['Sectors'])
                return None

        if item == 'tax_mast_dom':
            return MockCollection(self.tax_mast_dom_data)
        elif item == 'tax_mast_intl':
            return MockCollection(self.tax_mast_intl_data)
        return MockCollection({})


class MockConstantsUtils:
    def __init__(self):
        self.PRICING_GST_SECTORS = ['IXB', 'GAU', 'IXA', 'AJL']
        self.PRICING_RCS_SECTORS = ['IXB', 'GAU', 'IMF']
        self.PRICING_DUF_SECTORS = {'DEL': 10.0}


class MockRMDatabaseHelper:
    def get_stage_length(self, origin, destination):
        return 1150.0


def test_domestic_conversion_roundtrip():
    mongo_mock = MockMongoMasterDb()
    const_mock = MockConstantsUtils()

    helper = FareTaxHelper(
        mongo_master_db=mongo_mock,
        constants_utils=const_mock
    )

    base_fare = 3500.0
    gross_fare = helper.airline_component_to_allin(base_fare, 'BOM', 'DEL', route='D')
    assert gross_fare > base_fare

    converted_base = helper.allin_to_airline_component(gross_fare, 'BOM', 'DEL', route='D')
    # Round-trip difference within 0.05
    assert abs(converted_base - base_fare) < 0.05


def test_gst_exemption_sector():
    mongo_mock = MockMongoMasterDb()
    const_mock = MockConstantsUtils()

    helper = FareTaxHelper(
        mongo_master_db=mongo_mock,
        constants_utils=const_mock
    )

    base_fare = 4000.0
    # IXB is GST exempt
    gross_fare = helper.airline_component_to_allin(base_fare, 'BOM', 'IXB', route='D')
    converted_base = helper.allin_to_airline_component(gross_fare, 'BOM', 'IXB', route='D')
    assert abs(converted_base - base_fare) < 0.05


def test_international_conversion_roundtrip():
    mongo_mock = MockMongoMasterDb()
    const_mock = MockConstantsUtils()

    helper = FareTaxHelper(
        mongo_master_db=mongo_mock,
        constants_utils=const_mock
    )

    base_fare = 15000.0
    gross_fare = helper.airline_component_to_allin(base_fare, 'BOM', 'DOH', route='I')
    assert gross_fare > base_fare

    converted_base = helper.allin_to_airline_component(gross_fare, 'BOM', 'DOH', route='I')
    assert abs(converted_base - base_fare) < 0.05


def test_allin_to_airline_component_market_fares():
    mongo_mock = MockMongoMasterDb()
    const_mock = MockConstantsUtils()

    helper = FareTaxHelper(
        mongo_master_db=mongo_mock,
        constants_utils=const_mock
    )

    # Competitor all-in fare is 5000
    comp_allin = 5000.0
    comp_ac = helper.allin_to_airline_component(comp_allin, 'BOM', 'DEL', route='D')
    assert comp_ac < comp_allin
    # For BOM-DEL: other_taxes = 236 + 0 + 207 + 10 = 453.0; GST = 5%
    # comp_ac = (5000 - 453) / 1.05 = 4330.48
    assert 4200 < comp_ac < 4450


def test_fares_to_airline_component_dataframe():
    import pandas as pd
    mongo_mock = MockMongoMasterDb()
    const_mock = MockConstantsUtils()

    helper = FareTaxHelper(
        mongo_master_db=mongo_mock,
        constants_utils=const_mock
    )

    df = pd.DataFrame([
        {'SECTOR': 'BOMDEL', 'TOTAL': 5000.0, 'RBD': 'Y'},
        {'SECTOR': 'BOMIXB', 'TOTAL': 4443.0, 'RBD': 'M'},
        {'SECTOR': 'BOMDOH', 'TOTAL': 17094.0, 'RBD': 'B'},
    ])

    res = helper.fares_to_airline_component(df, fare_col='TOTAL')
    assert 'AirlineComponent' in res.columns
    assert len(res) == 3

    # BOMDEL: (5000 - 453) / 1.05 = 4330.476...
    assert abs(res.loc[res['SECTOR'] == 'BOMDEL', 'AirlineComponent'].iloc[0] - 4330.48) < 0.1
    # BOMIXB: (4443 - 443) / 1.0 = 4000.0
    assert abs(res.loc[res['SECTOR'] == 'BOMIXB', 'AirlineComponent'].iloc[0] - 4000.0) < 0.1

