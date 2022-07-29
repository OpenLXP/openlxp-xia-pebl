import pandas as pd
from django.test import TestCase


class TestSetUp(TestCase):
    """Class with setup and teardown for tests in XIS"""

    def setUp(self):
        """Function to set up necessary data for testing"""

        # globally accessible data sets

        self.source_metadata = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "test_name": "test name",
            "Start_date": "2017-03-28T00:00:00-04:00",
            "crs_header": "TestData 123",
            "SOURCESYSTEM": "DAU",
            "test_description": "test description",
        }

        self.key_value = "TestData 123_DAU"
        self.key_value_hash = "902b044e8abbea677ac6b5d75b70b9c7d819a1d246" \
                              "10812898f4c168fa2b7c4b85dc7143605f0e8da20ce" \
                              "73f65cbcae0fa08f0d21d40dbb9d9eafd876b8575a5"
        self.hash_value = "9cb7277d49f4649452bf16455f462258b85345251b4fe655" \
                          "7000acf7812bb0b3db399fc2132bef042923ef6843c3026fe" \
                          "1d0bc0914a378a34ad3d9141f1ee38b"

        self.test_data = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}

        self.metadata_df = pd.DataFrame.from_dict({1: self.source_metadata},
                                                  orient='index')

        self.xis_api_endpoint_url = 'http://example'
        self.xsr_api_endpoint_url = 'http://example'

        return super().setUp()

    def tearDown(self):
        return super().tearDown()
