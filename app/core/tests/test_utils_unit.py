import hashlib
import logging
from unittest.mock import patch

from ddt import data, ddt, unpack
from django.test import tag

from core.management.utils.xsr_client import (get_source_metadata_key_value,
                                              get_xsr_api_endpoint,
                                              get_xsr_api_response,
                                              read_source_file)
from core.models import XSRConfiguration

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class UtilsTests(TestSetUp):
    """Unit Test cases for utils """

    # Test cases for XSR_CLIENT
    def test_get_xsr_api_endpoint(self):
        """Test to check if API endpoint is present"""
        with patch('core.management.utils.xsr_client'
                   '.XSRConfiguration.objects') as xsrCfg:
            xsrConfig = XSRConfiguration(
                xsr_api_endpoint=self.xsr_api_endpoint_url)
            xsrCfg.first.return_value = xsrConfig
            return_from_function = get_xsr_api_endpoint()
            self.assertEqual(xsrConfig.xsr_api_endpoint, return_from_function)

    def test_get_xsr_api_response(self):
        """Test to Function to get api response from xsr endpoint"""
        with patch('core.management.utils.xsr_client.get_xsr_api_endpoint') \
                as xsr_ep, patch('requests.get') as response_obj:
            xsr_ep.return_value = self.xsr_api_endpoint_url
            response_obj.return_value = response_obj

            result_xsr_api_response = get_xsr_api_response()
            self.assertTrue(result_xsr_api_response)

    @patch('core.management.utils.xsr_client.extract_source',
           return_value=dict({1: {'a': 'b'}}))
    def test_read_source_file(self, extract):
        """test to check if data is present for extraction """

        result_data = read_source_file()
        self.assertIsInstance(result_data, list)

    @data(('key_field1', 'key_field2'), ('key_field11', 'key_field22'))
    @unpack
    def test_get_source_metadata_key_value(self, first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'shortname': first_value,
            'SOURCESYSTEM': second_value
        }

        expected_key = first_value + '_' + second_value
        expected_key_hash = hashlib.sha512(expected_key.encode('utf-8')). \
            hexdigest()

        result_key_dict = get_source_metadata_key_value(test_dict)
        self.assertEqual(result_key_dict['key_value'], expected_key)
        self.assertEqual(result_key_dict['key_value_hash'], expected_key_hash)

    @data(('key_field1', ''))
    @unpack
    def test_get_source_metadata_key_value_fail(self,
                                                first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'shortname': first_value,
            'SOURCESYSTEM': second_value
        }

        result_key_dict = get_source_metadata_key_value(test_dict)

        self.assertEqual(result_key_dict, None)
