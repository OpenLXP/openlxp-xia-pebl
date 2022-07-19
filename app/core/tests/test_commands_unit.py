import logging
from unittest.mock import patch

import pandas as pd
from ddt import ddt
from django.core.management import call_command
from django.db.utils import OperationalError
from django.test import tag
from openlxp_xia.models import XIAConfiguration

from core.management.commands.extract_source_metadata import (
    add_publisher_to_source, extract_metadata_using_key, get_source_metadata)

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class CommandTests(TestSetUp):

    # Test cases for waitdb
    def test_wait_for_db_ready(self):
        """Test that waiting for db when db is available"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.return_value = True
            call_command('waitdb')
            self.assertEqual(gi.call_count, 1)

    @patch('time.sleep', return_value=True)
    def test_wait_for_db(self, ts):
        """Test waiting for db"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.side_effect = [OperationalError] * 5 + [True]
            call_command('waitdb')
            self.assertEqual(gi.ensure_connection.call_count, 6)

    # Test cases for extract_source_metadata
    def test_get_source_metadata(self):
        """ Test to retrieving source metadata"""
        with patch('core.management.commands.extract_source_metadata'
                   '.read_source_file') as read_obj, patch(
            'core.management.commands.extract_source_metadata'
            '.extract_metadata_using_key', return_value=None) as \
                mock_extract_obj:
            read_obj.return_value = read_obj
            read_obj.return_value = [
                pd.DataFrame.from_dict(self.test_data, orient='index')]
            get_source_metadata()
            self.assertEqual(mock_extract_obj.call_count, 1)

    def test_add_publisher_to_source(self):
        """Test for Add publisher column to source metadata and return
        source metadata"""
        with patch('openlxp_xia.management.utils.xia_internal'
                   '.get_publisher_detail'), \
                patch('openlxp_xia.management.utils.xia_internal'
                      '.XIAConfiguration.objects') as xisCfg:
            xiaConfig = XIAConfiguration(publisher='JKO')
            xisCfg.first.return_value = xiaConfig
            test_df = pd.DataFrame.from_dict(self.test_data)
            result = add_publisher_to_source(test_df)
            key_exist = 'SOURCESYSTEM' in result.columns
            self.assertTrue(key_exist)

    def test_extract_metadata_using_key(self):
        """Test to creating key, hash of key & hash of metadata"""

        data = {1: self.source_metadata}
        data_df = pd.DataFrame.from_dict(data, orient='index')
        with patch(
                'core.management.commands.extract_source_metadata'
                '.add_publisher_to_source',
                return_value=data_df), \
                patch(
                    'core.management.commands.extract_source_metadata'
                    '.get_source_metadata_key_value',
                    return_value=None) as mock_get_source, \
                patch(
                    'core.management.commands.extract_source_metadata'
                    '.store_source_metadata',
                    return_value=None) as mock_store_source:
            mock_get_source.return_value = mock_get_source
            mock_get_source.exclude.return_value = mock_get_source
            mock_get_source.filter.side_effect = [
                mock_get_source, mock_get_source]

            extract_metadata_using_key(data_df)
            self.assertEqual(mock_get_source.call_count, 1)
            self.assertEqual(mock_store_source.call_count, 1)
