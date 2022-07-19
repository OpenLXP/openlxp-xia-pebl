import hashlib
import json
import logging
import numpy as np
import pandas as pd
from django.core.management.base import BaseCommand
from django.utils import timezone
from openlxp_xia.management.utils.xia_internal import (
    convert_date_to_isoformat, get_publisher_detail)
from openlxp_xia.models import MetadataLedger

from core.management.utils.xsr_client import (get_source_metadata_key_value,
                                              read_source_file, find_dates,
                                              find_html)

logger = logging.getLogger('dict_config_logger')


def get_source_metadata():
    """Retrieving source metadata"""

    #  Retrieve metadata from agents as a list of sources
    df_source_list = read_source_file()

    # Iterate through the list of sources and extract metadata
    for source_item in df_source_list:
        logger.info('Loading metadata to be extracted from source')

        # Changing null values to None for source dataframe
        std_source_df = source_item.where(pd.notnull(source_item),
                                          None)
        if std_source_df.empty:
            logger.error("Source metadata is empty!")
        extract_metadata_using_key(std_source_df)


def add_publisher_to_source(source_df):
    """Add publisher column to source metadata and return source metadata"""

    # Get publisher name from system operator
    publisher = get_publisher_detail()
    if not publisher:
        logger.warning("Publisher field is empty!")
    # Assign publisher column to source data
    source_df['SOURCESYSTEM'] = publisher
    return source_df


def store_source_metadata(key_value, key_value_hash, hash_value, metadata):
    """Extract data from Experience Source Repository(XSR)
        and store in metadata ledger
    """
    # Setting record_status & deleted_date for updated record
    MetadataLedger.objects.filter(
        source_metadata_key_hash=key_value_hash,
        record_lifecycle_status='Active').exclude(
        source_metadata_hash=hash_value).update(
        metadata_record_inactivation_date=timezone.now())
    MetadataLedger.objects.filter(
        source_metadata_key_hash=key_value_hash,
        record_lifecycle_status='Active').exclude(
        source_metadata_hash=hash_value).update(
        record_lifecycle_status='Inactive')

    # Retrieving existing records or creating new record to MetadataLedger
    MetadataLedger.objects.get_or_create(
        source_metadata_key=key_value,
        source_metadata_key_hash=key_value_hash,
        source_metadata=metadata,
        source_metadata_hash=hash_value,
        record_lifecycle_status='Active')


def extract_metadata_using_key(source_df):
    """Creating key, hash of key & hash of metadata """
    # Convert source data to dictionary and add publisher to metadata
    source_df = add_publisher_to_source(source_df)
    source_remove_nan_df = source_df.replace(np.nan, '', regex=True)
    source_data_dict = source_remove_nan_df.to_dict(orient='index')

    logger.info('Setting record_status & deleted_date for updated record')
    logger.info('Getting existing records or creating new record to '
                'MetadataLedger')
    for temp_key, temp_val in source_data_dict.items():

        # key dictionary creation function called
        key = \
            get_source_metadata_key_value(source_data_dict[temp_key])
        # function to convert int to date
        temp_val_date_convert = find_dates(temp_val)
        # function to convert HTML to text
        temp_val_html_convert = find_html(temp_val_date_convert)
        # function to convert date to iso format
        temp_val_convert = json.dumps(temp_val_html_convert,
                                      default=convert_date_to_isoformat)
        temp_val_json = json.loads(temp_val_convert)
        # creating hash value of metadata
        hash_value = hashlib.sha512(str(temp_val_json).encode('utf-8')). \
            hexdigest()
        if key:
            # Call store function with key, hash of key, hash of metadata,
            # metadata
            store_source_metadata(key['key_value'], key['key_value_hash'],
                                  hash_value, temp_val_json)


class Command(BaseCommand):
    """Django command to extract data from Experience Source Repository (
    XSR) """

    def handle(self, *args, **options):
        """
            Metadata is extracted from XSR and stored in Metadata Ledger
        """
        get_source_metadata()

        logger.info('MetadataLedger updated with extracted data from XSR')
