import datetime
import hashlib
import json
import logging
import uuid
import zipfile

import html2text
import pandas as pd
import requests
from bs4 import BeautifulSoup
from lxml import etree
from openlxp_xia.management.utils.xia_internal import (
    dict_flatten, get_key_dict, traverse_dict_with_key_list)

from core.models import XSRConfiguration

logger = logging.getLogger('dict_config_logger')


def get_xsr_api_endpoint():
    """Setting API endpoint from XIA and XIS communication """
    logger.debug("Retrieve xsr_api_endpoint from XSR configuration")
    xsr_data = XSRConfiguration.objects.first()
    xsr_api_endpoint = xsr_data.xsr_api_endpoint
    return xsr_api_endpoint


def get_xsr_api_response():
    """Function to get api response from xsr endpoint"""
    # url of rss feed
    url = get_xsr_api_endpoint()

    # creating HTTP response object from given url
    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        raise SystemExit('Exiting! Can not make connection with XSR.')

    return resp


def append_root_url(path):
    """function to append base url and path"""
    url = get_xsr_api_endpoint()
    base_url = url.split("epub_content")[0]

    return base_url + "?epub=" + str(path)


def append_url(path):
    """function to append base url and path"""
    url = get_xsr_api_endpoint()
    base_url = url.split("epub_content")[0]

    return base_url + str(path)


def get_epub_info(fname):
    ns = {
        'n': 'urn:oasis:names:tc:opendocument:xmlns:container',
        'pkg': 'http://www.idpf.org/2007/opf',
        'dc': 'http://purl.org/dc/elements/1.1/'
    }

    # prepare to read from the .epub file
    zip = zipfile.ZipFile(fname)

    # find the contents metafile
    txt = zip.read('META-INF/container.xml')
    tree = etree.fromstring(txt)
    cfname = tree.xpath('n:rootfiles/n:rootfile/@full-path', namespaces=ns)[0]

    # grab the metadata block from the contents metafile
    cf = zip.read(cfname)
    tree = etree.fromstring(cf)
    p = tree.xpath('/pkg:package/pkg:metadata', namespaces=ns)[0]

    # repackage the data
    epub_data = {}
    for data in ['creator', 'subject', 'description', 'publisher',
                 'date', 'rights', 'language', 'identifier']:
        try:
            epub_temp = p.xpath('dc:%s/text()' % data, namespaces=ns)[0]
            epub_encode = epub_temp.encode("ascii", "ignore")
            epub_data[data] = epub_encode.decode()
        except IndexError:
            logger.error(data + " Has no value in epub")

    return epub_data


def extract_source():
    """function to parse xml xsr data and convert to dictionary"""

    resp = get_xsr_api_response()
    resp_json = resp.json()

    for data_dict in resp_json:
        if "rootUrl" in data_dict:
            epub_path = append_url(data_dict["rootUrl"])
            data_dict["rootUrl"] = append_root_url(data_dict["rootUrl"])
            try:
                response = requests.get(epub_path)
                file_name = "/media/" + epub_path.split("/")[-1]
                open(file_name, "wb").write(response.content)
                logger.info(file_name)
                extracted_data = get_epub_info(file_name)
                data_dict.update(extracted_data)
            except requests.exceptions.RequestException as e:
                logger.error(e)

        if "coverHref" in data_dict:
            data_dict["coverHref"] = append_url(data_dict["coverHref"])
    resp_dump = json.dumps(resp_json)
    source_data_dict = json.loads(resp_dump)
    logger.info("Retrieving data from source page ")
    source_df_list = [pd.DataFrame(source_data_dict)]
    source_df_final = pd.concat(source_df_list).reset_index(drop=True)
    logger.info("Completed retrieving data from source")
    return source_df_final


def read_source_file():
    """sending source data in dataframe format"""
    logger.info("Retrieving data from XSR")
    # load rss from web to convert to xml
    xsr_items = extract_source()
    # convert xsr dictionary list to Dataframe
    source_df = pd.DataFrame(xsr_items)
    logger.info("Changing null values to None for source dataframe")
    std_source_df = source_df.where(pd.notnull(source_df),
                                    None)
    return [std_source_df]


def get_uuid_from_source(data_dict):
    """Function to find uuid value and return"""
    field = ['identifier']

    for item in field:
        if data_dict.get(item):
            try:
                uuid.UUID(data_dict[item])
                return str(data_dict[item])
            except ValueError:
                return None
    return None


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['uniqueId', 'SOURCESYSTEM']
    field_values = []

    for item in field:
        if not data_dict.get(item):
            logger.error('Field name ' + item + ' is missing for '
                                                'key creation')
            return None
        field_values.append(data_dict.get(item))

    # Key value creation for source metadata
    key_value = '_'.join(field_values)

    # Key value hash creation for source metadata
    key_value_hash = hashlib.sha512(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key


def convert_str_to_date(element, target_data_dict):
    """Convert integer date to date time"""
    key_list = element.split(".")
    check_key_dict = target_data_dict
    check_key_dict = traverse_dict_with_key_list(check_key_dict, key_list)
    if check_key_dict:
        if key_list[-1] in check_key_dict:
            if isinstance(check_key_dict[key_list[-1]], str):
                try:
                    check_key_dict[key_list[-1]] = datetime.datetime.strptime(
                        check_key_dict[key_list[-1]], '%m/%d/%Y')
                except ValueError:
                    logger.error("Incorrect data format, should be MM/DD/YYYY")


def convert_int_to_date(element, target_data_dict):
    """Convert integer date to date time"""
    key_list = element.split(".")
    check_key_dict = target_data_dict
    check_key_dict = traverse_dict_with_key_list(check_key_dict, key_list)
    if check_key_dict:
        if key_list[-1] in check_key_dict:
            if isinstance(check_key_dict[key_list[-1]], int):
                check_key_dict[key_list[-1]] = datetime. \
                    fromtimestamp(check_key_dict[key_list[-1]])


def find_dates(data_dict):
    """Function to convert integer value to date value """

    data_flattened = dict_flatten(data_dict, [])

    for element in data_flattened.keys():
        element_lower = element.lower()
        if (element_lower.find("date") != -1 or element_lower.find(
                "time")) != -1:
            convert_int_to_date(element, data_dict)
            convert_str_to_date(element, data_dict)

    return data_dict


def convert_html(element, target_data_dict):
    """Convert HTML to text data"""
    key_list = element.split(".")
    check_key_dict = target_data_dict
    check_key_dict = traverse_dict_with_key_list(check_key_dict, key_list)
    if check_key_dict:
        if key_list[-1] in check_key_dict:
            check_key_dict[key_list[-1]] = \
                html2text.html2text(check_key_dict[key_list[-1]])


def find_html(data_dict):
    """Function to convert HTML value to text"""
    data_flattened = dict_flatten(data_dict, [])

    for element in data_flattened.keys():
        if data_flattened[element]:
            if bool(BeautifulSoup(str(data_flattened[element]),
                                  "html.parser").find()):
                convert_html(element, data_dict)
    return data_dict
