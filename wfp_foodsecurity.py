#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WFP FOOD SECURITY:
-----------------

Reads WFP food security data and creates datasets.

"""
import logging
from datetime import datetime
from os.path import join

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.downloader import DownloadError
from slugify import slugify

logger = logging.getLogger(__name__)


def get_countriesdata(countries_url, downloader):
    """Download a list of countries and provide mapping if necessary.
    A list of dictionaries is returned, each containing the following keys:
    iso3 - ISO 3 country code
    name - country name
    code - WFP country code
    """
    countries = list()

    for row in downloader.get_tabular_rows(countries_url, dict_rows=True, headers=1, format='csv'):
        wfp_name = row['ADM0_NAME']
        code = row['ADM0_CODE']
        iso3, fuzzy = Country.get_iso3_country_code_fuzzy(wfp_name)
        if iso3 is None:
            continue
        countries.append({'iso3': iso3, 'code': code})

    return countries


def get_mvamvariables(indicators_url, downloader):
    return downloader.download_tabular_key_value(indicators_url, headers=1, format='xlsx', sheet='Variable Dictionary')


def checkfor_mvamdata(data_url, downloader, table, country_code):
    response = downloader.setup(data_url, post=True,
                                parameters={'table': table,
                                            'where': "ADM0_CODE=%s" % country_code})
    if 'Transfer-Encoding' in response.headers:
        return True
    else:
        return False


def get_mvamdata(data_url, downloader, table, country_code):
    json = list()
    no = 0

    while 1:
        response = downloader.setup(data_url, post=True,
                                    parameters={'table': table,
                                                'where': "ADM0_CODE=%s" % country_code,
                                                'page': no})
        if 'Transfer-Encoding' in response.headers:
            json += downloader.get_json()
            no += 1
        else:
            if len(json) == 0:
                return None
            return json


def generate_dataset_and_showcase(mvam_url, showcase_url, showcase_lookup, downloader, folder, countrydata, variables):
    """Parse json of the form:
    {
    },
    """
    iso3 = countrydata['iso3']
    countryname = Country.get_country_name_from_iso3(iso3)
    country_code = countrydata['code']
    if not checkfor_mvamdata(mvam_url, downloader, 'pblStatsSum', country_code):
        logger.warning('%s has no data!' % countryname)
        return None, None, None
    title = '%s - Food Security Indicators' % countryname
    logger.info('Creating dataset: %s' % title)
    name = 'WFP Food Security indicators for %s' % countryname
    slugified_name = slugify(name).lower()
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('eda0ee04-7436-47f0-87ab-d1b9edcd3bb9')
    dataset.set_organization('3ecac442-7fed-448d-8f78-b385ef6f84e7')
    dataset.set_expected_update_frequency('Every month')
    dataset.set_subnational(False)
    try:
        dataset.add_country_location(iso3)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None, None

    tags = ['hxl', 'food security', 'indicators']
    dataset.add_tags(tags)

    earliest_year = 10000
    latest_year = 0
    file_type = 'csv'

    dateformat = '%Y-%m-%dT%H:%M:%S'
    table = 'pblStatsSum'
    inputrows = get_mvamdata(mvam_url, downloader, table, country_code)

    rows = list()
    hxlrow = {'SvyDate': '#date', 'ADM0_NAME': '#country+name', 'ADM1_NAME': '#adm1+name', 'ADM2_NAME': '#adm2+name',
              'AdminStrata': '#loc+name', 'Variable': '#indicator+code', 'VariableDescription': '#indicator+name',
              'Demographic': '#category', 'Mean': '#indicator+value+num'}
    rows.append(hxlrow)
    bites_disabled = [True, True, True]
    for row in inputrows:
        if row['NumObs'] <= 25:
            continue
        rows.append(row)
        indicator_code = row['Variable']
        if indicator_code == 'rCSI':
            bites_disabled[0] = False
        elif indicator_code == 'FCS':
            bites_disabled[1] = False
        elif indicator_code == 'Proteins':
            bites_disabled[2] = False
        description = variables.get(indicator_code, '')
        row['VariableDescription'] = description
        svydate = row['SvyDate']
        if svydate is None:
            continue
        svydate = datetime.strptime(svydate, dateformat)
        year = svydate.year
        if year < earliest_year:
            earliest_year = year
        elif year > latest_year:
            latest_year = year

    if earliest_year == 10000 or latest_year == 0:
        logger.warning('%s has no data!' % countryname)
        return None, None, None

    dataset.set_dataset_year_range(earliest_year, latest_year)

    filename = ('%s.%s' % (table, file_type)).lower()
    resource_data = {
        'name': filename,
        'description': '%s: %s' % (table, title)
    }
    resource = Resource(resource_data)
    resource.set_file_type(file_type)
    file_to_upload = join(folder, filename)
    write_list_to_csv(rows, file_to_upload, headers=list(rows[0].keys()))
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)

    showcase_country = showcase_lookup.get(iso3, slugify(countryname.lower()))
    url = showcase_url % showcase_country
    try:
        downloader.setup(url)
    except DownloadError:
        url = showcase_url % showcase_lookup[iso3]
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title,
        'notes': 'Reports on food security for %s' % countryname,
        'url': url,
        'image_url': 'https://media.licdn.com/media/gcrc/dms/image/C5612AQHtvuWFVnGKAA/article-cover_image-shrink_423_752/0?e=2129500800&v=beta&t=00XnoAp85WXIxpygKvG7eGir_LqfxzXZz5lRGRrLUZw'
    })
    showcase.add_tags(tags)
    return dataset, showcase, bites_disabled
