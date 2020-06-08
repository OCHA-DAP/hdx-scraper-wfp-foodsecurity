#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WFP FOOD SECURITY:
-----------------

Reads WFP food security data and creates datasets.

"""
import logging
from datetime import datetime

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.downloader import DownloadError
from slugify import slugify

logger = logging.getLogger(__name__)

headers = ['SvyDate', 'ADM0_NAME', 'ADM1_NAME', 'ADM2_NAME', 'AdminStrata', 'Variable', 'VariableDescription', 'Demographic', 'Mean']
hxltags = {'SvyDate': '#date', 'ADM0_NAME': '#country+name', 'ADM1_NAME': '#adm1+name', 'ADM2_NAME': '#adm2+name',
          'AdminStrata': '#loc+name', 'Variable': '#indicator+code', 'VariableDescription': '#indicator+name',
          'Demographic': '#category', 'Mean': '#indicator+value+num'}


def get_countries(countries_path, downloader):
    """Download a list of countries and provide mapping if necessary.
    A list of dictionaries is returned, each containing the following keys:
    iso3 - ISO 3 country code
    name - country name
    code - WFP country code
    """
    countries = list()

    headers, iterator = downloader.get_tabular_rows(countries_path, headers=1, dict_form=True, format='csv')
    for row in iterator:
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


def generate_dataset_and_showcase(mvam_url, showcase_url, downloader, folder, countrydata, variables):
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

    dateformat = '%Y-%m-%dT%H:%M:%S'
    table = 'pblStatsSum'
    inputrows = get_mvamdata(mvam_url, downloader, table, country_code)

    filename = ('%s.csv' % table).lower()
    resourcedata = {
        'name': table,
        'description': '%s: %s' % (table, title)
    }

    def process_date(row):
        if row['NumObs'] <= 25:
            return None
        row['VariableDescription'] = variables.get(row['Variable'], '')
        svydate = row['SvyDate']
        if svydate is None:
            return None
        svydate = datetime.strptime(svydate, dateformat)
        return {'startdate': svydate, 'enddate': svydate}

    quickcharts = {'hashtag': '#indicator+code', 'values': ['FCS', 'rCSI', 'Proteins'], 'cutdown': 2,
                   'cutdownhashtags': ['#date', '#category', '#indicator+code', '#indicator+value+num']}
    success, results = dataset.generate_resource_from_iterator(
        headers, inputrows, hxltags, folder, filename, resourcedata, date_function=process_date, quickcharts=quickcharts)
    if success is False:
        logger.warning('%s has no data!' % countryname)
        return None, None, None

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title,
        'notes': 'Reports on food security for %s' % countryname,
        'url': showcase_url % iso3,
        'image_url': 'https://media.licdn.com/media/gcrc/dms/image/C5612AQHtvuWFVnGKAA/article-cover_image-shrink_423_752/0?e=2129500800&v=beta&t=00XnoAp85WXIxpygKvG7eGir_LqfxzXZz5lRGRrLUZw'
    })
    showcase.add_tags(tags)
    return dataset, showcase, results['bites_disabled']
