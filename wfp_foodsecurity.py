#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
SCRAPERNAME:
------------

Reads ScraperName JSON and creates datasets.

"""
import json
import logging
from collections import OrderedDict
from datetime import datetime
from os.path import join
from tempfile import gettempdir

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import write_list_to_csv
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
    if int(response.headers['Content-Length']) < 125:
        return False
    else:
        return True


def get_mvamdata(data_url, downloader, table, country_code, indicator_code):
    response = downloader.setup(data_url, post=True,
                                parameters={'table': table,
                                            'where': "ADM0_CODE=%s AND VARIABLE='%s'" % (country_code, indicator_code)})
    if int(response.headers['Content-Length']) < 125:
        return None
    else:
        return downloader.get_json()


def process_mvamdata(mvam_url, downloader, table, country_code, variable_code, description, dataset, file_type, folder):
    start_year = 10000
    end_year = 0

    data = get_mvamdata(mvam_url, downloader, table, country_code, variable_code)
    if not data:
        return start_year, end_year

    dateformat = '%Y-%m-%dT%H:%M:%S'

    filename = slugify('%s-%s.%s' % (table, variable_code, file_type)).lower()
    resource_data = {
        'name': filename,
        'description': '%s: %s' % (table, description)
    }
    resource = Resource(resource_data)
    resource.set_file_type(file_type)
    file_to_upload = join(folder, filename)
    for rowdict in data:
        svydate = datetime.strptime(rowdict['SvyDate'], dateformat)
        year = svydate.year
        if year < start_year:
            start_year = year
        if year > end_year:
            end_year = year
    write_list_to_csv(data, file_to_upload, headers=list(data[0].keys()))
    resource.set_file_to_upload(file_to_upload)
    dataset.add_update_resource(resource)
    return start_year, end_year


def generate_dataset_and_showcase(mvam_url, showcase_url, showcase_lookup, downloader, countrydata, variables):
    """Parse json of the form:
    {
    },
    """
    iso3 = countrydata['iso3']
    countryname = Country.get_country_name_from_iso3(iso3)
    country_code = countrydata['code']
    if not checkfor_mvamdata(mvam_url, downloader, 'pblStatsSum', country_code):
        logger.exception('%s has no data!' % countryname)
        return None, None
    title = '%s - Food Security Indicators' % countryname
    logger.info('Creating dataset: %s' % title)
    name = 'WFP Food Security indicators for %s' % countryname
    slugified_name = slugify(name).lower()
    dataset = Dataset({
        'name': slugified_name,
        'title': title,
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('3ecac442-7fed-448d-8f78-b385ef6f84e7')
    dataset.set_expected_update_frequency('daily')
    try:
        dataset.add_country_location(iso3)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None

    tags = ['food security']
    dataset.add_tags(tags)

    earliest_year = 10000
    latest_year = 0
    folder = gettempdir()
    file_type = 'csv'

    for variable_code in variables:
        description = variables[variable_code]
        start_year, end_year = process_mvamdata(mvam_url, downloader, 'pblStatsSum', country_code, variable_code,
                                                description, dataset, file_type, folder)
        if start_year < earliest_year:
            earliest_year = start_year
        if end_year > latest_year:
            latest_year = end_year
        start_year, end_year = process_mvamdata(mvam_url, downloader, 'pblStatsSum4Maps', country_code, variable_code,
                                                description, dataset, file_type, folder)
        if start_year < earliest_year:
            earliest_year = start_year
        if end_year > latest_year:
            latest_year = end_year

    if len(dataset.get_resources()) == 0:
        logger.exception('%s has no data!' % countryname)
        return None, None
    dataset.set_dataset_year_range(earliest_year, latest_year)
    showcase_country = showcase_lookup.get(iso3, slugify(countryname.lower()))
    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title,
        'notes': 'Reports on food security for %s' % countryname,
        'url': showcase_url % showcase_country,
        'image_url': 'https://media.licdn.com/media/gcrc/dms/image/C5612AQHtvuWFVnGKAA/article-cover_image-shrink_423_752/0?e=2129500800&v=beta&t=00XnoAp85WXIxpygKvG7eGir_LqfxzXZz5lRGRrLUZw'
    })
    showcase.add_tags(tags)
    return dataset, showcase
