#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from wfp_foodsecurity import get_countriesdata, get_mvamvariables, generate_dataset_and_showcase

from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)

lookup = 'hdxscraper-wfp-foodsecurity'


def main():
    """Generate dataset and create it in HDX"""

    configuration = Configuration.read()
    countries_url = configuration['countries_url']
    indicators_url = configuration['indicators_url']
    mvam_url = configuration['mvam_url']
    showcase_url = configuration['showcase_url']
    showcase_lookup = configuration['showcase_lookup']
    with Download() as downloader:
        countriesdata = get_countriesdata(countries_url, downloader)
        variables = get_mvamvariables(indicators_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countriesdata))
        for countrydata in countriesdata:
            dataset, showcase = generate_dataset_and_showcase(mvam_url, showcase_url, showcase_lookup,
                                                              downloader, countrydata, variables)
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))


