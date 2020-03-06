#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from wfp_foodsecurity import get_countries, get_mvamvariables, generate_dataset_and_showcase

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-wfp-foodsecurity'


def main():
    """Generate dataset and create it in HDX"""

    with Download() as downloader:
        configuration = Configuration.read()
        countries_path = join('config', configuration['countries_filename'])
        indicators_url = configuration['indicators_url']
        mvam_url = configuration['mvam_url']
        showcase_url = configuration['showcase_url']
        countries = get_countries(countries_path, downloader)
        variables = get_mvamvariables(indicators_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countries))
        for folder, country in progress_storing_tempdir('WFPFoodSecurity', countries, 'iso3'):
            dataset, showcase, bites_disabled = \
                generate_dataset_and_showcase(mvam_url, showcase_url, downloader, folder,
                                              country, variables)
            if dataset:
                dataset.update_from_yaml()
                dataset.generate_resource_view(bites_disabled=bites_disabled)
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: WFP Food Security')
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))


