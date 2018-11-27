### Collector for WFP Food Security's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdx-scraper-wfp-foodsecurity.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdx-scraper-wfp-foodsecurity) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-wfp-foodsecurity/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-wfp-foodsecurity?branch=master)

This collector connects to the [WFP Food Security](http://vam.wfp.org/sites/mvam_monitoring/) website and extracts food security data country by country creating a dataset per country in HDX. It makes in the order of 20 reads from MVAM and 40 read/writes (API calls) to HDX in a one hour period. It saves 1 temporary file per country each less than 5Kb and these are what are uploaded to HDX. These files are then deleted. It runs every month. 


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod
    
 You will also need to supply the universal .useragents.yml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-wfp-foodsecurity** as specified in the parameter *user_agent_lookup*.
 
 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY
### Usage
python run.py

You will need to have a file called .hdxkey in your home directory containing only your HDX key for the script to run. The script was created to automatically register datasets on the [Humanitarian Data Exchange](http://data.humdata.org/) project.