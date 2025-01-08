import logging
import logging.config
import os
from pathlib import Path

import yaml

import cda_client
import cda_client.api
import cda_client.api.columns
import cda_client.api.columns.columns_endpoint_columns_get
import cda_client.api.data
import cda_client.api.data.file_fetch_rows_endpoint_data_file_post
import cda_client.api.data.subject_fetch_rows_endpoint_data_subject_post
import cda_client.api.summary
import cda_client.api.unique_values
import cda_client.api.unique_values.unique_values_endpoint_unique_values_columnname_post

#############################################################################################################################
#
# get_api_client(): Returns logger instance that uses config file settings to initialize
#
#############################################################################################################################


def get_logger() -> logging.Logger:
    """
    Returns logger instance that uses config file settings to initialize.

    Returns:
        log: logging tool that can be used to output messages of varying granularity
    """

    parent_dir = Path(__file__).parent
    log_config = Path(parent_dir / "config/logger.yml").resolve()
    with open(log_config) as log_config_file:
        log_config = yaml.safe_load(log_config_file)

    logging.config.dictConfig(log_config)
    logger = logging.getLogger("simple")
    return logger


log = get_logger()


#############################################################################################################################
#
# get_available_log_levels(): Returns list of log level strings that can be used to set_log_level
#
#############################################################################################################################


def get_available_log_levels():
    """
    Returns list of log level strings that can be used to set_log_level.

    Returns:
        list of strings: names of log levels that can be passed to set_log_level.
    """
    return {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


#############################################################################################################################
#
# set_default_log_level(): Changes the current log level to be 'WARNING", our default level.
#
#############################################################################################################################


def set_default_log_level():
    """
    Sets default log level ('WARNING').

    """
    log.setLevel("WARNING")


#############################################################################################################################
#
# set_log_level(): Changes the current log level
#
#############################################################################################################################


def set_log_level(debug=False, loglevel="WARNING"):
    """
    Changes the current log level

    Returns:
        query_api_instance: query api instance that can be used to communicate with CDA API
    """
    loglevel = loglevel.upper()

    if debug != True and debug != False:
        log.error(
            f"set_log_level(): ERROR: The `debug` parameter must be set to True or False; you specified '{debug}', which is neither."
        )
        return

    elif debug == True:
        log.setLevel("DEBUG")

    elif debug == False and loglevel in get_available_log_levels():
        log.setLevel(loglevel)

    else:
        log.setLevel("WARNING")
        log.warning(
            f"set_log_level(): loglevel set to '{loglevel}'. Should be one of 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'. Setting to 'WARNING'."
        )


#############################################################################################################################
#
# get_api_client(): Return an ApiClient object containing the information necessary to connect to the CDA database.
#
#############################################################################################################################


def get_api_client():
    """
    Return an ApiClient object.

    Returns:
        query_api_instance: query api instance that can be used to communicate with CDA API
    """
    # Allow users to override the system-default URL for the CDA API by setting their CDA_API_URL
    # environment variable.

    url = "http://127.0.0.1:8000"

    url_override = os.environ.get("CDA_API_URL")

    if url_override is not None and len(url_override) > 0:
        url = url_override

    # Alter our debug reports a bit if we're only counting results, instead of fetching them.
    return cda_client.Client(base_url=url)


#############################################################################################################################
#
# get_data_api_client(): Return a data ApiClient object containing the information necessary to connect to the CDA database.
#
#############################################################################################################################


def get_data_api_client(table):
    """
    Return an Data ApiClient object.

    Returns:
        data_api: query api instance that can be used to communicate with CDA API data endpoint
    """

    # Create an instance of the API class
    if table == "file":
        with get_api_client() as client:
            data_client = cda_client.api.data.file_fetch_rows_endpoint_data_file_post.sync(client=client)

    elif table == "subject":
        with get_api_client() as client:
            data_client = cda_client.api.data.subject_fetch_rows_endpoint_data_subject_post.sync(client=client)
    return data_client


#############################################################################################################################
#
# get_columns_api_client(): Return a columns ApiClient object containing the information necessary to connect to the CDA database.
#
#############################################################################################################################


def get_columns_api_client():
    """
    Return a columns ApiClient object.

    Returns:
        columns_api: query api instance that can be used to communicate with CDA API columns endpoint
    """

    # Create an instance of the API class
    with get_api_client() as client:
        columns_client = cda_client.api.columns.columns_endpoint_columns_get.sync(client=client)
    return columns_client


#############################################################################################################################
#
# get_unique_values_api_client(): Return a columns ApiClient object containing the information necessary to connect to the CDA database.
#
#############################################################################################################################


def get_unique_values_api_client():
    """
    Return a unique values ApiClient object.

    Returns:
        unique_values_api: unique values api instance that can be used to communicate with CDA API  endpoint
    """

    # Create an instance of the API class
    with get_api_client() as client:
        unique_values_client = cda_client.api.unique_values.unique_values_endpoint_unique_values_columnname_post.sync(
            client=client
        )
    return unique_values_client
