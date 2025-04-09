import logging
import logging.config
import os
import re
import pandas as pd

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
# get_logger(): Returns logger instance that uses config file settings to initialize
#
#############################################################################################################################


def get_logger() -> logging.Logger:
    """
    Returns logger instance that uses config file settings to initialize.

    Returns:
        log: logging tool that can be used to output messages of varying granularity
    """

    parent_dir = os.path.dirname( os.path.abspath( __file__ ) )
    log_config = os.path.join( parent_dir, 'config', 'logger.yml' )

    with open(log_config) as log_config_file:
        log_config = yaml.safe_load(log_config_file)

    logging.config.dictConfig(log_config)
    logger = logging.getLogger("simple")
    return logger





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
# set_log_level(): Changes the current log level
#
#############################################################################################################################


def set_log_level(log, debug=False, loglevel="INFO"):
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
        # print('debug is true...')
        for handler in log.handlers:
            handler.setLevel('DEBUG')

    elif debug == False and loglevel in get_available_log_levels():
        # print('debug is false...')
        for handler in log.handlers:
            handler.setLevel(loglevel)

    else:
        for handler in log.handlers:
            handler.setLevel('WARNING')
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


#############################################################################################################################
#
# cleanup_match_statement(column_data, match_statement) : Parse `match_*` filter expressions: complain if
#
#     * requested columns don't exist
#     * illegal or type-inappropriate operators are used
#     * filter values don't match the data types of the columns they're paired with
#     * wildcards appear anywhere but at the ends of a filter string
#
# ...and recombine elements for use in querying
#
#############################################################################################################################


def cleanup_match_statement(column_data, match_statement):
    """
    Parse `match_*` filter expressions and transform for validity with API

    Arguments:
        column_data ( list of strings; required ):
            Result of columns() call. Parameterized to save from calling columns() multiple times

        match_statement ( list of strings; optional ):
            One or more conditions, expressed as filter strings

    Returns:
        List of transformed and cleaned up match statements, or an empty list if no inputs were given

    """
    queries_for_match_statement = []

    if len(match_statement) == 0:
        return queries_for_match_statement

    #############################################################################################################################
    # Define the list of supported filter-string operators.

    allowed_operators = {">", ">=", "<", "<=", "=", "!=", "like"}

    #############################################################################################################################
    # Enumerate restrictions on operator use to appropriate data types.

    operators_by_data_type = {
        "bigint": allowed_operators,
        "boolean": {"=", "!="},
        "integer": allowed_operators,
        "numeric": allowed_operators,
        "text": {"=", "!=", "like"},
    }

    #############################################################################################################################
    # Enable aliases for various ways to say "True" and "False". (Case will be lowered as soon as each literal is received.)

    boolean_alias = {"true": "true", "t": "true", "false": "false", "f": "false"}

    for item in match_statement:
        # Try to extract a column name from this filter expression. Don't be case-sensitive.

        filter_column_name = re.sub(r"^([\S]+)\s.*", r"\1", item).lower()

        # Let's see if this thing exists.

        filter_column_metadata = column_data.query(f'column == "{filter_column_name}"')

        if filter_column_metadata is None or len(filter_column_metadata) != 1:
            raise RuntimeError(f"ERROR: requested column '{filter_column_name}' is not a searchable CDA column.")

        # See what the operator is.

        filter_operator = re.sub(r"^\S+\s+(\S+)\s.*", r"\1", item)

        if filter_operator == "==":
            # Be kind to computer scientists.

            filter_operator = "="

        elif filter_operator not in allowed_operators:
            raise RuntimeError(f"ERROR:  operator '{filter_operator}' is not supported.")

        # Identify the data type in the column being filtered.

        target_data_type = filter_column_metadata["data_type"].iloc[0]

        # Make sure the operator specified is allowed for the data type of the column being filtered.

        if filter_operator not in operators_by_data_type[target_data_type]:
            raise RuntimeError(
                f"ERROR: operator '{filter_operator}' is not usable for values of type '{target_data_type}'."
            )

        # We said quotes weren't required for string values. Doesn't technically mean they can't be used. Remove them.

        filter_value = re.sub(r"^\S+\s+\S+\s+(\S.*)$", r"\1", item)

        filter_value = re.sub(r"""^['"]+""", r"", filter_value)
        filter_value = re.sub(r"""['"]+$""", r"", filter_value)

        # Validate VALUE types and process wildcards.

        if target_data_type != "text":
            # Ignore leading and trailing whitespace unless we're dealing with strings.

            filter_value = re.sub(r"^\s+", r"", filter_value)
            filter_value = re.sub(r"\s+$", r"", filter_value)

        if filter_value.lower() != "null":
            if target_data_type == "boolean":
                # If we're supposed to be in a boolean column, make sure we've got a true/false value.

                filter_value = filter_value.lower()

                if filter_value not in boolean_alias:
                    raise RuntimeError(
                        f"ERROR: requested column {filter_column_name} has data type 'boolean', requiring a true/false value; you specified '{filter_value}', which is neither."
                    )

                else:
                    filter_value = boolean_alias[filter_value]

            elif target_data_type in ["bigint", "integer", "numeric"]:
                # If we're supposed to be in a numeric column, make sure we've got a number.

                if re.search(r"^[-+]?\d+(\.\d+)?$", filter_value) is None:
                    raise RuntimeError(
                        f"ERROR: requested column {filter_column_name} has data type '{target_data_type}', requiring a number value; you specified '{filter_value}', which is not."
                    )

            elif target_data_type == "text":
                # Check for wildcards: if found, adjust operator and
                # wildcard syntax to match API expectations on incoming queries.

                original_filter_value = filter_value

                if re.search(r"^\*", filter_value) is not None or re.search(r"\*$", filter_value) is not None:
                    filter_value = re.sub(r"^\*+", r"%", filter_value)

                    filter_value = re.sub(r"\*+$", r"%", filter_value)

                    if filter_operator == "!=":
                        filter_operator = "NOT LIKE"

                    else:
                        filter_operator = "LIKE"

                if re.search(r"\*", filter_value) is not None:
                    raise RuntimeError(
                        f"ERROR: wildcards (*) are only allowed at the ends of string values; string '{original_filter_value}' is noncompliant (it has one in the middle). Please fix."
                    )

            else:
                # Just to be safe. Types change.

                raise RuntimeError(
                    f"ERROR: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers."
                )

        filtered_match_statement = filter_column_name + " " + filter_operator + " " + filter_value

        queries_for_match_statement.append(filtered_match_statement)

    return queries_for_match_statement


def cleanup_inputs(match_all, match_any, add_columns, exclude_columns, data_source):
    # Listify, so we don't have to care later about whether this was a string or a list of strings.
    if isinstance(match_all, str):
        match_all = [match_all]
    if isinstance(match_any, str):
        match_any = [match_any]
    if isinstance(add_columns, str):
        add_columns = [add_columns]
    if isinstance(exclude_columns, str):
        exclude_columns = [exclude_columns]
    if isinstance(data_source, str):
        data_source = [data_source]

    return match_all, match_any, add_columns, exclude_columns, data_source

def verify_inputs(
        column_values,
        match_all, 
        match_any, 
        add_columns, 
        exclude_columns, 
        data_source,
        table,
        match_from_file,
        link_to_table,
        provenance,
        return_data_as,
        output_file,
        count_only,
        log
        ):
    # Top-level type and sanity checking (i.e. not examining list contents yet): ensure nothing untoward got passed into our parameters.
    table_results = pd.DataFrame()

    if column_values is None:
        log.critical(
            "fetch_rows(): ERROR: Something went fatally wrong with columns(); can't complete tables(), aborting."
        )
        return

    else:
        # So - yes we have a function "def tables()" that does this already, but since we already have the columns data
        # we use this one-liner to extract the tables.
        table_results = sorted(column_values["table"].unique())

    # Make sure the requested table exists.
    if table is None or not isinstance(table, str) or table not in table_results:
        log.critical(
            f"fetch_rows(): ERROR: The required parameter 'table' must be a searchable CDA table; you supplied '{table}', which is not. Please run tables() for a list."
        )

        return

    # `match_all`
    if not isinstance(match_all, list):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to 'match_all' parameter must be a filter string or a list of filter strings; you specified '{match_all}', which is neither."
        )

        return

    # `match_any`
    if not isinstance(match_any, list):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to 'match_any' parameter must be a filter string or a list of filter strings; you specified '{match_any}', which is neither."
        )

        return

    # `match_from_file`
    if not isinstance(match_from_file, dict):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not."
        )

        return

    else:
        received_keys = set(match_from_file.keys())

        expected_keys = {"input_file", "input_column", "cda_column_to_match"}

        if received_keys != expected_keys:
            log.critical(
                f"fetch_rows(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not."
            )

            return

    # `data_source`
    if not isinstance(data_source, list):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to the 'data_source' parameter must be a string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither."
        )

        return

    # `add_columns`
    if not isinstance(add_columns, list):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to 'add_columns' parameter must be a string (e.g. 'primary_diagnosis_site') or a list of strings (e.g. [ 'specimen_type', 'primary_diagnosis_condition' ]); you specified '{add_columns}', which is neither."
        )

        return

    # `exclude_columns`
    if not isinstance(exclude_columns, list):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to 'exclude_columns' parameter must be a string (e.g. 'primary_diagnosis_site') or a list of strings (e.g. [ 'specimen_type', 'primary_diagnosis_condition' ]); you specified '{exclude_columns}', which is neither."
        )

        return

    # `link_to_table`
    if not isinstance(link_to_table, str):
        log.critical(
            f"fetch_rows(): ERROR: parameter 'link_to_table' must be a string; you supplied '{link_to_table}', which is not."
        )

        return

    elif link_to_table != "" and link_to_table not in table_results:
        log.critical(
            f"fetch_rows(): ERROR: parameter 'link_to_table' must be the name of a searchable CDA table; you provided '{link_to_table}', which is not. See tables() for a list of valid table names."
        )

        return

    elif link_to_table != "" and link_to_table == table:
        log.critical(
            "fetch_rows(): ERROR: parameter 'link_to_table' can't specify the same table as the 'table' parameter. Please try again."
        )

        return

    # `provenance`
    if provenance != True and provenance != False:
        log.critical(
            f"fetch_rows(): ERROR: The `provenance` parameter must be set to True or False; you specified '{provenance}', which is neither."
        )

        return
    # `return_data_as`
    if not isinstance(return_data_as, str):
        log.critical(
            f"fetch_rows(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe' or 'tsv'."
        )

        return

    # `output_file`
    if not isinstance(output_file, str):
        log.critical(
            f"fetch_rows(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path."
        )

        return

    # `count_only`
    if count_only != True and count_only != False:
        log.critical(
            f"fetch_rows(): ERROR: The `count_only` parameter must be set to True or False; you specified '{count_only}', which is neither."
        )

        return


