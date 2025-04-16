import os
import re
import pandas as pd

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
        provenance,
        return_data_as,
        output_file,
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
        
    if match_from_file["cda_column_to_match"] == "":
        if match_from_file["input_file"] != "" or match_from_file["input_column"] != "":
            log.critical(
                f"fetch_rows(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that."
            )

            return

    elif match_from_file["input_file"] == "":
        if match_from_file["cda_column_to_match"] != "" or match_from_file["input_column"] != "":
            log.critical(
                f"fetch_rows(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that."
            )

            return

    elif match_from_file["input_column"] == "":
        if match_from_file["cda_column_to_match"] != "" or match_from_file["input_file"] != "":
            log.critical(
                f"fetch_rows(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that."
            )

            return
        
    if match_from_file["input_file"] != '' and  match_from_file["input_file"] == output_file:
            log.critical(
                f"fetch_rows(): ERROR: You specified the same file ('{output_file}') as both a source of filter values (via 'match_from_file') and the target output file ( via 'output_file'). Please make sure these two files are different."
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


def build_match_from_file_filter(match_from_file, target_data_type, log):
    match_from_file_input_file = match_from_file["input_file"]

    match_from_file_source_column_name = match_from_file["input_column"]

    match_from_file_target_values = set()

    # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
    # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
    # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
    # anger and confusion.

    match_from_file_nulls_allowed = False

    # Make sure the dictionary values are either all null or all not null.      

    try:
        with open(match_from_file_input_file) as IN:
            column_names = next(IN).rstrip("\n").split("\t")

            if match_from_file_source_column_name not in column_names:
                log.critical(
                    f"fetch_rows(): ERROR: TSV column '{match_from_file_source_column_name}' (specified in your 'match_from_file' parameter) does not exist. Columns in your specified input file ('{match_from_file_input_file}') are:\n\n    {column_names}\n"
                )

                return

            else:
                for line in [next_line.rstrip("\n") for next_line in IN]:
                    record = dict(zip(column_names, line.split("\t")))

                    target_value = record[match_from_file_source_column_name]

                    if target_value is None or target_value == "" or target_value == "<NA>":
                        # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
                        # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
                        # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
                        # anger and confusion.

                        match_from_file_nulls_allowed = True

                    else:
                        match_from_file_target_values.add(target_value)
        

    except Exception as error:
        log.critical(
            f"fetch_rows(): ERROR: Couldn't load requested column '{match_from_file_source_column_name}' from requested TSV file '{match_from_file_input_file}': got error of type '{type(error)}', with error message '{error}'."
        )

        return
    
    
    boolean_alias = {"true": "true", "t": "true", "false": "false", "f": "false"}
    processed_target_values = set()

    for target_value in match_from_file_target_values:
        # Validate value types and test for wildcards.

        if target_data_type != "text":
            # Ignore leading and trailing whitespace unless we're dealing with strings.

            target_value = re.sub(r"^\s+", r"", target_value)
            target_value = re.sub(r"\s+$", r"", target_value)

        if target_data_type == "boolean":
            # If we're supposed to be in a boolean column, make sure we've got a true/false value.

            target_value = target_value.lower()

            if target_value not in boolean_alias:
                log.error(f"fetch_rows(): match_from_file: requested column {match_from_file["cda_column_to_match"]} has data type 'boolean', requiring a true/false value; you specified '{target_value}', which is neither.")

                raise Exception

            else:
                target_value = boolean_alias[target_value]

        elif target_data_type in ["bigint", "integer", "numeric"]:
            # If we're supposed to be in a numeric column, make sure we've got a number.

            if re.search(r"^[-+]?\d+(\.\d+)?$", target_value) is None:
                log.error(f"fetch_rows(): match_from_file: requested column {match_from_file["cda_column_to_match"]} has data type '{target_data_type}', requiring a number value; you specified '{target_value}', which is not.",)

                raise Exception

        elif target_data_type == "text":
            # Check for wildcards: if found, vomit.

            if re.search(r"\*", target_value) is not None:
                log.error(f"fetch_rows(): ERROR: match_from_file: wildcards (*) are disallowed here (only exact matches are supported for this option); string '{target_value}' is noncompliant. Please fix.")

                raise Exception

        else:
            # Just to be safe. Types change.

            log.error(f"fetch_rows(): ERROR: match_from_file: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.",)

            raise Exception

        processed_target_values.add(target_value)

    match_list = ",".join(sorted(processed_target_values))

    return f'{match_from_file["cda_column_to_match"]} IN [{match_list}]'
