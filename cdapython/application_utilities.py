import os
import re
import pandas as pd

import cda_client

#############################################################################################################################
#
# get_api_url(): Return the current system URL for the CDA REST API.
#
#############################################################################################################################

def get_api_url():
    """
    Return the currently-set URL pointing to the CDA REST API.
    """

    # System default.

    default_api_url = 'http://127.0.0.1:8000'

    # Has the user set a non-default URL?

    local_api_url = os.environ.get( '__CDA_API_URL' )

    if local_api_url is not None and len( local_api_url ) > 0:
        
        return local_api_url

    else:
        
        return default_api_url

#############################################################################################################################
#
# set_api_url(): Set the current system URL for the CDA REST API.
#
#############################################################################################################################

def set_api_url( new_api_url ):
    """
    Set the current system URL for the CDA REST API.
    """

    # Do some basic sanity checking.

    if re.search( r'^https*:\/\/', new_api_url ) is None:
        raise RuntimeError( 'set_api_url(): Only HTTP and HTTPS URLs are allowed.' )
    elif len( new_api_url ) > 100:
        raise RuntimeError( 'set_api_url(): Whatever that was, it wasn\'t the URL of the CDA REST API.' )

    os.environ['__CDA_API_URL'] = new_api_url



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
