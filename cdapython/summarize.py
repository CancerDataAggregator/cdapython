import json
import os
import pandas as pd
import re
import tabulate

import cda_client

from multiprocessing.pool import ApplyResult

from cdapython.application_utilities import get_api_url
from cdapython.discover import columns, tables
from cdapython.logging_wrappers import get_logger
from cdapython.validation import validate_and_transform_match_filter_list

from cda_client.errors import UnexpectedStatus
from cda_client.models.q_node import QNode
from cda_client.api.summary import file_summary_endpoint_summary_file_post as summary_file_endpoint
from cda_client.api.summary import subject_summary_endpoint_summary_subject_post as summary_subject_endpoint


#############################################################################################################################
#############################################################################################################################
# Nomenclature notes:
#
# * try to standardize all potential user-facing synonyms for basic database data structures
#   (field, entity, endpoint, cell, value, term, etc.) to "table", "column", "row" and "value".
#############################################################################################################################
#############################################################################################################################


#############################################################################################################################
#
# summarize_files(): Get a report describing columns of interest in the CDA file table, summarizing column values over
#                    all files matching user-supplied query filters. Optionally add columns from other tables, which are
#                    summarized across all of their own rows that are directly related to files matching the given filters.
#
#############################################################################################################################

def summarize_files(
    *,
    return_data_as="",
    output_file="",
    match_all=[],
    match_any=[],
    match_from_file={"input_file": "", "input_column": "", "cda_column_to_match": ""},
    data_source=[],
    add_columns=[],
    exclude_columns=[]
):
    """
    For a set of CDA file rows that all match a user-specified set of filters --
    "result rows" -- get a report showing counts of values present in that
    set of rows, profiled across (user-modifiable) columns of interest.

    Arguments:
        return_data_as ( string; optional: 'dataframe_list' or 'dict' or 'json' ):
            Specify how to return results: as a list of pandas DataFrames, as a
            Python dictionary, or as output written to a JSON file named by the user.
            If this argument is omitted, then for each DataFrame that would have
            been returned by the 'dataframe_list' option, a table will be
            pretty-printed to the standard output stream (and nothing will be returned).

        output_file( string; optional ):
            If return_data_as='json' is specified, output_file should contain a
            resolvable path to a file into which JSON-formatted results will be
            written.

        match_all ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            ALL of which must be met by all result rows.

        match_any ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            AT LEAST ONE of which must be met by all result rows.

        match_from_file ( 3-element dictionary of strings; optional ):
            A dictionary containing 3 named elements:
                1. 'input_file': The name of a (local) TSV file (with column names in its first row)
                2. 'input_column': The name of a column in that TSV
                3. 'cda_column_to_match': The name of a CDA column
            Restrict result rows to those where the value of the given CDA
            column matches at least one value from the given column
            in the given TSV file.

        data_source ( string or list of strings; optional ):
            Restrict results to those deriving from the given upstream
            data source(s). Current valid values are 'GDC', 'IDC', 'PDC',
            'CDS' and 'ICDC'. (Default: no filter.)

        add_columns ( string or list of strings; optional ):
            One or more columns from a second table to add to summary output.

        exclude_columns ( string or list of strings; optional ):
            One or more columns to remove from summary output.

    Filter strings:
        Filter strings are expressions of the form "COLUMN_NAME OP VALUE"
        (note in particular that the whitespace surrounding OP is required),
        where

            COLUMN_NAME is a searchable CDA column (see the columns() function
            for details)

            OP is one of: < <=  > >= = !=

            VALUE is a particular value of whatever data type is stored
            in COLUMN_NAME (see the columns() function for details), or
            the special keyword NULL, indicating the filter should match
            missing (null) values in COLUMN_NAME.

        Operators = and != will work on numeric, boolean and string VALUEs.

        Operators < <= > >= will only work on numeric VALUEs.

        Users can require partial matches to string VALUEs by adding * to either or
        both ends. For example:

            primary_disease_type = *duct*
            sex = F*
            size < 100

        String VALUEs need not be quoted inside of filter strings. For example, to include
        the filters specified just above in the `match_all` argument, we can write:

            summarize_files( match_all=[ 'primary_disease_type = *duct*', 'sex = F*', 'size < 100' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get a summary report for CDA files where the `access` field
        is missing data, we can write:

            summarize_files( match_all=[ 'access = NULL' ] )

    Returns:

        list of pandas DataFrames, with one DataFrame for each summarized column,
        enumerating counts (or statistically summarizing unbounded numeric values) over all
        of that column's data values appearing in any CDA file rows that match the
        user-specified filter criteria (the 'result rows'). Two DataFrames in this list --
        'number_of_matching_files' and 'number_of_subjects_related_to_matching_files' --
        will contain integers representing the total number of result file rows and the
        total number of related subjects, respectively. Every other DataFrame in the list
        will be titled with a CDA column name and will contain value counts or statistical
        summaries for that column as filtered by the result row set.

        OR Python dictionary enumerating counts of all data values for each summarized column
        (or a statistical summary of those data values, in the case of unbounded numeric data)
        across all CDA file rows that match the user-specified filter criteria (the 'result rows').
        Two summary keys in this dictionary -- 'number_of_matching_files' and
        'number_of_subjects_related_to_matching_files' -- will point to integers representing
        the total number of result file rows and the total number of associated subject rows,
        respectively. Every other key in the dictionary will contain a CDA column name; every
        dictionary value will itself be a dictionary either enumerating observed counts of all
        values appearing in that column as filtered by the result row set, or encoding a
        statistical summary of those values in the case of unbounded numeric data.

        OR JSON-formatted text representing the same structure as the `return_data_as='dict'`
        option, written to `output_file`.

        OR returns nothing, but displays a series of tables to standard output
        describing the same data returned by the other `return_data_as` options.

        And yes, we know how those first two paragraphs look. We apologize to the entire English language.
    """

    return summarize( table='file', return_data_as=return_data_as, output_file=output_file, match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns )

#############################################################################################################################
#
# END summarize_files()
#
#############################################################################################################################

#############################################################################################################################
#
# summarize_subjects(): Get a report describing columns of interest in the CDA subject table, summarizing column values over
#                    all subjects matching user-supplied query filters. Optionally add columns from other tables, which are
#                    summarized across all of their own rows that are directly related to subjects matching the given filters.
#
#############################################################################################################################

def summarize_subjects(
    *,
    return_data_as="",
    output_file="",
    match_all=[],
    match_any=[],
    match_from_file={"input_file": "", "input_column": "", "cda_column_to_match": ""},
    data_source=[],
    add_columns=[],
    exclude_columns=[]
):
    """
    For a set of CDA subject rows that all match a user-specified set of filters --
    "result rows" -- get a report showing counts of values present in that
    set of rows, profiled across (user-modifiable) columns of interest.

    Arguments:
        return_data_as ( string; optional: 'dataframe_list' or 'dict' or 'json' ):
            Specify how to return results: as a list of pandas DataFrames, as a
            Python dictionary, or as output written to a JSON file named by the user.
            If this argument is omitted, then for each DataFrame that would have
            been returned by the 'dataframe_list' option, a table will be
            pretty-printed to the standard output stream (and nothing will be returned).

        output_file( string; optional ):
            If return_data_as='json' is specified, output_file should contain a
            resolvable path to a file into which JSON-formatted results will be written.

        match_all ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            ALL of which must be met by all result rows.

        match_any ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            AT LEAST ONE of which must be met by all result rows.

        match_from_file ( 3-element dictionary of strings; optional ):
            A dictionary containing 3 named elements:
                1. 'input_file': The name of a (local) TSV file (with column names in its first row)
                2. 'input_column': The name of a column in that TSV
                3. 'cda_column_to_match': The name of a CDA column
            Restrict result rows to those where the value of the given CDA
            column matches at least one value from the given column
            in the given TSV file.

        data_source ( string or list of strings; optional ):
            Restrict results to those deriving from the given upstream
            data source(s). Current valid values are 'GDC', 'IDC', 'PDC',
            'CDS' and 'ICDC'. (Default: no filter.)

        add_columns ( string or list of strings; optional ):
            One or more columns from a second table to add to summary output.

        exclude_columns ( string or list of strings; optional ):
            One or more columns to remove from summary output.

    Filter strings:
        Filter strings are expressions of the form "COLUMN_NAME OP VALUE"
        (note in particular that the whitespace surrounding OP is required),
        where

            COLUMN_NAME is a searchable CDA column (see the columns() function
            for details)

            OP is one of: < <=  > >= = !=

            VALUE is a particular value of whatever data type is stored
            in COLUMN_NAME (see the columns() function for details), or
            the special keyword NULL, indicating the filter should match
            missing (null) values in COLUMN_NAME.

        Operators = and != will work on numeric, boolean and string VALUEs.

        Operators < <= > >= will only work on numeric VALUEs.

        Users can require partial matches to string VALUEs by adding * to either or
        both ends. For example:

            primary_disease_type = *duct*
            sex = F*
            size < 100

        String VALUEs need not be quoted inside of filter strings. For example, to include
        the filters specified just above in the `match_all` argument, we can write:

            summarize_subjects( match_all=[ 'primary_disease_type = *duct*', 'sex = F*' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get a summary report for CDA subjects where the `year_of_birth` field
        is missing data, we can write:

            summarize_subjects( match_all=[ 'year_of_birth = NULL' ] )

    Returns:

        list of pandas DataFrames, with one DataFrame for each summarized column,
        enumerating counts (or statistically summarizing unbounded numeric values) over all
        of that column's data values appearing in any CDA subject rows that match the
        user-specified filter criteria (the 'result rows'). Two DataFrames in this list --
        'number_of_matching_subjects' and 'number_of_files_related_to_matching_subjects' --
        will contain integers representing the total number of result subject rows and the
        total number of related files, respectively. Every other DataFrame in the list
        will be titled with a CDA column name and will contain value counts or statistical
        summaries for that column as filtered by the result row set.

        OR Python dictionary enumerating counts of all data values for each summarized column
        (or a statistical summary of those data values, in the case of unbounded numeric data)
        across all CDA subject rows that match the user-specified filter criteria (the 'result rows').
        Two summary keys in this dictionary -- 'number_of_matching_subjects' and
        'number_of_files_related_to_matching_subjects' -- will point to integers representing
        the total number of result subject rows and the total number of associated file rows,
        respectively. Every other key in the dictionary will contain a CDA column name; every
        dictionary value will itself be a dictionary either enumerating observed counts of all
        values appearing in that column as filtered by the result row set, or encoding a
        statistical summary of those values in the case of unbounded numeric data.

        OR JSON-formatted text representing the same structure as the `return_data_as='dict'`
        option, written to `output_file`.

        OR returns nothing, but displays a series of tables to standard output
        describing the same data returned by the other `return_data_as` options.

        And yes, we know how those first two paragraphs look. We apologize to the entire English language.
    """

    return summarize( table='subject', return_data_as=return_data_as, output_file=output_file, match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns )

#############################################################################################################################
#
# END summarize_subjects()
#
#############################################################################################################################

#############################################################################################################################
#
# summarize(): Get a report describing columns of interest in a user-specified CDA table, summarizing column values over
#                    all rows matching user-supplied query filters. Optionally add columns from other tables, which are
#                    summarized across all of their own rows that are directly related to rows from the main table that
#                    match the given filters.
#
#############################################################################################################################

def summarize(
    table="",
    *,
    return_data_as="",
    output_file="",
    match_all=[],
    match_any=[],
    match_from_file={"input_file": "", "input_column": "", "cda_column_to_match": ""},
    data_source=[],
    add_columns=[],
    exclude_columns=[]
):
    """
    For a set of rows in a user-specified table that all match a user-specified set of
    filters -- "result rows" -- get a report showing counts of values present in that
    set of rows, profiled across (user-modifiable) columns of interest.

    Arguments:
        table ( string; required: 'file' or 'subject' ):
            The CDA table to be queried and summarized.

        return_data_as ( string; optional: 'dataframe_list' or 'dict' or 'json' ):
            Specify how summarize() should return results: as a list
            of pandas DataFrames, as a Python dictionary, or as output written to a
            JSON file named by the user.  If this argument is omitted,
            summarize() will, for each DataFrame that would have been returned
            by the 'dataframe_list' option, print a table to the standard output
            stream (and nothing will be returned).

        output_file( string; optional ):
            If return_data_as='json' is specified, output_file should contain a
            resolvable path to a file into which summarize() will write
            JSON-formatted results.

        match_all ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            ALL of which must be met by all result rows.

        match_any ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            AT LEAST ONE of which must be met by all result rows.

        match_from_file ( 3-element dictionary of strings; optional ):
            A dictionary containing 3 named elements:
                1. 'input_file': The name of a (local) TSV file (with column names in its first row)
                2. 'input_column': The name of a column in that TSV
                3. 'cda_column_to_match': The name of a CDA column
            Restrict result rows to those where the value of the given CDA
            column matches at least one value from the given column
            in the given TSV file.

        data_source ( string or list of strings; optional ):
            Restrict results to those deriving from the given upstream
            data source(s). Current valid values are 'GDC', 'IDC', 'PDC',
            'CDS' and 'ICDC'. (Default: no filter.)

        add_columns ( string or list of strings; optional ):
            One or more columns from a second table to add to summary output for `table`.

        exclude_columns ( string or list of strings; optional ):
            One or more columns to remove from summary output.

    Filter strings:
        Filter strings are expressions of the form "COLUMN_NAME OP VALUE"
        (note in particular that the whitespace surrounding OP is required),
        where

            COLUMN_NAME is a searchable CDA column (see the columns() function
            for details)

            OP is one of: < <=  > >= = !=

            VALUE is a particular value of whatever data type is stored
            in COLUMN_NAME (see the columns() function for details), or
            the special keyword NULL, indicating the filter should match
            missing (null) values in COLUMN_NAME.

        Operators = and != will work on numeric, boolean and string VALUEs.

        Operators < <= > >= will only work on numeric VALUEs.

        Users can require partial matches to string VALUEs by adding * to either or
        both ends. For example:

            primary_disease_type = *duct*
            sex = F*

        String VALUEs need not be quoted inside of filter strings. For example, to include
        the filters specified just above in the `match_all` argument, we can write:

            summarize( table='subject', match_all=[ 'primary_disease_type = *duct*', 'sex = F*' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get a count summary for rows where the `sex` field is missing data,
        we can write:

            summarize( table='subject', match_all=[ 'sex = NULL' ] )

    Returns:

        list of pandas DataFrames, with one DataFrame for each summarized column,
        enumerating counts (or statistically summarizing unbounded numeric values) over all
        of that column's data values appearing in any rows that match the
        user-specified filter criteria (the 'result rows'). Two of four possible special
        DataFrames in this list ('number_of_matching_files', 'number_of_matching_subjects',
        'number_of_subjects_related_to_matching_files', 'number_of_files_related_to_matching_subjects')
        will contain integers representing the total number of result rows and the
        total number of result-related rows in another table, as appropriate. Summaries for
        table='subject' will include a count of all related files; summaries for table='file'
        will include a count of all related subjects. Every other DataFrame in the list
        will be titled with a CDA column name and will contain value counts or statistical
        summaries for that column as filtered by the result row set.

        OR Python dictionary enumerating counts of all data values for each summarized column
        (or a statistical summary of those data values, in the case of unbounded numeric data)
        across all rows that match the user-specified filter criteria (the 'result rows').
        Two of four possible special summary keys in this dictionary ('number_of_matching_files',
        'number_of_matching_subjects', 'number_of_subjects_related_to_matching_files',
        'number_of_files_related_to_matching_subjects') will point to integers representing
        the total number of result rows and the total number of result-related rows in another
        table, as appropriate. Summaries for table='subject' will include a count of all related
        files; summaries for table='file' will include a count of all related subjects. Every
        other key in the dictionary will contain a CDA column name; every dictionary value will
        itself be a dictionary either enumerating observed counts of all values appearing in
        that column as filtered by the result row set, or encoding a statistical summary of
        those values in the case of unbounded numeric data.

        OR JSON-formatted text representing the same structure as the `return_data_as='dict'`
        option, written to `output_file`.

        OR returns nothing, but displays a series of tables to standard output
        describing the same data returned by the other `return_data_as` options.

        And yes, we know how those first two paragraphs look. We apologize to the entire English language.
    """

    col_values = columns()

    log = get_logger()

    # Top-level type and sanity checking (i.e. not examining list contents yet): ensure nothing untoward got passed into our parameters.

    if col_values is None:
        log.error( "FATAL: Something went wrong calling columns()." )
        return

    #############################################################################################################################
    # Ensure our one required argument exists and is a valid table name.

    if not isinstance(table, str) or table == "":
        log.error( f"Parameter 'table' is required and must be a nonempty string; you supplied '{table}', which is not." )
        return

    valid_tables = tables()

    if table not in valid_tables:
        log.error( f"Parameter 'table' must be a searchable CDA table; you supplied '{table}', which is not." )
        return

    #############################################################################################################################
    # Process return-type directives `output_file` and `return_data_as`.

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance(output_file, str):
        log.error( f"The `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path." )
        return

    output_file = output_file.strip()

    if not isinstance(return_data_as, str):
        log.error( f"Unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe_list', 'dict' or 'json' (or omit the 'return_data_as' parameter altogether)." )
        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame_LIsT' or return_data_as='JSON'

    return_data_as = return_data_as.lower()

    allowed_return_types = {"", "dataframe_list", "dict", "json" }

    if return_data_as not in allowed_return_types:
        # Complain if we receive an unexpected `return_data_as` value.
        log.error( f"Unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe_list', 'dict' or 'json' (or omit the 'return_data_as' parameter altogether)." )
        return

    elif return_data_as == "json" and output_file == "":
        # If the user asks for JSON, they also have to give us a path for the output file. If they didn't, complain.
        log.error( "Return type 'json' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/json/to'." )
        return

    elif return_data_as != "json" and output_file != "":
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='json'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.
        log.error( f"'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'json'. You requested return_data_as='{return_data_as}'." )
        log.error( "(Note that if you don't specify any value for 'return_data_as', it defaults to printing tables to the standard output stream and not to an output file.)." )
        return

    #############################################################################################################################
    # Define the list of supported filter-string operators.

    allowed_operators = {">", ">=", "<", "<=", "=", "!="}

    #############################################################################################################################
    # Enumerate restrictions on operator use to appropriate data types.

    operators_by_data_type = {
        "bigint": allowed_operators,
        "boolean": {"=", "!="},
        "integer": allowed_operators,
        "numeric": allowed_operators,
        "text": {"=", "!="},
    }

    #############################################################################################################################
    # Enable aliases for various ways to say "True" and "False". (Case will be lowered as soon as each literal is received.)

    boolean_alias = {"true": "true", "t": "true", "false": "false", "f": "false"}

    #############################################################################################################################
    # Manage basic validation for the `match_all` parameter, which enumerates user-specified requirements that returned
    # records must all be simultaneously satisfied (AND; intersection; 'all of these must apply').

    if isinstance(match_all, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        match_all = [match_all]

    if not isinstance(match_all, list):
        log.critical(f"summarize(): ERROR: value assigned to 'match_all' parameter must be a filter string or a list of filter strings; you specified '{match_all}', which is neither.")
        return

    for item in match_all:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(f"summarize(): ERROR: value assigned to 'match_all' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_all}', which is neither.")
            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(f"summarize(): ERROR: match_all: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.")
            return

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned records must satisfy at least one (OR; union; 'at least one of these must apply').

    if isinstance(match_any, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.
        match_any = [match_any]

    if not isinstance(match_any, list):
        log.critical(f"summarize(): ERROR: value assigned to 'match_any' parameter must be a filter string or a list of filter strings; you specified '{match_any}', which is neither.")

        return

    for item in match_any:
        if not isinstance(item, str) or len(item) == 0:
            log.critical( f"summarize(): ERROR: value assigned to 'match_any' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_any}', which is neither.")

            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(f"summarize(): ERROR: match_any: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.")
            return

    #############################################################################################################################
    # Manage basic validation for the `match_from_file` parameter, which refers to a target CDA column and a list of allowed
    # values, and constrains summarize() to describe only data from rows that contain an allowed value in the target
    # CDA column. Also load column data here from the given TSV, so we can fail early if something goes wrong with the I/O.

    # Top-level type and sanity checking (i.e. not examining dictionary values yet): ensure nothing untoward got passed into `match_from_file`.

    if not isinstance(match_from_file, dict):
        log.critical(f"summarize(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not.")
        return

    else:
        received_keys = set(match_from_file.keys())

        expected_keys = {"input_file", "input_column", "cda_column_to_match"}

        if received_keys != expected_keys:
            log.critical(f"summarize(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not.")
            return

    # Cache metadata about this parameter, if it's been used (i.e. if values aren't blank).

    match_from_file_target_column = match_from_file["cda_column_to_match"]

    match_from_file_input_file = match_from_file["input_file"]

    match_from_file_source_column_name = match_from_file["input_column"]

    match_from_file_target_values = set()

    # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
    # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
    # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
    # anger and confusion when results don't match the input set along the given column.

    match_from_file_nulls_allowed = False

    # Make sure the dictionary values are either all zero-length strings or all nonzero-length strings.

    # I know the following isn't efficiently written -- sorry. Was in an unusually urgent hurry and the logic reflects first-round thoughts only.

    if match_from_file_target_column == "":
        if match_from_file["input_file"] != "" or match_from_file["input_column"] != "":
            log.critical(f"summarize(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.")
            return

    elif match_from_file["input_file"] == "":
        if match_from_file_target_column != "" or match_from_file["input_column"] != "":
            log.critical(f"summarize(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.")
            return

    elif match_from_file["input_column"] == "":
        if match_from_file_target_column != "" or match_from_file["input_file"] != "":
            log.critical(f"summarize(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.")
            return

    else:
        # See if columns() agrees that the requested column exists.

        if len(columns(column=match_from_file_target_column, return_data_as="list")) == 0:
            log.critical(f"summarize(): ERROR: CDA column '{match_from_file_target_column}' (specified in your 'match_from_file' parameter) does not exist. Please see the output of columns() for a list of those that do.")
            return

        if match_from_file_input_file == output_file:
            log.critical(f"summarize(): ERROR: You specified the same file ('{output_file}') as both a source of filter values (via 'match_from_file') and the target output file ( via 'output_file'). Please make sure these two files are different.")
            return

        try:
            with open(match_from_file_input_file) as IN:
                column_names = next(IN).rstrip("\n").split("\t")

                if match_from_file_source_column_name not in column_names:
                    log.critical(f"summarize(): ERROR: TSV column '{match_from_file_source_column_name}' (specified in your 'match_from_file' parameter) does not exist. Columns in your specified input file ('{match_from_file_input_file}') are:\n\n    {column_names}\n")
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
            log.critical(f"summarize(): ERROR: Couldn't load requested column '{match_from_file_source_column_name}' from requested TSV file '{match_from_file_input_file}': got error of type '{type(error)}', with error message '{error}'.")
            return

    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which enumerates user-specified filters on upstream data
    # sources.

    if isinstance(data_source, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        data_source = [data_source]

    elif not isinstance(data_source, list):
        log.critical(f"summarize(): ERROR: value assigned to the 'data_source' parameter must be a string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.")
        return

    for item in data_source:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(f"summarize(): ERROR: value assigned to the 'data_source' parameter must be a nonempty string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.")
            return
        
    # `add_columns`

    if isinstance(add_columns, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        add_columns = [add_columns]

    if not isinstance(add_columns, list):
        log.critical(
            f"summarize(): ERROR: value assigned to 'add_columns' parameter must be a string (e.g. 'primary_diagnosis_site') or a list of strings (e.g. [ 'specimen_type', 'primary_diagnosis_condition' ]); you specified '{add_columns}', which is neither."
        )

        return

    # `exclude_columns`

    if isinstance(exclude_columns, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        exclude_columns = [exclude_columns]

    if not isinstance(exclude_columns, list):
        log.critical(
            f"summarize(): ERROR: value assigned to 'exclude_columns' parameter must be a string (e.g. 'primary_diagnosis_site') or a list of strings (e.g. [ 'specimen_type', 'primary_diagnosis_condition' ]); you specified '{exclude_columns}', which is neither."
        )

        return

    # Let us not care about case, and remove any whitespace before it can do any damage.

    data_source = [re.sub(r"\s+", r"", item).lower() for item in data_source]

    # TEMPORARY: enumerate valid values and warn the user if they supplied something else.
    # At time of writing this is too expensive to retrieve dynamically from the API,
    # so the valid value list is hard-coded here and in the docstring for this function.
    #
    # This should be replaced ASAP with a fetch from a 'release metadata' table or something
    # similar.

    allowed_data_source_values = {"gdc", "pdc", "idc", "cds", "icdc"}

    for item in data_source:
        if item not in allowed_data_source_values:
            log.critical(f"summarize(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{item}', which is not.")
            return

    result_column_data_types = dict()

    # Store the default column ordering as provided by the columns() function,
    # to enable us to always display the same data in the same way.

    source_table_columns_in_order = list()

    table_cols = col_values.query(f'table == "{table}"')

    if table_cols is None:
        # Since we've checked for the existence of table previously, this case should never happen.
        log.critical(f"No such table {table}. Please retry with an existent table.")

        return

    for row_index, column_record in table_cols.iterrows():
        result_column_data_types[column_record["column"]] = column_record["data_type"]

        source_table_columns_in_order.append(column_record["column"])

    #############################################################################################################################
    # Parse `match_all` filter expressions: complain if
    #
    #     * requested columns don't exist
    #     * illegal or type-inappropriate operators are used
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere but at the ends of a filter string
    #
    # ...and save parse results for each filter expression as a separate Query object (to be combined later).

    try:
        queries_for_match_all = validate_and_transform_match_filter_list( col_values, match_all )
    except Exception as e:
        log.critical(e)
        return

    #############################################################################################################################
    # Parse `match_any` filter expressions: complain if
    #
    #     * requested columns don't exist
    #     * illegal or type-inappropriate operators are used
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere but at the ends of a filter string
    #
    # ...and save parse results for each filter expression as a separate Query object (to be combined later).

    try:
        queries_for_match_any = validate_and_transform_match_filter_list( col_values, match_any )
    except Exception as e:
        log.critical(e)
        return

    #############################################################################################################################
    # Parse `match_from_file` filter values: complain if
    #
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere
    #
    # ...and save parse results as a combined filter expression in a Query object (to be combined with others later).

    # Identify the data type of the target column.

    target_data_type = columns(column=match_from_file_target_column)["data_type"][0]

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
                log.critical(f"summarize(): ERROR: match_from_file: requested column {match_from_file_target_column} has data type 'boolean', requiring a true/false value; you specified '{target_value}', which is neither.")
                return

            else:
                target_value = boolean_alias[target_value]

        elif target_data_type in ["bigint", "integer", "numeric"]:
            # If we're supposed to be in a numeric column, make sure we've got a number.

            if re.search(r"^[-+]?\d+(\.\d+)?$", target_value) is None:
                log.critical(f"summarize(): ERROR: match_from_file: requested column {match_from_file_target_column} has data type '{target_data_type}', requiring a number value; you specified '{target_value}', which is not.")
                return

        elif target_data_type == "text":
            # Check for wildcards: if found, vomit.

            if re.search(r"\*", target_value) is not None:
                log.critical(f"summarize(): ERROR: match_from_file: wildcards (*) are disallowed here (only exact matches are supported for this option); string '{target_value}' is noncompliant. Please fix.")
                return

        else:
            # Just to be safe. Types change.

            log.critical(f"summarize(): ERROR: match_from_file: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.")
            return

        processed_target_values.add(target_value)

    # Build a Query object for the column data loaded according to `match_from_file`.

    query_for_match_from_file = None

    # if match_from_file_nulls_allowed == True:
    #     query_for_match_from_file = Query()

    #     query_for_match_from_file.node_type = "OR"

    #     match_from_file_null_match_subquery = Query()

    #     match_from_file_null_match_subquery.node_type = "IS"

    #     match_from_file_null_match_subquery.l = Query()

    #     match_from_file_null_match_subquery.l.node_type = "column"

    #     match_from_file_null_match_subquery.l.value = match_from_file_target_column

    #     match_from_file_null_match_subquery.r = Query()

    #     match_from_file_null_match_subquery.r.node_type = "unquoted"

    #     match_from_file_null_match_subquery.r.value = "NULL"

    #     query_for_match_from_file.l = match_from_file_null_match_subquery

    #     match_from_file_allowed_values_subquery = Query()

    #     match_from_file_allowed_values_subquery.node_type = "IN"

    #     match_from_file_allowed_values_subquery.l = Query()

    #     match_from_file_allowed_values_subquery.l.node_type = "column"

    #     match_from_file_allowed_values_subquery.l.value = match_from_file_target_column

    #     match_from_file_allowed_values_subquery.r = Query()

    #     match_from_file_allowed_values_subquery.r.node_type = "unquoted"

    #     if target_data_type == "text":
    #         match_from_file_allowed_values_subquery.r.value = (
    #             r'("' + r'","'.join(sorted(processed_target_values)) + r'")'
    #         )

    #     else:
    #         match_from_file_allowed_values_subquery.r.value = r"(" + r",".join(sorted(processed_target_values)) + r")"

    #     query_for_match_from_file.r = match_from_file_allowed_values_subquery

    # elif len(processed_target_values) > 0:
    #     query_for_match_from_file = Query()

    #     query_for_match_from_file.node_type = "IN"

    #     query_for_match_from_file.l = Query()

    #     query_for_match_from_file.l.node_type = "column"

    #     query_for_match_from_file.l.value = match_from_file_target_column

    #     query_for_match_from_file.r = Query()

    #     query_for_match_from_file.r.node_type = "unquoted"

    #     if target_data_type == "text":
    #         query_for_match_from_file.r.value = r'("' + r'","'.join(sorted(processed_target_values)) + r'")'

    #     else:
    #         query_for_match_from_file.r.value = r"(" + r",".join(sorted(processed_target_values)) + r")"

    #############################################################################################################################
    # Parse `data_source` filter expressions: complain if any are nonconformant, and (for now) save parse results for each
    # filter expression as a separate Query object.

    queries_for_data_source = []

    for ds in data_source:
        queries_for_match_all.append(f"{table}_data_at_{ds} = True")


    #############################################################################################################################
    # Parse `add_columns` list: use the API's SELECT and SELECTVALUES operators
    # to build a Query object encoding the given column selections.

    columns_to_fetch = list()

    # Always begin by including all `table` fields, to which any extra
    # columns requested via `add_columns` will be added in each result row.
    #
    # If anyone wants, we can upgrade later to let users select specific
    # `table` columns to withhold from returned results.

    columns_to_fetch = source_table_columns_in_order.copy()

    # Tracking variable: will our results just include the default
    # column set from `table`?

    use_only_default_columns = True

    for column_to_add in add_columns:
        # Ignore requests for columns that are already present by default.

        if column_to_add not in columns_to_fetch:
            use_only_default_columns = False

            columns_to_fetch.append(column_to_add)

    columns_to_remove = []

    for col in exclude_columns:
        # Ignore requests to exclude columns that are already excluded.

        if col in columns_to_fetch:
            columns_to_fetch.remove(col)

            columns_to_remove.append(col)

        else:
            log.debug( f'Ignoring request to remove column "{col}" because it doesn\'t exist or is already excluded.' )

    # for item in data_source:
    #     # Build a Query object for this data source and add it to the data_source list.

    #     data_source_query = Query()

    #     data_source_query.node_type = "="

    #     data_source_query.l = Query()

    #     data_source_query.l.node_type = "column"

    #     data_source_query.l.value = f"{table}_from_{item}"

    #     data_source_query.r = Query()

    #     data_source_query.r.node_type = "unquoted"

    #     data_source_query.r.value = "true"

    #     queries_for_data_source.append(data_source_query)

    #############################################################################################################################
    # Combine all match_all filter queries into one big AND-linked query.

    # combined_match_all_query = Query()

    # if len(queries_for_match_all) == 0:
    #     # No filters in this group: nothing to do here.

    #     combined_match_all_query = None

    # elif len(queries_for_match_all) == 1:
    #     # No fancy combination needed. Just pass the API the one filter we've got.

    #     combined_match_all_query = queries_for_match_all[0]

    # else:
    #     # Hook up the first two queries in `queries_for_match_all` with an `AND`.

    #     combined_match_all_query.node_type = "AND"

    #     combined_match_all_query.l = queries_for_match_all[0]

    #     combined_match_all_query.r = queries_for_match_all[1]

    #     if len(queries_for_match_all) > 2:
    #         # Add any remaining queries in `queries_for_match_all`, one at a time.

    #         for i in range(2, len(queries_for_match_all)):
    #             bigger_query = Query()

    #             bigger_query.node_type = "AND"

    #             bigger_query.l = combined_match_all_query

    #             bigger_query.r = queries_for_match_all[i]

    #             combined_match_all_query = bigger_query

    # #############################################################################################################################
    # # Combine all `match_any` filter queries into one big OR-linked query.

    # combined_match_any_query = Query()

    # if len(queries_for_match_any) == 0:
    #     # No filters in this group: nothing to do here.

    #     combined_match_any_query = None

    # elif len(queries_for_match_any) == 1:
    #     # No fancy combination needed. Just pass the API the one filter we've got.

    #     combined_match_any_query = queries_for_match_any[0]

    # else:
    #     # Hook up the first two queries in `queries_for_match_any` with an `OR`.

    #     combined_match_any_query.node_type = "OR"

    #     combined_match_any_query.l = queries_for_match_any[0]

    #     combined_match_any_query.r = queries_for_match_any[1]

    #     if len(queries_for_match_any) > 2:
    #         # Add any remaining queries in `queries_for_match_any`, one at a time.

    #         for i in range(2, len(queries_for_match_any)):
    #             bigger_query = Query()

    #             bigger_query.node_type = "OR"

    #             bigger_query.l = combined_match_any_query

    #             bigger_query.r = queries_for_match_any[i]

    #             combined_match_any_query = bigger_query

    # #############################################################################################################################
    # # Combine all data_source queries into one big AND-linked query.

    # combined_data_source_query = Query()

    # if len(queries_for_data_source) == 0:
    #     # No filters in this group: nothing to do here.

    #     combined_data_source_query = None

    # elif len(queries_for_data_source) == 1:
    #     # No fancy combination needed. Just pass the API the one filter we've got.

    #     combined_data_source_query = queries_for_data_source[0]

    # else:
    #     # Hook up the first two queries in `queries_for_data_source` with an `AND`.

    #     combined_data_source_query.node_type = "AND"

    #     combined_data_source_query.l = queries_for_data_source[0]

    #     combined_data_source_query.r = queries_for_data_source[1]

    #     if len(queries_for_data_source) > 2:
    #         # Add any remaining queries in `queries_for_data_source`, one at a time.

    #         for i in range(2, len(queries_for_data_source)):
    #             bigger_query = Query()

    #             bigger_query.node_type = "AND"

    #             bigger_query.l = combined_data_source_query

    #             bigger_query.r = queries_for_data_source[i]

    #             combined_data_source_query = bigger_query

    #############################################################################################################################
    # Combine all the filters we've processed.

    # final_query = Query()

    # if (
    #     combined_match_all_query is None
    #     and combined_match_any_query is None
    #     and combined_data_source_query is None
    #     and query_for_match_from_file is None
    # ):
    #     # We got no filters. Retrieve everything: make the query we pass to the API
    #     # equivalent to 'id IS NOT NULL', which should match everything.

    #     final_query.node_type = "IS NOT"

    #     final_query.l = Query()

    #     final_query.l.node_type = "column"

    #     final_query.l.value = f"{table}_id"

    #     final_query.r = Query()

    #     final_query.r.node_type = "unquoted"

    #     final_query.r.value = "NULL"

    #     # `somatic_mutation` has no ID field.

    #     if table == "somatic_mutation":
    #         final_query.node_type = "OR"

    #         final_query.l = Query()
    #         final_query.l.node_type = "IS"

    #         final_query.l.l = Query()
    #         final_query.l.l.node_type = "column"
    #         final_query.l.l.value = "hotspot"

    #         final_query.l.r = Query()
    #         final_query.l.r.node_type = "unquoted"
    #         final_query.l.r.value = "NULL"

    #         final_query.r = Query()
    #         final_query.r.node_type = "IS NOT"

    #         final_query.r.l = Query()
    #         final_query.r.l.node_type = "column"
    #         final_query.r.l.value = "hotspot"

    #         final_query.r.r = Query()
    #         final_query.r.r.node_type = "unquoted"
    #         final_query.r.r.value = "NULL"

    # elif combined_data_source_query is None:
    #     if query_for_match_from_file is None:
    #         if combined_match_all_query is not None and combined_match_any_query is None:
    #             final_query = combined_match_all_query

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             final_query = combined_match_any_query

    #         else:
    #             # Join both non-null filter groups with an `AND`.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = combined_match_any_query

    #     else:
    #         if combined_match_all_query is None and combined_match_any_query is None:
    #             final_query = query_for_match_from_file

    #         elif combined_match_all_query is not None and combined_match_any_query is None:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = query_for_match_from_file

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_any_query

    #             final_query.r = query_for_match_from_file

    #         else:
    #             final_query.node_type = "AND"

    #             final_query.l = Query()

    #             final_query.l.node_type = "AND"

    #             final_query.l.l = combined_match_all_query

    #             final_query.l.r = combined_match_any_query

    #             final_query.r = query_for_match_from_file

    # else:
    #     if query_for_match_from_file is None:
    #         if combined_match_all_query is None and combined_match_any_query is None:
    #             final_query = combined_data_source_query

    #         elif combined_match_all_query is not None and combined_match_any_query is None:
    #             # Join both non-null filter groups with an `AND`.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_data_source_query

    #             final_query.r = combined_match_all_query

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             # Join both non-null filter groups with an `AND`.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_data_source_query

    #             final_query.r = combined_match_any_query

    #         else:
    #             # Join all three filter groups via 'AND'.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = combined_match_any_query

    #             actually_final_query = Query()

    #             actually_final_query.node_type = "AND"

    #             actually_final_query.l = final_query

    #             actually_final_query.r = combined_data_source_query

    #             final_query = actually_final_query

    #     else:
    #         if combined_match_all_query is None and combined_match_any_query is None:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_data_source_query

    #             final_query.r = query_for_match_from_file

    #         elif combined_match_all_query is not None and combined_match_any_query is None:
    #             final_query.node_type = "AND"

    #             final_query.l = Query()

    #             final_query.l.node_type = "AND"

    #             final_query.l.l = combined_data_source_query

    #             final_query.l.r = combined_match_all_query

    #             final_query.r = query_for_match_from_file

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             final_query.node_type = "AND"

    #             final_query.l = Query()

    #             final_query.l.node_type = "AND"

    #             final_query.l.l = combined_data_source_query

    #             final_query.l.r = combined_match_any_query

    #             final_query.r = query_for_match_from_file

    #         else:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = combined_match_any_query

    #             actually_final_query = Query()

    #             actually_final_query.node_type = "AND"

    #             actually_final_query.l = final_query

    #             actually_final_query.r = Query()

    #             actually_final_query.r.node_type = "AND"

    #             actually_final_query.r.l = combined_data_source_query

    #             actually_final_query.r.r = query_for_match_from_file

    #             final_query = actually_final_query

    query_object = QNode()
    query_object.match_all = queries_for_match_all
    query_object.match_some = queries_for_match_any
    query_object.add_columns = columns_to_fetch
    query_object.exclude_columns = columns_to_remove

    #############################################################################################################################
    # Fetch data from the API.

    # Dump JSON describing the full combined query structure.
    log.debug( f"Querying CDA API '/summary/{table}' endpoint:\n{json.dumps( query_object.to_dict(), indent=4 )}\n" )

    # Support selection of the appropriate endpoint based on the value of `table`.

    query_selector = {
        'file': summary_file_endpoint,
        'subject': summary_subject_endpoint,
    }

    # Make an API client object to manage query transmission.

    query_api_instance = cda_client.Client( base_url=get_api_url() )

    # Send the query to the relevant endpoint and save response data.

    paged_response_data_object = query_selector[table].sync( client=query_api_instance, body=query_object )

    # Gracefully fetch asynchronously-generated results once they're ready.

    if isinstance(paged_response_data_object, ApplyResult):
        while paged_response_data_object.ready() is False:
            paged_response_data_object.wait(5)

        try:
            paged_response_data_object = paged_response_data_object.get()

        except UnexpectedStatus as e:
            try:
                # Ordinarily, this exception represents a structured complaint
                # from the API service that something went wrong. In this case,
                # the `body` property of the ApiException object will contain
                # a JSON-encoded message generated by the API describing the
                # unfortunate circumstance.

                error_message = json.loads(e.body)["message"]

            except:
                # Unfortunately, if something goes wrong at the level of the
                # HTTP service on which the API relies -- that is, when we
                # can't actually communicate with the API as such because
                # something's gone wrong with our ability to talk to the web
                # server -- the ApiException class is overloaded to encode
                # that HTTP protocol error (and not throw any further exceptions),
                # instead of handling such events somewhere more appropriate
                # (like via a different exception class altogether).

                error_message = str(e)

            log.critical(f"summarize(): ERROR: error message from API: '{error_message}'")

            return

        except BaseException as e:
            if re.search("urllib3.exceptions.MaxRetryError", str(type(e))) is not None:
                log.critical("summarize(): ERROR: Can't connect to the CDA API service.")

            else:
                log.critical(f"summarize(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call after calling set_log_level( 'DEBUG' ) will give more information).")
            return

    # Report some metadata about the results we got back.

    log.debug( f"/summary/{table} endpoint query SQL:\n{paged_response_data_object.to_dict()['query_sql']}" )

    # This is immensely verbose, sometimes.

    log.debug( f"/summary/{table} endpoint response:\n{json.dumps( paged_response_data_object.to_dict()['result'], indent=4 )}\n" )

    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function.

    ### REMOVE AFTER DEBUG: return paged_response_data_object.to_dict()["result"]

    result_dataframe = pd.json_normalize( paged_response_data_object.to_dict()["result"] )

    #############################################################################################################################
    # Postprocess API result data.

    log.debug( "Organizing result data..." )

    #############################################################################################################################
    # Postprocess API result data.

    # For some reason, the highest-level summary counts come through as floats. Fix that
    # (and rename them while we're at it).

    toplevel_columns_to_fix = {
        
        'total_count': 'number_of_matching_files' if table == 'file' else 'number_of_matching_subjects' if table == 'subject' else 'number_of_matching_rows',
        'file_count': 'number_of_files_related_to_matching_subjects',
        'subject_count': 'number_of_subjects_related_to_matching_files'
    }

    for result_column in toplevel_columns_to_fix:
        
        if result_column in result_dataframe:
            
            result_dataframe[result_column] = result_dataframe[result_column].round().astype(int)

            result_dataframe = result_dataframe.rename( columns={ result_column: toplevel_columns_to_fix[result_column] } )

    # Remove '_summary' from ordinary result column names before returning.

    skip_rename = {
        'data_source',
        'file_data_source_count_summary',
        'subject_data_source_count_summary',
        'number_of_matching_files',
        'number_of_matching_subjects',
        'number_of_matching_rows',
        'number_of_files_related_to_matching_subjects',
        'number_of_subjects_related_to_matching_files'
    }

    result_column_names = result_dataframe.columns.values

    for result_column in result_column_names:
        
        if result_column not in skip_rename:
            
            new_column_name = re.sub( r'_summary$', r'', result_column )

            if new_column_name != result_column:
                
                result_dataframe = result_dataframe.rename( columns={ result_column: new_column_name } )

    if return_data_as == "" or return_data_as == "dataframe_list":
        
        # Right now, the default is to print one table to standard output
        # for each DataFrame that would be returned had they requested
        # `return_data_as='dataframe_list'`.

        result_list = list()

        for toplevel_column in [ 'number_of_matching_files', 'number_of_matching_subjects', 'number_of_matching_rows', 'number_of_files_related_to_matching_subjects', 'number_of_subjects_related_to_matching_files' ]:
            
            if toplevel_column in result_dataframe:
                
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                result_list.append( pd.DataFrame( result_dataframe[toplevel_column], columns=[toplevel_column] ) )

        # Put the numeric summaries at the end of the displayed block of results.

        result_list_tail = list()

        for result_column in result_dataframe.columns:
            
            if result_column not in skip_rename and result_dataframe[result_column].dtype == 'object' and isinstance( result_dataframe[result_column][0], list ) and isinstance( result_dataframe[result_column][0][0], dict ) and 'median' in result_dataframe[result_column][0][0]:
                
                # These are one-element arrays, with the element being a key/value dictionary containing summary stats.

                result_column_dict = dict()

                result_column_dict['cda_column_name'] = [result_column]

                # Hard-coding this is fragile, but safe for now and there's a lot to do.

                for key in [ 'mean', 'min', 'lower_quartile', 'median', 'upper_quartile', 'max' ]:
                    
                    result_column_dict[key] = [result_dataframe[result_column][0][0][key]]

                result_list_tail.append( pd.DataFrame.from_dict( result_column_dict ).reset_index( drop=True ) )

            elif result_column not in skip_rename:
                
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                if result_dataframe[result_column].dtype == "int64":
                    
                    result_dataframe[result_column] = int( result_dataframe[result_column][0] )

                elif result_dataframe[result_column].dtype == "object":
                    
                    result_column_dict = {
                        result_column: list(),
                        "count_result": list()
                    }

                    if result_dataframe[result_column][0] is not None:
                        
                        # This cell should contain an array of Python dicts, with each dict containing two entries:
                        #
                        #    data column label and value:
                        #       keyword: `result_column`, e.g. 'cause_of_death'
                        #       value: one allowable value for `result_column`, e.g. 'Cancer-Related Death'
                        # 
                        #    observed count of the given value:
                        #       keyword: 'count_result'
                        #       value: (int) number of times the given data value (described in the previous dictionary entry) was observed in this set of result data

                        for dict_pair in result_dataframe[result_column][0]:
                            
                            print_value = "<NA>"

                            actual_value = dict_pair[result_column]

                            if actual_value is not None and actual_value != "":
                                
                                print_value = actual_value

                            result_column_dict[result_column].append( print_value )

                            result_column_dict["count_result"].append( dict_pair["count_result"] )

                    result_list.append( pd.DataFrame.from_dict( result_column_dict ).sort_values( by=[ "count_result", result_column ], ascending=[ False, True ] ).reset_index( drop=True ) )

                else:
                    
                    log.error( f"Unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event." )

                    return

        result_list = result_list + result_list_tail

        if return_data_as == "":
            
            log.debug( "Returning results in default form (printing list of tables to standard output)" )

            with pd.option_context( "display.max_rows", None, "display.max_columns", None, "display.max_colwidth", 65 ):
                
                for result_list_df in result_list:
                    
                    print_df = result_list_df

                    max_col_width = 80

                    maxcolwidths_list = [ None ]

                    colalign_list = [ "left" ]

                    if print_df is not None and len( print_df ) > 0:
                        
                        if len( print_df.columns ) == 1:
                            
                            colalign_list = [ "left" ]

                        elif 'count_result' in print_df.columns.values:
                            
                            maxcolwidths_list = [ None, max_col_width ]

                            colalign_list = [ "right", "right" ]

                            # Truncate displayed text values manually and add ellipses. The `tabulate` library doesn't do this on its own (as Pandas does).

                            print_df[print_df.columns[0]] = print_df[print_df.columns[0]].apply( lambda x: re.sub( f"^(.{{{max_col_width-3}}}).*", r"\1...", x ) if ( x is not None and len( x ) > max_col_width ) else x )

                            # Put the count values first in the display.

                            new_column_ordering = list( reversed( print_df.columns.tolist() ) )

                            print_df = print_df[new_column_ordering]

                        elif 'median' in print_df.columns.values:
                            
                            colalign_list = [ "right" ]

                            result_name = print_df['cda_column_name'][0]

                            result_dict = {
                                
                                '': list(),
                                result_name: list()
                            }

                            # Hard-coding this is fragile, but safe for now and there's a lot to do.

                            for key in [ 'mean', 'min', 'lower_quartile', 'median', 'upper_quartile', 'max' ]:
                                
                                result_dict[''].append( f"{re.sub( r'_', r' ', key )}" )

                                print( len( print_df ) )
                                print ( print_df.columns.values )
                                print ( key )

                                result_dict[result_name].append( f"{print_df[key][0]:>15}" )

                            print_df = pd.DataFrame.from_dict( result_dict ).reset_index( drop=True )

                        # Suppress output of confusing row-index column when displaying DataFrame contents and get some control over cell alignment.

                        print(
                            tabulate.tabulate(
                                print_df,
                                showindex=False,
                                headers=print_df.columns,
                                tablefmt="double_outline",
                                colalign=colalign_list,
                                maxcolwidths=maxcolwidths_list,
                                disable_numparse=True,
                            )
                        )

            return

        elif return_data_as == "dataframe_list":
            
            log.debug( "Returning results as a list of pandas.DataFrame objects" )

            return result_list

    elif return_data_as == "dict" or return_data_as == "json":
        
        # Build a Python dictionary to shape returned results.

        result_dict = dict()

        for result_column in result_dataframe.columns:
            
            if ( result_column not in skip_rename or re.search( r'_data_source_count_summary$', result_column ) is not None ) and result_dataframe[result_column].dtype == 'object' and isinstance( result_dataframe[result_column][0], list ) and isinstance( result_dataframe[result_column][0][0], dict ) and 'median' in result_dataframe[result_column][0][0]:
                
                # These are one-element arrays, with the element being a key/value dictionary containing summary stats.

                result_dict[result_column] = dict()

                # Hard-coding this is fragile, but safe for now and there's a lot to do.

                for key in [ 'mean', 'min', 'lower_quartile', 'median', 'upper_quartile', 'max' ]:
                    
                    result_dict[result_column][key] = result_dataframe[result_column][0][0][key]

            else:
                
                if result_dataframe[result_column].dtype == "int64":
                    
                    result_dict[result_column] = int( result_dataframe[result_column][0] )

                elif result_dataframe[result_column].dtype == "object":
                    
                    result_dict[result_column] = None

                    if result_dataframe[result_column][0] is not None:
                        
                        # This cell should contain an array of Python dicts, with each dict containing two entries:
                        #
                        #    data column label and value:
                        #       keyword: `result_column`, e.g. 'cause_of_death'
                        #       value: one allowable value for `result_column`, e.g. 'Cancer-Related Death'
                        # 
                        #    observed count of the given value:
                        #       keyword: 'count_result'
                        #       value: (int) number of times the given data value (described in the previous dictionary entry) was observed in this set of result data

                        result_dict[result_column] = dict()

                        for dict_pair in result_dataframe[result_column][0]:
                            
                            result_dict[result_column][dict_pair[result_column]] = dict_pair["count_result"]

                else:
                    
                    log.error( f"Unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event." )
                    return

        if return_data_as == "dict":
            
            log.debug( "Returning results as a Python dictionary" )

            return result_dict

        elif return_data_as == "json":
            
            # Write the results to a user-specified JSON file.

            log.debug( f"Printing results to JSON file '{output_file}'" )

            try:
                
                with open( output_file, "w" ) as OUT:
                    
                    json.dump( result_dict, OUT, indent=4, ensure_ascii=True )

                return

            except Exception as error:
                
                log.error( f"Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'." )

                return

    log.error( "Something has gone unexpectedly and disastrously wrong with return-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error." )

    return


#############################################################################################################################
#
# END summarize()
#
#############################################################################################################################


