import json
import os
import pandas as pd
import re
import tabulate

import cda_client

from multiprocessing.pool import ApplyResult

from cdapython.application_utilities import get_api_url
from cdapython.discover import columns, release_metadata, tables
from cdapython.logging_wrappers import get_logger
from cdapython.validation import normalize_to_list, validate_and_transform_match_filter_list, validate_and_transform_match_from_file_values, validate_parameter_values

from cda_client.api.summary import file_summary_endpoint_summary_file_post as summary_file_endpoint
from cda_client.api.summary import subject_summary_endpoint_summary_subject_post as summary_subject_endpoint
from cda_client.errors import UnexpectedStatus
from cda_client.models.client_error import ClientError
from cda_client.models.internal_error import InternalError
from cda_client.models.summary_request_body import SummaryRequestBody

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
# expand_file_results(): Expand the information from one embedded DataFrame column in a result DataFrame returned by
#                            get_file_data() into a 2D table with one row per embdedded DataFrame row, with a `file_id` column
#                            prepended to each to identify the parent file record.
#
#############################################################################################################################

def expand_file_results(
    results_dataframe,
    column_to_expand
):
    """
    Given a result DataFrame R returned by get_file_data(), and a column C in R
    that contains DataFrames, return a version of the information in C
    expanded into a 2-dimensional table T, with one row in T for every row in every
    DataFrame in C, and with each row in T also containing the file_id in R that goes with
    that row.
    """

    log = get_logger()
    id_column_name = 'file_id'

    if not isinstance( results_dataframe, pd.DataFrame ):
        log.error( f"The 'results_dataframe' parameter must be a pandas DataFrame; you passed in an object of type { type( results_dataframe ) }, which is not that." )
        return
    elif id_column_name not in results_dataframe.columns:
        if 'subject_id' in results_dataframe.columns:
            log.error( f"The results dataframe you passed in does not have the expected '{id_column_name}' column: did you mean to use expand_subject_results() instead?" )
        else:
            log.error( f"The results dataframe you passed in does not have the expected '{id_column_name}' column: cannot collate sub-DataFrame results by file without it." )
        return
    elif not isinstance( column_to_expand, str ):
        log.error( f"The 'column_to_expand' parameter must be a string naming a column that exists in results_dataframe and contains (sub-)DataFrames as values. You passed in a value for 'column_to_expand' which wasn't a string (type {type(column_to_expand)})." )
        return
    elif column_to_expand not in results_dataframe.columns:
        log.error( f"The 'column_to_expand' parameter must be a string naming a column that exists in results_dataframe and contains (sub-)DataFrames as values. You passed in column_to_expand='{column_to_expand}', which isn't the name of a column in the result DataFrame you specified." )
        return

    return expand_results( results_dataframe=results_dataframe, column_to_expand=column_to_expand, table='file' )

#############################################################################################################################
#
# expand_subject_results(): Expand the information from one embedded DataFrame column in a result DataFrame returned by
#                            get_subject_data() into a 2D table with one row per embdedded DataFrame row, with a `subject_id` column
#                            prepended to each to identify the parent subject record.
#
#############################################################################################################################

def expand_subject_results(
    results_dataframe,
    column_to_expand
):
    """
    Given a result DataFrame R returned by get_subject_data(), and a column C in R
    that contains DataFrames, return a version of the information in C
    expanded into a 2-dimensional table T, with one row in T for every row in every
    DataFrame in C, and with each row in T also containing the subject_id in R that goes with
    that row.
    """

    log = get_logger()
    id_column_name = 'subject_id'

    if not isinstance( results_dataframe, pd.DataFrame ):
        log.error( f"The 'results_dataframe' parameter must be a pandas DataFrame; you passed in an object of type { type( results_dataframe ) }, which is not that." )
        return
    elif id_column_name not in results_dataframe.columns:
        if 'file_id' in results_dataframe.columns:
            log.error( f"The results dataframe you passed in does not have the expected '{id_column_name}' column: did you mean to use expand_file_results() instead?" )
        else:
            log.error( f"The results dataframe you passed in does not have the expected '{id_column_name}' column: cannot collate sub-DataFrame results by subject without it." )
        return
    elif not isinstance( column_to_expand, str ):
        log.error( f"The 'column_to_expand' parameter must be a string naming a column that exists in results_dataframe and contains (sub-)DataFrames as values. You passed in a value for 'column_to_expand' which wasn't a string (type {type(column_to_expand)})." )
        return
    elif column_to_expand not in results_dataframe.columns:
        log.error( f"The 'column_to_expand' parameter must be a string naming a column that exists in results_dataframe and contains (sub-)DataFrames as values. You passed in column_to_expand='{column_to_expand}', which isn't the name of a column in the result DataFrame you specified." )
        return

    return expand_results( results_dataframe=results_dataframe, column_to_expand=column_to_expand, table='subject' )

#############################################################################################################################
#
# expand_results(): Expand the information from one embedded DataFrame column in a result DataFrame returned by
#                            get_X_data() into a 2D table with one row per embdedded DataFrame row, with an `X_id` column
#                            prepended to each to identify the parent result record.
#
#############################################################################################################################

def expand_results(
    results_dataframe,
    column_to_expand,
    table
):
    """
    Given a result DataFrame R returned by get_data(), and a column C in R
    that contains DataFrames, return a version of the information in C
    expanded into a 2-dimensional table T, with one row in T for every row in every
    DataFrame in C, and with each row in T also containing the ID in R that goes with
    that row.
    """

    log = get_logger()
    id_column_name = f"{table}_id"

    expanded_column_data = []

    for _, result in results_dataframe.iterrows():
        if not isinstance( result[column_to_expand], pd.DataFrame ):
            log.error( f"The 'column_to_expand' parameter must be a string naming a column that exists in results_dataframe and contains (sub-)DataFrames as values. You passed in column_to_expand='{column_to_expand}', which isn't a column that contains DataFrames as values." )
            return
        else:
            for _, dataframe_row in result[column_to_expand].iterrows():
                row_data = {}
                row_data[id_column_name] = result[id_column_name]
                row_data.update( dataframe_row.to_dict() )
                expanded_column_data.append( row_data )

    return pd.DataFrame( expanded_column_data )

#############################################################################################################################
#
# intersect_file_results(): Combine DataFrames produced by get_file_data() into one DataFrame describing all the file rows
#                            that are present in all input DataFrames.
#
#############################################################################################################################

def intersect_file_results(
    *result_dfs_to_merge,
    ignore_added_columns=False
):
    """
    Combine two or more DataFrames produced by get_file_data() via intersection: merge result data for
    all files present in all input DataFrames.

    Arguments:
        two or more DataFrames returned by get_file_data()

        ignore_added_columns ( boolean; optional ):
            Merge only columns from the file table: avoids breakages in
            cases where added extra (non-file) columns can't be merged due
            to differences in how similar but different upstream queries produced
            the results we're trying to merge.
            (Default: False: try to merge file data plus all extra data appearing in
            all input DataFrames.)

    Returns:
        A pandas.DataFrame containing combined metadata about all file rows that
        appear in all input DataFrames, including by default all associated non-file data
        present in all input DataFrames.
    """

    return intersect_results( *result_dfs_to_merge, ignore_added_columns=ignore_added_columns, table='file' )

#############################################################################################################################
#
# intersect_subject_results(): Combine DataFrames produced by get_subject_data() into one DataFrame describing all the subject rows
#                               that are present in all input DataFrames.
#
#############################################################################################################################

def intersect_subject_results(
    *result_dfs_to_merge,
    ignore_added_columns=False
):
    """
    Combine two or more DataFrames produced by get_subject_data() via intersection: merge result data for
    all subjects present in all input DataFrames.

    Arguments:
        two or more DataFrames returned by get_subject_data()

        ignore_added_columns ( boolean; optional ):
            Merge only columns from the subject table: avoids breakages in
            cases where added extra (non-subject) columns can't be merged due
            to differences in how similar but different upstream queries produced
            the results we're trying to merge.
            (Default: False: try to merge subject data plus all extra data appearing in
            all input DataFrames.)

    Returns:
        A pandas.DataFrame containing combined metadata about all subject rows that
        appear in all input DataFrames, including by default all associated non-subject data
        present in all input DataFrames.
    """

    return intersect_results( *result_dfs_to_merge, ignore_added_columns=ignore_added_columns, table='subject' )

#############################################################################################################################
# 
# intersect_results(): Compute the intersection of two or more result DataFrames returned by get_data() from the same endpoint.
# 
# when merging each (top-level) column for each result row:
#
#     merging str, int, bool values :
#         * preserve first-seen values
#           --> complain (should never happen) if clashes are ever observed
#           home-table columns need not be present in all of the DataFrames to be merged since
#           all target values are guaranteed inferrable from at least one of the
#           other DataFrames to merge because this function computes an _intersection_
#           of home-table records: every value in any home-table column included in any of the
#           constitutent DataFrames is guaranteed to be available somewhere
#           /for all target records/ because all target records are present in all
#           input DataFrames by definition, including in particular the DataFrame
#           containing values for that column
#
#     merging list values :
#         * some (foreign column value summary) list contents can be filtered by upstream
#           queries and so may no longer be representative of the merged data if just
#           copied into merged results via union heedless of context
#           --> error if any list describing one foreign file, subject or project column
#               that is present in one result set is missing from any of the other result sets
#           potential gaps cannot be filled in the above case
#         * if no error, then iteratively and pairwise: make lists into sets, take union, re-listify
#
#     merging (sub-)DataFrames :
#         * some sub-DataFrame contents (project_data, subject_data, file_data, external_reference_data)
#           can be row-filtered by upstream queries and so may no longer be representative of the merged
#           data if just copied into merged results via union heedless of context
#           --> error if any sub-DataFrame in { file_data, project_data, subject_data, external_reference_data }
#               that is present in one result set is missing from any of the other result sets
#           --> error if the list of columns for ANY NONEMPTY sub-DataFrame in one result set
#               does not match the list of columns for NONEMPTY DataFrame values in any
#               same-named sibling column in any other result set
#           potential gaps cannot be filled in either of the above cases
#         * error for file_data, subject_data, project_data sub-DataFrames if file_id,
#           subject_id, project_id, resp., not present in NONEMPTY DataFrame cell values:
#           cannot unambiguously reconstitute results given the potential presence of row
#           filtering and/or possibly identical sets of file/subject/project records
#           distinguishable from one another only by ID
#         * if no error, then deduplicate row records across all input DataFrames and return unioned result
# 
# ----------------------------------------------------------------------------------------
# 
# non-list, non-DataFrame result columns can have mixed types -- e.g. ints (or any non-null
# values that aren't lists or DataFrames) mixed with str ('<NA>' null codes for display)
#
# make it an error to filter twice on the same column via match_all
#
# any number (>= 2) of dataframes can be combined in one call
#
# add ignore_added_columns flag to merge just the core-table columns
#
# Result column ordering:
#
#     {table} default, minus any excluded columns
#     'data_source' if present
#     foreign list columns in first-seen order
#     all foreign sub-DataFrame columns in first-seen order except the last two:
#     upstream_identifiers_data if present
#     external_reference_data if present
# 
#############################################################################################################################

def intersect_results(
    *result_dfs_to_merge,
    ignore_added_columns=False,
    table=None
):
    """
    Help me help you. Help text.
    """

    log = get_logger()

    # To do: move validation to validation.py

    if table is None:
        log.error( "'table' parameter cannot be omitted." )
        return
    elif table not in { 'file', 'subject' }:
        log.error( f"'table' parameter must be one of {{ 'file', 'subject' }}. You specified '{table}', which is neither." )
        return
    elif len( result_dfs_to_merge ) < 2:
        log.error( 'You need to specify at least two result DataFrames to be merged.' )
        return

    # Cache CDA table and column metadata from the API for downstream reuse without further
    # network disturbance. The data structure coming back from columns() is a DataFrame
    # with columns [ 'table', 'column', 'data_type', 'nullable', 'description' ].

    cached_column_metadata = columns()

    # Result column ordering:
    #
    #     {table} default, minus any excluded columns
    #     'data_source' if present
    #     foreign list columns in first-seen order
    #     all foreign sub-DataFrame columns in first-seen order except the last two:
    #     upstream_identifiers_data if present
    #     external_reference_data if present

    # Store the default column ordering for {table} as provided by the columns() function,
    # so all cdapython interfaces always display the same data in the same way
    # by default. In this case, we'll use this ordering to guide construction of
    # the {table} portion of the output we return.

    source_table_columns_in_order = list()

    for row_index, column_record in cached_column_metadata.iterrows():
        if column_record['table'] == table:
            # Remember the order in which columns() delivered the {table}'s columns.
            source_table_columns_in_order.append( column_record['column'] )
    # Gracefully handle the virtual 'data_source' column generated by cdapython during
    # its initial data fetches.
    source_table_columns_in_order.append( 'data_source' )
    # Track which of {table}'s columns we actually see.
    seen_source_table_columns = set()

    # For the rest of the columns we might encounter in our input DataFrames:

    # * if the column name exists in cached_column_metadata, we process it
    #   as a list of values from a foreign table (as generated according to the
    #   user's upstream query).
    foreign_table_value_list_order = list()
    # Save value-list data as we go.
    foreign_table_value_list_data_by_column_and_id = dict()

    # * If the column name is unknown to cached_column_metadata, we assume it's a
    #   DataFrame column created by cdapython containing covariant-grouped tabular
    #   results (again, as generated according to the user's upstream query).
    foreign_table_df_order = list()
    # Track column lists for (nonempty) sub-DataFrame values.
    foreign_table_df_columns = dict()
    # Save DataFrame-row data as we go.
    foreign_table_df_row_data_by_column_and_id = dict()

    # We make sure these are always at the end of our returned results, if present.
    seen_upstream_identifiers_data = False
    seen_external_reference_data = False

    # Make sure {table}_id is present, or we can't merge.
    main_id_column = f"{table}_id"
    if main_id_column not in result_dfs_to_merge[0].columns:
        log.error( f"Column '{main_id_column}' must be present in all input DataFrames." )
        return

    # Track all result columns which, if present anywhere, must be present everywhere.
    must_see_everywhere = { main_id_column }

    # Scan the list of columns in the first DataFrame the user sent, classify each,
    # and initialize relevant trackers.
    source_table_data_by_column_and_id = dict()
    for column_name in result_dfs_to_merge[0].columns:
        if column_name in source_table_columns_in_order:
            # This is a column (or virtual column a la 'data_source') from {table}.
            seen_source_table_columns.add( column_name )
            source_table_data_by_column_and_id[column_name] = dict()
        elif not ignore_added_columns:
            if column_name in cached_column_metadata['column'].unique():
                # This is a foreign-table value list.
                must_see_everywhere.add( column_name )
                foreign_table_value_list_order.append( column_name )
                foreign_table_value_list_data_by_column_and_id[column_name] = dict()
            else:
                # This is a DataFrame column created by cdapython containing covariant-grouped tabular results.
                foreign_table_df_columns[column_name] = list()
                foreign_table_df_row_data_by_column_and_id[column_name] = dict()
                if column_name == 'upstream_identifiers_data':
                    seen_upstream_identifiers_data = True
                elif column_name == 'external_reference_data':
                    must_see_everywhere.add( column_name )
                    seen_external_reference_data = True
                else:
                    foreign_table_df_order.append( column_name )
                    if column_name in { 'external_reference_data', 'file_data', 'project_data', 'subject_data' }:
                        must_see_everywhere.add( column_name )

    target_record_ids = set( result_dfs_to_merge[0][main_id_column].unique() )

    for result_df in result_dfs_to_merge:
        target_record_ids = target_record_ids & set( result_df[main_id_column].unique() )

    # Save result data as we go.

    for result_df in result_dfs_to_merge:
        for column_name in must_see_everywhere:
            # Make sure every DataFrame has the columns we need to produce a merged result set.
            if column_name not in result_df.columns:
                if column_name == main_id_column:
                    log.error( f"Column '{main_id_column}' must be present in all input DataFrames." )
                else:
                    log.error( f"If the optional 'ignore_added_columns' flag is not set to True, then all input DataFrames must have compatible columns. '{column_name}' is present in some but not all of your input DataFrames: cannot continue." )
                return
        for row_index, result_record in result_df.iterrows():
            main_id = result_record[main_id_column]
            if main_id in target_record_ids:
                for column_name in list( result_record.index ):
                    if column_name in source_table_columns_in_order:
                        # We might encounter some {table} columns that aren't in all input DataFrames.
                        # This is fine. See the discussion before this function's defline.
                        if column_name not in seen_source_table_columns:
                            seen_source_table_columns.add( column_name )
                            source_table_data_by_column_and_id[column_name] = dict()
                        if main_id in source_table_data_by_column_and_id[column_name]:
                            new_value = result_record[column_name]
                            if new_value != source_table_data_by_column_and_id[column_name][main_id]:
                                log.error( f"Unexpectedly encountered different clashing values across different input DataFrames for {table} record '{main_id}', column '{column_name}': '{source_table_data_by_column_and_id[column_name][main_id]}' vs. {new_value}'. Cannot continue." )
                                return
                        else:
                            source_table_data_by_column_and_id[column_name][main_id] = result_record[column_name]
                    elif not ignore_added_columns:
                        if column_name in cached_column_metadata['column'].unique():
                            # This is a foreign-table value list.
                            if column_name not in must_see_everywhere:
                                # We need these to be in all input DataFrames.
                                log.error( f"If the optional 'ignore_added_columns' flag is not set to True, then all input DataFrames must have compatible columns. '{column_name}' is present in some but not all of your input DataFrames: cannot continue." )
                                return
                            if main_id not in foreign_table_value_list_data_by_column_and_id[column_name]:
                                foreign_table_value_list_data_by_column_and_id[column_name][main_id] = set()
                            foreign_table_value_list_data_by_column_and_id[column_name][main_id] = foreign_table_value_list_data_by_column_and_id[column_name][main_id] | set( result_record[column_name] )
                        else:
                            # This is a DataFrame column created by cdapython containing covariant-grouped tabular results.
                            if column_name in { 'external_reference_data', 'file_data', 'project_data', 'subject_data' } and column_name not in must_see_everywhere:
                                # We need these to be in all input DataFrames, or none.
                                log.error( f"If the optional 'ignore_added_columns' flag is not set to True, then all input DataFrames must have compatible columns. '{column_name}' is present in some but not all of your input DataFrames: cannot continue." )
                                return
                            elif column_name not in foreign_table_df_columns:
                                foreign_table_df_order.append( column_name )
                                foreign_table_df_columns[column_name] = list()
                                foreign_table_df_row_data_by_column_and_id[column_name] = dict()
                            if len( foreign_table_df_columns[column_name] ) == 0 and len( result_record[column_name].columns ) > 0:
                                for sub_column_name in list( result_record[column_name].columns ):
                                    foreign_table_df_columns[column_name].append( sub_column_name )
                                if column_name in { 'file_data', 'project_data', 'subject_data' }:
                                    target_id_column = re.sub( r'_data$', r'', column_name ) + '_id'
                                    if target_id_column not in foreign_table_df_columns[column_name]:
                                        log.error( f"If the optional 'ignore_added_columns' flag is not set to True and your input DataFrames contain 'file_data', 'project_data' or 'subject_data' columns, the DataFrames in those columns must have 'file_id', 'project_id' and 'subject_id' columns, respectively, or we cannot merge the results." )
                                        return
                            elif len( result_record[column_name].columns ) > 0:
                                if list( result_record[column_name].columns ) != foreign_table_df_columns[column_name]:
                                    log.error( f"If the optional 'ignore_added_columns' flag is not set to True, then all input DataFrames must have compatible columns. '{column_name}' is present in multiple input DataFrames, but the columns present in '{column_name}' (sub-)DataFrames do not match across all (top-level) input DataFrames." )
                                    return
                            if main_id not in foreign_table_df_row_data_by_column_and_id[column_name]:
                                foreign_table_df_row_data_by_column_and_id[column_name][main_id] = pd.DataFrame()
                            if len( result_record[column_name].columns ) > 0:
                                if len( foreign_table_df_row_data_by_column_and_id[column_name][main_id].columns ) == 0:
                                    foreign_table_df_row_data_by_column_and_id[column_name][main_id] = result_record[column_name].copy()
                                else:
                                    results_with_dupes = pd.concat( [ foreign_table_df_row_data_by_column_and_id[column_name][main_id], result_record[column_name] ], ignore_index = True )
                                    # Evades 'unhashable type: list' error when trying to deduplicate rows whose cells contain list values.
                                    foreign_table_df_row_data_by_column_and_id[column_name][main_id] = results_with_dupes.loc[ results_with_dupes.astype(str).drop_duplicates().index ]

    if not ignore_added_columns:
        if seen_upstream_identifiers_data:
            foreign_table_df_order.append( 'upstream_identifiers_data' )
        if seen_external_reference_data:
            foreign_table_df_order.append( 'external_reference_data' )

    # Build result DataFrame.

    merged_result_df = pd.DataFrame()

    main_id_list = sorted( target_record_ids )

    for column_name in source_table_columns_in_order:
        if column_name in seen_source_table_columns:
            if column_name == main_id_column:
                merged_result_df[column_name] = main_id_list
            else:
                merged_result_df[column_name] = [ source_table_data_by_column_and_id[column_name][main_id] for main_id in main_id_list ]

    if not ignore_added_columns:
        for column_name in foreign_table_value_list_order:
            merged_result_df[column_name] = [ sorted( foreign_table_value_list_data_by_column_and_id[column_name][main_id] ) for main_id in main_id_list ]
        for column_name in foreign_table_df_order:
            merged_result_df[column_name] = [ foreign_table_df_row_data_by_column_and_id[column_name][main_id] for main_id in main_id_list ]

    return merged_result_df

#############################################################################################################################
#
# summarize_files(): Get a report describing columns of interest in the CDA file table, summarizing column values over
#                    all files matching user-supplied query filters. Optionally add columns from other tables, which are
#                    summarized across all of their own rows that are directly related to files matching the given filters.
#
#############################################################################################################################

def summarize_files(
    *,
    match_all=None,
    match_any=None,
    match_from_file={ 'input_file': '', 'input_column': '', 'cda_column_to_match': '' },
    data_source=None,
    add_columns=None,
    exclude_columns=None,
    return_data_as='',
    output_file=''
):
    """
    For a set of CDA file rows that all match a user-specified set of filters --
    "result rows" -- get a report showing counts of values present in that
    set of rows, profiled across (user-modifiable) columns of interest.

    Arguments:
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

    return summarize( table='file', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, return_data_as=return_data_as, output_file=output_file )

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
    match_all=None,
    match_any=None,
    match_from_file={ 'input_file': '', 'input_column': '', 'cda_column_to_match': '' },
    data_source=None,
    add_columns=None,
    exclude_columns=None,
    return_data_as='',
    output_file=''
):
    """
    For a set of CDA subject rows that all match a user-specified set of filters --
    "result rows" -- get a report showing counts of values present in that
    set of rows, profiled across (user-modifiable) columns of interest.

    Arguments:
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

        return_data_as ( string; optional: 'dataframe_list' or 'dict' or 'json' ):
            Specify how to return results: as a list of pandas DataFrames, as a
            Python dictionary, or as output written to a JSON file named by the user.
            If this argument is omitted, then for each DataFrame that would have
            been returned by the 'dataframe_list' option, a table will be
            pretty-printed to the standard output stream (and nothing will be returned).

        output_file( string; optional ):
            If return_data_as='json' is specified, output_file should contain a
            resolvable path to a file into which JSON-formatted results will be written.

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

    return summarize( table='subject', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, return_data_as=return_data_as, output_file=output_file )

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
    table='',
    *,
    match_all=None,
    match_any=None,
    match_from_file={ 'input_file': '', 'input_column': '', 'cda_column_to_match': '' },
    data_source=None,
    add_columns=None,
    exclude_columns=None,
    return_data_as='',
    output_file=''
):
    """
    For a set of rows in a user-specified table that all match a user-specified set of
    filters -- "result rows" -- get a report showing counts of values present in that
    set of rows, profiled across (user-modifiable) columns of interest.

    Arguments:
        table ( string; required: 'file' or 'subject' ):
            The CDA table to be queried and summarized.

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

    log = get_logger()

    #############################################################################################################################
    # Validate parameter inputs.
    #############################################################################################################################

    # Normalize user-supplied parameter data so we can assume from here on out that these are always lists of values:
    # convert any of the following that come in as single values (instead of lists of values) into one-element lists,
    # and leave the rest unmodified.

    # If someone can devise a way to do this with a control loop, I'm all ears. I gave up after 20 minutes
    # of fiddling with `locals()`.

    try:
        match_all = normalize_to_list( 'match_all', match_all, str )
        match_any = normalize_to_list( 'match_any', match_any, str )
        data_source = normalize_to_list( 'data_source', data_source, str )
        add_columns = normalize_to_list( 'add_columns', add_columns, str )
        exclude_columns = normalize_to_list( 'exclude_columns', exclude_columns, str )
    except Exception as e:
        log.error( e )
        return

    # Cache CDA table and column metadata from the API for downstream reuse without further
    # network disturbance. The data structure coming back from columns() is a DataFrame
    # with columns [ 'table', 'column', 'data_type', 'nullable', 'description' ].

    cached_column_metadata = columns()

    # Cache valid labels for upstream data sources. The data structure coming back from
    # release_metadata() is a list of dicts, with each dict looking like
    # 
    # {
    #     'cda_table': 'file',
    #     'cda_column': 'access',
    #     'data_source': 'GDC',
    #     'data_source_version': 'March 2025',
    #     'data_source_extraction_date': '2025-03-21',
    #     'data_source_row_count': 3025352,
    #     'data_source_unique_value_count': 4,
    #     'data_source_null_count': 407714
    # }

    try:
        cached_release_metadata = release_metadata()
    except Exception as e:
        log.critical( e )
        return

    valid_data_sources = set()

    for column_record in cached_release_metadata:
        record_data_source = column_record['data_source']
        if record_data_source != 'CDA':
            # Let's not care about case.
            valid_data_sources.add( record_data_source.upper() )

    # Validate user-supplied parameter data.

    try:
        validate_parameter_values(
            called_function='summarize',
            cached_column_metadata=cached_column_metadata,
            valid_data_sources=valid_data_sources,
            table=table,
            match_from_file=match_from_file,
            data_source=data_source,
            add_columns=add_columns,
            exclude_columns=exclude_columns,
            collate_results=None,
            include_external_refs=None,
            return_data_as=return_data_as,
            output_file=output_file,
            log=log
        )
    except Exception as e:
        # As of right now (2025-04), we return rather than halting the system via sys.exit() to avoid
        # completely crashing interactive sessions when errors are encountered. This behavior may be
        # updated in future to a more robust response.
        log.error( e )
        return

    #############################################################################################################################
    # Preprocess table metadata, to enable consistent processing (and reporting) throughout.
    #############################################################################################################################

    # Track the data type present in each CDA column, so we can
    # format results properly downstream. Among other things we need to
    # know details of numeric types, when constructing DataFrames to return
    # to the user, so we can compensate for pandas' inconsistent handling
    # of numeric null values.

    column_data_types = dict()

    # Store the default column ordering as provided by the columns() function,
    # so all cdapython interfaces always display the same data in the same way
    # by default.

    source_table_columns_in_order = list()

    for row_index, column_record in cached_column_metadata.iterrows():
        
        # Save the data_type of each CDA column.
        column_data_types[ column_record['column'] ] = column_record['data_type']

        if column_record['table'] == table:
            
            # Remember the order in which columns() delivered the source table's columns.
            source_table_columns_in_order.append( column_record['column'] )

    #############################################################################################################################
    # Construct query substructures according to user directives.
    #############################################################################################################################

    #############################################################################################################################
    # Manage basic validation for the `match_all` parameter, which enumerates user-specified requirements that returned
    # rows must all simultaneously satisfy (AND; intersection; 'all of these must apply').
    # 
    # Parse (and normalize: resolving various operator aliases, etc.) `match_all` filter expressions: complain if
    #
    #     * requested columns don't exist
    #     * illegal or type-inappropriate operators are used
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere but at the ends of a filter string
    #
    # ...and save parse results for each filter expression as a separate Query object (to be combined later).

    try:
        queries_for_match_all = validate_and_transform_match_filter_list( cached_column_metadata, match_all, enforce_column_uniqueness=True )
    except Exception as e:
        log.error( e )
        return

    #############################################################################################################################
    # Update `queries_for_match_all` to restrict results to optionally-specified `data_source` values.

    for upstream_data_source in data_source:
        queries_for_match_all.append( f"{table}_data_at_{upstream_data_source.lower()} = true" )

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned rows must satisfy at least one (OR; union; 'at least one of these must apply').
    # 
    # Parse (and normalize: resolving various operator aliases, etc.) `match_any` filter expressions: complain if
    #
    #     * requested columns don't exist
    #     * illegal or type-inappropriate operators are used
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere but at the ends of a filter string
    #
    # ...and save parse results for each filter expression as a separate Query object (to be combined later).

    try:
        queries_for_match_any = validate_and_transform_match_filter_list( cached_column_metadata, match_any, enforce_column_uniqueness=False )
    except Exception as e:
        log.error( e )
        return

    #############################################################################################################################
    # If not null, process match_from_file query information: load target values to match and check to see if records with
    # missing values in the target column should be included.

    if match_from_file['input_file'] != '':
        
        match_from_file_target_column = match_from_file['cda_column_to_match']
        match_from_file_target_data_type = column_data_types[ match_from_file_target_column ]

        # Load target values to search against.
        # 
        # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
        # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
        # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
        # anger and confusion when results don't match the input set along the given column.

        match_from_file_target_values = set()

        match_from_file_nulls_allowed = False

        try:
            
            with open( match_from_file['input_file'] ) as IN:
                
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )

                for next_line in IN:
                    
                    record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                    target_value = record[match_from_file['input_column']]

                    if target_value is None or target_value == '' or target_value == '<NA>':
                        match_from_file_nulls_allowed = True
                    else:
                        match_from_file_target_values.add( target_value )

        except Exception as error:
            
            log.error( f"Couldn't load data from requested column '{match_from_file['input_column']}' from requested TSV file '{match_from_file['input_file']}': got error of type '{type( error )}', with error message '{error}'.")
            return

        # Parse target values: complain if
        #
        #     * values don't match the data types of the columns they're paired with
        #     * wildcards appear anywhere (they're not compatible with the IN keyword, and we don't currently support the construction of per-value LIKE filters)
        #
        # ...strip apostrophes, and save parse results as a processed set of valid values.

        try:
            processed_target_values = validate_and_transform_match_from_file_values( match_from_file_target_column, match_from_file_target_data_type, match_from_file_target_values )
        except Exception as e:
            log.error( e )
            return

        # Create API filter strings from processed `match_from_file` input data.

        match_from_file_filter_strings = set()

        if match_from_file_nulls_allowed:
            match_from_file_filter_strings.add( f"{match_from_file_target_column} is null" )

        if match_from_file_target_data_type == 'text' and len( processed_target_values ) > 0:
            match_from_file_filter_strings.add( f"{match_from_file_target_column} in [ '" + "', '".join( processed_target_values ) + "' ]" )

        # Add results to the queries_for_match_any list.

        initial_match_any_filter_strings = set( queries_for_match_any )

        queries_for_match_any = list( initial_match_any_filter_strings | match_from_file_filter_strings )

    #############################################################################################################################
    # Parse `add_columns` and `exclude_columns` lists.

    columns_to_add = list()

    for column_to_add in add_columns:
        
        # Ignore requests for columns that are already present by default, and don't add columns twice.
        if column_to_add not in source_table_columns_in_order and column_to_add not in columns_to_add:
            columns_to_add.append( column_to_add )
    
    columns_to_exclude = list()

    suppress_data_source_results = False

    for column_to_exclude in exclude_columns:
        
        # Handle 'data_source' explicitly; it's a user-facing summary column the API neither knows
        # nor needs to care about.

        if column_to_exclude.lower() == 'data_source':
            suppress_data_source_results = True

        # Ignore requests to exclude columns that are already excluded. Let the API sort out
        # what to do if a user requests to both add and exclude a column.

        if column_to_exclude not in columns_to_exclude:
            columns_to_exclude.append( column_to_exclude )

    #############################################################################################################################
    # Build an object to represent our upcoming API query.

    query_object = SummaryRequestBody()
    query_object.match_all = queries_for_match_all
    query_object.match_some = queries_for_match_any
    query_object.add_columns = columns_to_add
    query_object.exclude_columns = columns_to_exclude

    #############################################################################################################################
    # Fetch data from the API.
    #############################################################################################################################

    # Support selection of the appropriate endpoint based on the value of `table`.

    query_selector = {
        'file': summary_file_endpoint,
        'subject': summary_subject_endpoint
    }

    # Try to get data from the REST API.

    log.debug( f"Sending query to API '/summary/{table}' endpoint:\n{json.dumps( query_object.to_dict(), indent=4 )}\n" )
    
    query_api_instance = cda_client.Client( base_url=get_api_url(), raise_on_unexpected_status=True )

    try:
        api_response_object = query_selector[table].sync(
            client=query_api_instance,
            body=query_object
        )
    except UnexpectedStatus as error:
        log.error( f"UnexpectedStatus error from API, status code {error.status_code}: {json.loads( error.content )['message']}" )
        return
    except Exception as error:
        log.error( f"{type(error)}: {error}" )
        return

    # Forward error types known to be returned by the API.
    if isinstance( api_response_object, ClientError ) or isinstance( api_response_object, InternalError ):
        log.error( f"{api_response_object.error_type}: {api_response_object.message}" )
        return

    # Sample JSON response from a /summary endpoint:
    # 
    # {
    #   "result": [
    #     {
    #       "total_count": 6904,
    #       "file_count": 584670,
    #       "species_summary": [
    #         {
    #           "species": "human",
    #           "count_result": 6904
    #         }
    #       ],
    #       "year_of_birth_summary": [
    #         {
    #           "min": 1908,
    #           "max": 2010,
    #           "mean": 1958,
    #           "median": 1956,
    #           "lower_quartile": 1942,
    #           "upper_quartile": 1971
    #         }
    #       ],
    #       "year_of_death_summary": [
    #         {
    #           "min": 1996,
    #           "max": 2022,
    #           "mean": 2009,
    #           "median": 2007,
    #           "lower_quartile": 2002,
    #           "upper_quartile": 2018
    #         }
    #       ],
    #       "cause_of_death_summary": [
    #         {
    #           "cause_of_death": null,
    #           "count_result": 6683
    #         },
    #         {
    #           "cause_of_death": "Non-Cancer Related Death",
    #           "count_result": 28
    #         },
    #         {
    #           "cause_of_death": "Surgical Complication",
    #           "count_result": 4
    #         },
    #         {
    #           "cause_of_death": "Cancer-Related Death",
    #           "count_result": 186
    #         },
    #         {
    #           "cause_of_death": "Cardiovascular Disorder",
    #           "count_result": 3
    #         }
    #       ],
    #       "race_summary": [
    #         {
    #           "race": null,
    #           "count_result": 902
    #         },
    #         {
    #           "race": "Black or African American",
    #           "count_result": 596
    #         },
    #         {
    #           "race": "Asian",
    #           "count_result": 431
    #         },
    #         {
    #           "race": "More than one race",
    #           "count_result": 14
    #         },
    #         {
    #           "race": "White",
    #           "count_result": 4922
    #         },
    #         {
    #           "race": "American Indian or Alaska Native",
    #           "count_result": 24
    #         },
    #         {
    #           "race": "Native Hawaiian or Other Pacific Islander",
    #           "count_result": 15
    #         }
    #       ],
    #       "ethnicity_summary": [
    #         {
    #           "ethnicity": null,
    #           "count_result": 1595
    #         },
    #         {
    #           "ethnicity": "Non-Hispanic",
    #           "count_result": 4903
    #         },
    #         {
    #           "ethnicity": "Hispanic or Latino",
    #           "count_result": 406
    #         }
    #       ],
    #       "subject_data_source_count_summary": [
    #         {
    #           "min": 1,
    #           "max": 4,
    #           "mean": 2,
    #           "median": 2,
    #           "lower_quartile": 2,
    #           "upper_quartile": 2
    #         }
    #       ],
    #       "data_source": {
    #         "cds_exclusive": 235,
    #         "gdc_exclusive": 351,
    #         "icdc_exclusive": 0,
    #         "idc_exclusive": 0,
    #         "pdc_exclusive": 6,
    #         "cds_gdc_exclusive": 0,
    #         "cds_icdc_exclusive": 0,
    #         "cds_idc_exclusive": 898,
    #         "cds_pdc_exclusive": 0,
    #         "gdc_icdc_exclusive": 0,
    #         "gdc_idc_exclusive": 4544,
    #         "gdc_pdc_exclusive": 7,
    #         "icdc_idc_exclusive": 0,
    #         "icdc_pdc_exclusive": 0,
    #         "idc_pdc_exclusive": 0,
    #         "cds_gdc_icdc_exclusive": 0,
    #         "cds_gdc_idc_exclusive": 574,
    #         "cds_gdc_pdc_exclusive": 1,
    #         "cds_icdc_idc_exclusive": 0,
    #         "cds_icdc_pdc_exclusive": 0,
    #         "cds_idc_pdc_exclusive": 0,
    #         "gdc_icdc_idc_exclusive": 0,
    #         "gdc_icdc_pdc_exclusive": 0,
    #         "gdc_idc_pdc_exclusive": 205,
    #         "icdc_idc_pdc_exclusive": 0,
    #         "cds_gdc_icdc_idc_exclusive": 0,
    #         "cds_gdc_icdc_pdc_exclusive": 0,
    #         "cds_gdc_idc_pdc_exclusive": 83,
    #         "cds_icdc_idc_pdc_exclusive": 0,
    #         "gdc_icdc_idc_pdc_exclusive": 0,
    #         "cds_gdc_icdc_idc_pdc": 0
    #       },
    #       "sex": [
    #         {
    #           "sex": null,
    #           "count_result": 6
    #         },
    #         {
    #           "sex": "female",
    #           "count_result": 3591
    #         },
    #         {
    #           "sex": "male",
    #           "count_result": 3309
    #         }
    #       ]
    #     }
    #   ],
    #   "query_sql": "WITH subject_preselect AS (SELECT subject.id AS subject_id, subject.id_alias AS subject_id_alias, [...these are very long!...] AS cds_gdc_icdc_idc_pdc) AS subquery) AS data_source, (SELECT observation_columns.sex FROM observation_columns) AS sex) AS json_result"
    # }

    # Report some metadata about the results we got back.

    log.debug( f"/summary/{table} endpoint query SQL:\n{api_response_object.to_dict()['query_sql']}" )

    # This is immensely verbose, sometimes.

    log.debug( f"/summary/{table} endpoint results:\n{json.dumps( api_response_object.to_dict()['result'], indent=4 )}\n" )

    #############################################################################################################################
    # Postprocess API result data.
    #############################################################################################################################

    log.debug( "Organizing result data..." )

    # Make a dict out of the results so we can restructure a bit before DataFrame conversion.

    api_response_dict = api_response_object.to_dict()['result'][0]

    # Wrap the 'data_source' response element in a list to avoid splitting the entries into individual columns
    # when converting into a DataFrame.

    api_response_dict['data_source'] = [api_response_dict['data_source']]

    # Convert response JSON into a DataFrame using pandas' json_normalize() function.

    result_dataframe = pd.json_normalize( [api_response_dict] )

    # For some reason, the highest-level summary counts come through as floats. Fix that
    # (and rename them while we're at it).

    toplevel_columns_to_fix = {
        
        'total_count': 'number_of_matching_files' if table == 'file' else 'number_of_matching_subjects' if table == 'subject' else 'number_of_matching_rows',
        'file_count': 'number_of_files_related_to_matching_subjects',
        'subject_count': 'number_of_subjects_related_to_matching_files'
    }

    for result_column in toplevel_columns_to_fix:
        
        if result_column in result_dataframe:
            
            result_dataframe[result_column] = result_dataframe[result_column].round().astype( int )

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

    #############################################################################################################################
    # Build a response for the user according to the directives we got, and send the result back to them.
    #############################################################################################################################

    if return_data_as == '' or return_data_as == 'dataframe_list':
        
        # Right now, the default is to print one table to standard output
        # for each DataFrame that would be returned had the user requested
        # `return_data_as='dataframe_list'`.

        result_list = list()

        # Return overall result summary counts first.

        for toplevel_column in [ 'number_of_matching_files', 'number_of_matching_subjects', 'number_of_matching_rows', 'number_of_files_related_to_matching_subjects', 'number_of_subjects_related_to_matching_files' ]:
            
            if toplevel_column in result_dataframe:
                
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                result_list.append( pd.DataFrame( result_dataframe[toplevel_column], columns=[toplevel_column] ) )

        # Next, summarize data sources unless `exclude_columns='data_source'` was specified by the user.

        if not suppress_data_source_results:
            
            output_data_source_dict = {
                'data_source': list(),
                f"{table}s": list()
            }

            if result_dataframe['data_source'] is not None:
                
                input_data_source_dict = result_dataframe['data_source'][0][0]

                # This cell should be a Python dict pairing some combination of valid data sources with a count of matching results.

                for data_source_combo in input_data_source_dict:
                    
                    current_count = input_data_source_dict[data_source_combo]

                    if current_count is not None and current_count != 0:
                        
                        data_source_combo = re.sub( r'_exclusive$', r'', data_source_combo )

                        if re.search( r'_', data_source_combo ) is None:
                            
                            data_source_combo = f"{data_source_combo.upper()} only"

                        else:
                            
                            data_source_combo = " + ".join( data_source_combo.upper().split( '_' ) )

                        output_data_source_dict[f"{table}s"].append( current_count )
                        output_data_source_dict['data_source'].append( data_source_combo )

            result_list.append( pd.DataFrame.from_dict( output_data_source_dict ).sort_values( by=f"{table}s", ascending=False ).reset_index( drop=True ) )

        # Put the numeric summaries at the end of the displayed block of results.

        result_list_tail = list()

        for result_column in result_dataframe.columns:
            
            if result_column not in skip_rename and result_dataframe[result_column].dtype == 'object' and isinstance( result_dataframe[result_column][0], list ) and isinstance( result_dataframe[result_column][0][0], dict ) and 'median' in result_dataframe[result_column][0][0]:
                
                # These are one-element arrays, with the element being a key/value dictionary containing summary stats.
                # 
                # They come back with null values if there are no results. In such a case, we don't want to include this structure in our output.

                if result_dataframe[result_column][0][0]['median'] is not None:
                    
                    result_column_dict = dict()

                    result_column_dict['cda_column_name'] = [result_column]

                    # Hard-coding this is fragile, but safe for now and there's a lot to do.

                    for key in [ 'mean', 'min', 'lower_quartile', 'median', 'upper_quartile', 'max' ]:
                        
                        result_column_dict[key] = [result_dataframe[result_column][0][0][key]]

                    result_list_tail.append( pd.DataFrame.from_dict( result_column_dict ).reset_index( drop=True ) )

            elif result_column not in skip_rename:
                
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                if result_dataframe[result_column].dtype == 'int64':
                    
                    result_dataframe[result_column] = int( result_dataframe[result_column][0] )

                elif result_dataframe[result_column].dtype == 'object':
                    
                    result_column_dict = {
                        result_column: list(),
                        'count_result': list()
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
                            
                            print_value = '<NA>'

                            actual_value = dict_pair[result_column]

                            if actual_value is not None and actual_value != '':
                                
                                print_value = actual_value

                            result_column_dict[result_column].append( print_value )

                            result_column_dict['count_result'].append( dict_pair['count_result'] )

                    result_list.append( pd.DataFrame.from_dict( result_column_dict ).sort_values( by=[ 'count_result', result_column ], ascending=[ False, True ] ).reset_index( drop=True ) )

                else:
                    
                    log.critical( f"Unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event." )
                    return

        result_list = result_list + result_list_tail

        if return_data_as == '':
            
            log.debug( 'Returning results in default form (printing list of tables to standard output)' )

            with pd.option_context( 'display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', 65 ):
                
                for result_list_df in result_list:
                    
                    print_df = result_list_df

                    max_col_width = 80

                    maxcolwidths_list = [ None ]

                    colalign_list = [ 'left' ]

                    if print_df is not None and len( print_df ) > 0:
                        
                        if len( print_df.columns ) == 1:
                            
                            colalign_list = [ 'left' ]

                        elif 'count_result' in print_df.columns.values or f"{table}s" in print_df.columns.values:
                            
                            maxcolwidths_list = [ None, max_col_width ]

                            colalign_list = [ 'right', 'right' ]

                            # Truncate displayed text values manually and add ellipses. The `tabulate` library doesn't do this on its own (as Pandas does).

                            print_df[print_df.columns[0]] = print_df[print_df.columns[0]].apply( lambda x: re.sub( f"^(.{{{max_col_width-3}}}).*", r"\1...", x ) if ( x is not None and not isinstance( x, bool ) and len( x ) > max_col_width ) else x )

                            # Put the count values first in the display.

                            new_column_ordering = list( reversed( print_df.columns.tolist() ) )

                            print_df = print_df[new_column_ordering]

                        elif 'median' in print_df.columns.values:
                            
                            colalign_list = [ 'right' ]

                            result_name = print_df['cda_column_name'][0]

                            result_dict = {
                                
                                '': list(),
                                result_name: list()
                            }

                            # Hard-coding this is fragile, but safe for now and there's a lot to do.

                            for key in [ 'mean', 'min', 'lower_quartile', 'median', 'upper_quartile', 'max' ]:
                                
                                result_dict[''].append( f"{re.sub( r'_', r' ', key )}" )

                                result_dict[result_name].append( f"{print_df[key][0]:>15}" )

                            print_df = pd.DataFrame.from_dict( result_dict ).reset_index( drop=True )

                        # Suppress output of confusing row-index column when displaying DataFrame contents and get some control over cell alignment.

                        print(
                            tabulate.tabulate(
                                print_df,
                                showindex=False,
                                headers=print_df.columns,
                                tablefmt='double_outline',
                                colalign=colalign_list,
                                maxcolwidths=maxcolwidths_list,
                                disable_numparse=True,
                            )
                        )

            return

        elif return_data_as == 'dataframe_list':
            
            log.debug( 'Returning results as a list of pandas.DataFrame objects' )

            return result_list

    elif return_data_as == 'dict' or return_data_as == 'json':
        
        # Build a Python dictionary to shape returned results.

        result_dict = dict()

        for result_column in result_dataframe.columns:
            
            if ( result_column not in skip_rename or re.search( r'_data_source_count_summary$', result_column ) is not None ) and result_dataframe[result_column].dtype == 'object' and isinstance( result_dataframe[result_column][0], list ) and isinstance( result_dataframe[result_column][0][0], dict ) and 'median' in result_dataframe[result_column][0][0]:
                
                # These are one-element arrays, with the element being a key/value dictionary containing summary stats.
                # 
                # They come back with null values if there are no results. In such a case, we don't want to include this structure in our output.

                if result_dataframe[result_column][0][0]['median'] is not None:
                    
                    result_dict[result_column] = dict()

                    # Hard-coding this is fragile, but safe for now and there's a lot to do.

                    for key in [ 'mean', 'min', 'lower_quartile', 'median', 'upper_quartile', 'max' ]:
                        
                        result_dict[result_column][key] = result_dataframe[result_column][0][0][key]

            elif result_column == 'data_source':
                
                if result_dataframe['data_source'] is not None:
                    
                    input_data_source_dict = result_dataframe['data_source'][0][0]

                    # This cell should be a Python dict pairing some combination of valid data sources with a count of matching results.

                    for data_source_combo in input_data_source_dict:
                        
                        current_count = input_data_source_dict[data_source_combo]

                        if current_count is not None and current_count != 0:
                            
                            data_source_combo = re.sub( r'_exclusive$', r'', data_source_combo )

                            if re.search( r'_', data_source_combo ) is None:
                                
                                data_source_combo = f"{data_source_combo.upper()} only"

                            else:
                                
                                data_source_combo = " and ".join( data_source_combo.upper().split( '_' ) )

                            if 'data_source' not in result_dict:
                                
                                result_dict['data_source'] = dict()

                            result_dict['data_source'][data_source_combo] = current_count

            else:
                
                if result_dataframe[result_column].dtype == 'int64':
                    
                    result_dict[result_column] = int( result_dataframe[result_column][0] )

                elif result_dataframe[result_column].dtype == 'object':
                    
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
                            
                            result_dict[result_column][dict_pair[result_column]] = dict_pair['count_result']

                else:
                    
                    log.critical( f"Unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event." )
                    return

        if return_data_as == 'dict':
            
            log.debug( 'Returning results as a Python dictionary' )

            return result_dict

        elif return_data_as == 'json':
            
            # Write the results to a user-specified JSON file.

            log.debug( f"Printing results to JSON file '{output_file}'" )

            try:
                
                with open( output_file, 'w' ) as OUT:
                    
                    json.dump( result_dict, OUT, indent=4, ensure_ascii=True )

                return

            except Exception as error:
                
                log.error( f"Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'." )

                return

    log.critical( 'Something has gone unexpectedly and disastrously wrong with return-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error.' )
    return

#############################################################################################################################
#
# END summarize()
#
#############################################################################################################################


