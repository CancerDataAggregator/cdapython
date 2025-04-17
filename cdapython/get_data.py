import json
import numpy
import pandas as pd
import re

import cda_client
import cda_client.api.data.file_fetch_rows_endpoint_data_file_post
import cda_client.api.data.subject_fetch_rows_endpoint_data_subject_post

from cdapython.application_utilities import build_match_from_file_filter, get_api_url
from cdapython.discover import columns, release_metadata
from cdapython.logging_wrappers import get_logger
from cdapython.validation import normalize_to_list, validate_and_transform_match_filter_list, validate_parameter_values

from cda_client.models.client_error import ClientError
from cda_client.models.internal_error import InternalError
from cda_client.models.q_node import QNode


#############################################################################################################################
#############################################################################################################################
# Nomenclature notes:
#
# * try to standardize all potential user-facing synonyms for basic database data structures
#   (field, entity, endpoint, cell, value, term, etc.) to 'table', 'column', 'row' and 'value'.
#############################################################################################################################
#############################################################################################################################


#############################################################################################################################
#
# get_file_data( ): Get CDA file data rows ('result rows') that match user-specified criteria.
#
#############################################################################################################################

def get_file_data(
    *,
    match_all=[],
    match_any=[],
    match_from_file={'input_file': '', 'input_column': '', 'cda_column_to_match': ''},
    data_source=[],
    add_columns=[],
    exclude_columns=[],
    provenance=False,
    return_data_as='dataframe',
    output_file=''
):
    """
    Get CDA file rows ('result rows') that match user-specified criteria.

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
            One or more columns from a second table to add to result data.

        exclude_columns ( string or list of strings; optional ):
            One or more columns to remove from result data.

        provenance ( boolean; optional ):
            If True, attach cross-reference information to each result row
            identifying that row in the context of the upstream data source(s)
            from which it was derived.

        return_data_as ( string; optional: 'dataframe' or 'tsv' ):
            Specify how to return results: as a pandas DataFrame,
            or as output written to a TSV file named by the user. If this
            argument is omitted, the default is to return results as a DataFrame.

        output_file ( string; optional ):
            If return_data_as='tsv' is specified, `output_file` should contain a
            resolvable path to a file into which tab-delimited results will be
            written.

    Filter strings:
        Filter strings are expressions of the form 'COLUMN_NAME OP VALUE'
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

            diagnosis = *duct*
            sex = F*

        String VALUEs need not be quoted inside of filter strings. For example, to include
        the filters specified just above in the `match_all` argument, we can write:

            get_file_data( match_all=[ 'diagnosis = *duct*', 'sex = F*' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get CDA file data for which the `cause_of_death` field
        is missing data in associated subject rows, we can write:

            get_file_data( match_all=[ 'cause_of_death = NULL' ] )

    Returns:
        (Default) A pandas.DataFrame containing CDA file data matching the user-specified
            filter criteria. The DataFrame's named columns will match columns in the `file` table
            plus any optional user-added columns from other tables, and each row in the DataFrame
            will represent one CDA `file` row (possibly with related data from other tables
            appended to it, according to user directives).

        OR returns nothing, but writes results to a user-specified TSV file.

    """

    return get_data( table='file', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, provenance=provenance, return_data_as=return_data_as, output_file=output_file )

#############################################################################################################################
#
# get_subject_data( ): Get CDA subject data rows ('result rows') that match user-specified criteria.
#
#############################################################################################################################

def get_subject_data(
    *,
    match_all=[],
    match_any=[],
    match_from_file={'input_file': '', 'input_column': '', 'cda_column_to_match': ''},
    data_source=[],
    add_columns=[],
    exclude_columns=[],
    provenance=False,
    return_data_as='dataframe',
    output_file=''
):
    """
    Get CDA subject rows ('result rows') that match user-specified criteria.

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
            One or more columns from a second table to add to result data.

        exclude_columns ( string or list of strings; optional ):
            One or more columns to remove from result data.

        provenance ( boolean; optional ):
            If True, attach cross-reference information to each result row
            identifying that row in the context of the upstream data source(s)
            from which it was derived.

        return_data_as ( string; optional: 'dataframe' or 'tsv' ):
            Specify how to return results: as a pandas DataFrame,
            or as output written to a TSV file named by the user. If this
            argument is omitted, the default is to return results as a DataFrame.

        output_file ( string; optional ):
            If return_data_as='tsv' is specified, `output_file` should contain a
            resolvable path to a file into which tab-delimited results will be
            written.

    Filter strings:
        Filter strings are expressions of the form 'COLUMN_NAME OP VALUE'
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

            diagnosis = *duct*
            sex = F*

        String VALUEs need not be quoted inside of filter strings. For example, to include
        the filters specified just above in the `match_all` argument, we can write:

            get_subject_data( match_all=[ 'diagnosis = *duct*', 'sex = F*' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get CDA subject data for which the `cause_of_death` field
        is missing data, we can write:

            get_subject_data( match_all=[ 'cause_of_death = NULL' ] )

    Returns:
        (Default) A pandas.DataFrame containing CDA subject data matching the user-specified
            filter criteria. The DataFrame's named columns will match columns in the `subject` table
            plus any optional user-added columns from other tables, and each row in the DataFrame
            will represent one CDA `subject` row (possibly with related data from other tables
            appended to it, according to user directives).

        OR returns nothing, but writes results to a user-specified TSV file.

    """

    return get_data( table='subject', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, provenance=provenance, return_data_as=return_data_as, output_file=output_file )

#############################################################################################################################
#
# get_data( table=`table` ): Get CDA data rows ('result rows') from `table` that match user-specified criteria.
#
#############################################################################################################################

def get_data(
    table=None,
    *,
    match_all=[],
    match_any=[],
    match_from_file={'input_file': '', 'input_column': '', 'cda_column_to_match': ''},
    data_source=[],
    add_columns=[],
    exclude_columns=[],
    provenance=False,
    return_data_as='dataframe',
    output_file=''
):
    """
    Get CDA data rows ('result rows') from `table` that match user-specified criteria.

    Arguments:
        table ( string; required: 'file' or 'subject' ):
            The CDA table whose rows are to be filtered and retrieved.

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
            One or more columns from a second table to add to result data from `table`.

        exclude_columns ( string or list of strings; optional ):
            One or more columns to remove from result data.

        provenance ( boolean; optional ):
            If True, get_data() will attach cross-reference information
            to each result row identifying that row in the context of
            the upstream data source(s) from which it was derived.

        return_data_as ( string; optional: 'dataframe' or 'tsv' ):
            Specify how get_data() should return results: as a pandas DataFrame,
            or as output written to a TSV file named by the user. If this
            argument is omitted, get_data() will default to returning
            results as a DataFrame.

        output_file ( string; optional ):
            If return_data_as='tsv' is specified, `output_file` should contain a
            resolvable path to a file into which get_data() will write
            tab-delimited results.

    Filter strings:
        Filter strings are expressions of the form 'COLUMN_NAME OP VALUE'
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

            diagnosis = *duct*
            sex = F*

        String VALUEs need not be quoted inside of filter strings. For example, to include
        the filters specified just above in the `match_all` argument, when querying
        the `subject` table, we can write:

            get_data( table='subject', match_all=[ 'diagnosis = *duct*', 'sex = F*' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get `subject` rows where the `cause_of_death` field
        is missing data, we can write:

            get_data( table='subject', match_all=[ 'cause_of_death = NULL' ] )

    Returns:
        (Default) A pandas.DataFrame containing CDA `table` rows matching the user-specified
            filter criteria. The DataFrame's named columns will match columns in `table` plus
            any optional user-added columns from other tables, and each row in the DataFrame
            will represent one CDA `table` row (possibly with related data from other tables
            appended to it, according to user directives).

        OR returns nothing, but writes results to a user-specified TSV file.

    """

    # TO DO: re-enable provenance parameter
    # TO DO: re-enable match_from_file parameter

    log = get_logger()

    #############################################################################################################################
    # Validate parameter inputs.

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
    #     'data_source': 'CDA',
    #     'data_source_version': 'March 2025',
    #     'data_source_extraction_date': '2025-03-21',
    #     'data_source_row_count': 3025352,
    #     'data_source_unique_value_count': 4,
    #     'data_source_null_count': 407714
    # }

    cached_release_metadata = release_metadata()

    valid_data_sources = set()

    for column_record in cached_release_metadata:
        record_data_source = column_record['data_source']
        if record_data_source != 'CDA':
            # Let's not care about case.
            valid_data_sources.add( record_data_source.upper() )

    # Validate user-supplied parameter data.

    print( add_columns )

    try:
        validate_parameter_values(
            'get_data',
            cached_column_metadata,
            valid_data_sources,
            table,
            match_from_file,
            data_source,
            add_columns,
            exclude_columns,
            provenance,
            return_data_as,
            output_file,
            log
        )
    except Exception as e:
        log.error( e )
        return

    #############################################################################################################################
    # Preprocess table metadata, to enable consistent processing (and reporting) throughout.

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

    # Manage basic validation for the `match_all` parameter, which enumerates user-specified requirements that returned
    # rows must all simultaneously satisfy (AND; intersection; 'all of these must apply').
    # 
    # Validate and normalize match_all filter strings; save results as a list of statement strings.

    try:
        queries_for_match_all = validate_and_transform_match_filter_list( cached_column_metadata, match_all )
    except Exception as e:
        log.error( e )
        return

    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned rows must satisfy at least one (OR; union; 'at least one of these must apply').
    # 
    # Validate and normalize match_any filter strings; save results as a list of statement strings.

    try:
        queries_for_match_any = validate_and_transform_match_filter_list( cached_column_metadata, match_any )
    except Exception as e:
        log.error( e )
        return

    # Update `queries_for_match_all` to restrict results to optionally-specified `data_source` values.

    for upstream_data_source in data_source:
        queries_for_match_all.append( f"{table}_data_at_{upstream_data_source.lower()} = True" )

    # Make sure to retrieve the columns we need for data source summary output (whether or not
    # the data_source filter was used by the user, we summarize upstream data sources by default).
    # These columns are not returned by default from the API.

    for upstream_data_source in valid_data_sources:
        if f"{table}_data_at_{upstream_data_source.lower()}" not in add_columns:
            add_columns.append( f"{table}_data_at_{upstream_data_source.lower()}" )



    #############################################################################################################################
    ### NOT WORKING, PLEASE UPDATE

    if match_from_file['cda_column_to_match'] != '':
        target_data_type = columns(column=match_from_file['cda_column_to_match'])['data_type'][0]
        match_from_file_filter = build_match_from_file_filter(match_from_file, target_data_type, log)
        #TO DO: should this be added to match_all always?
        queries_for_match_all.append(match_from_file_filter)

    ### END NOT WORKING BLOCK
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

    query_object = QNode()
    query_object.match_all = queries_for_match_all
    query_object.match_some = queries_for_match_any
    query_object.add_columns = columns_to_add
    query_object.exclude_columns = columns_to_exclude

    #############################################################################################################################
    # Fetch data from the API.

    query_selector = {
        'file': cda_client.api.data.file_fetch_rows_endpoint_data_file_post,
        'subject': cda_client.api.data.subject_fetch_rows_endpoint_data_subject_post,
    }

    # We return all results to users at once. Paging can occur internally, but is made
    # transparent to the user. Track offset and page size in case we have to handle paged
    # results.

    starting_offset = 0
    rows_per_page = 500000

    # Use the QueryApi instance object's `{table}_query` endpoint-accessor function to get data from the REST API.

    log.debug( f"Sending query to API '/data/{table}' endpoint:\n{json.dumps( query_object.to_dict(), indent=4 )}\n" )
    
    query_api_instance = cda_client.Client( base_url=get_api_url() )

    paged_response_data_object = query_selector[table].sync(
        client=query_api_instance,
        body=query_object,
        limit=rows_per_page,
        offset=starting_offset
    )

    # Forward error types known to be returned by the API.
    if isinstance( paged_response_data_object, ClientError ) or isinstance( paged_response_data_object, InternalError ):
        log.error( f"{paged_response_data_object.error_type}: {paged_response_data_object.message}" )
        return

    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function. Example JSON response ( Note not all of these columns are returned by default: some were requested, others induced by a non-null `data_source` parameter):
    #
    # {
    #     "result": [
    #         {
    #             "subject_id": "TCGA.TCGA-AA-A022",
    #             "subject_crdc_id": null,
    #             "species": "human",
    #             "year_of_birth": 1917,
    #             "year_of_death": null,
    #             "cause_of_death": null,
    #             "race": null,
    #             "ethnicity": null,
    #             "subject_data_at_gdc": true,
    #             "subject_data_at_idc": true,
    #             "subject_data_at_cds": false,
    #             "subject_data_at_pdc": true,
    #             "subject_data_at_icdc": false,
    #             "sex": [
    #                 "female"
    #             ]
    #         },
    #         
    #         ...
    #         
    #         {
    #             "subject_id": "TCGA.TCGA-BH-A18N",
    #             "subject_crdc_id": null,
    #             "species": "human",
    #             "year_of_birth": 1913,
    #             "year_of_death": 2004,
    #             "cause_of_death": null,
    #             "race": "White",
    #             "ethnicity": "Non-Hispanic",
    #             "subject_data_at_gdc": true,
    #             "subject_data_at_idc": true,
    #             "subject_data_at_cds": false,
    #             "subject_data_at_pdc": true,
    #             "subject_data_at_icdc": false,
    #             "sex": [
    #                 "female"
    #             ]
    #         }
    #     ],
    #     "query_sql": "WITH subject_preselect AS (SELECT subject.id_alias AS id_alias FROM subject WHERE (EXISTS (SELECT 1 FROM observation WHERE subject.id_alias = observation.subject_alias AND coalesce(upper(observation.sex), :coalesce_2) = upper(:upper_1))) AND subject.year_of_birth < :year_of_birth_1 AND subject.data_at_gdc = true), observation_subject_columns AS (SELECT array_remove(array_agg(DISTINCT observation.sex), NULL) AS sex, observation.subject_alias AS subject_alias FROM observation WHERE observation.subject_alias IN (SELECT subject_preselect.id_alias FROM subject_preselect) GROUP BY observation.subject_alias) SELECT row_to_json(json_result) AS row_to_json_1 FROM (SELECT subject.id AS subject_id, subject.crdc_id AS subject_crdc_id, subject.species AS species, subject.year_of_birth AS year_of_birth, subject.year_of_death AS year_of_death, subject.cause_of_death AS cause_of_death, subject.race AS race, subject.ethnicity AS ethnicity, subject.year_of_birth AS year_of_birth, subject.data_at_gdc AS subject_data_at_gdc, subject.data_at_idc AS subject_data_at_idc, subject.data_at_cds AS subject_data_at_cds, subject.data_at_pdc AS subject_data_at_pdc, subject.data_at_gdc AS subject_data_at_gdc, subject.data_at_icdc AS subject_data_at_icdc, coalesce(observation_subject_columns.sex, :coalesce_1) AS sex FROM subject LEFT OUTER JOIN observation_subject_columns ON observation_subject_columns.subject_alias = subject.id_alias WHERE subject.id_alias IN (SELECT subject_preselect.id_alias FROM subject_preselect)) AS json_result",
    #     "total_row_count": 9,
    #     "next_url": ""
    # }

    log.debug( f"Page one results:\n{json.dumps( paged_response_data_object.to_dict(), indent=4 )}\n" )
    
    result_dataframe = pd.json_normalize( paged_response_data_object.to_dict()['result'] )

    # The data we've fetched so far might be just the first page (if the total number
    # of results is greater than `rows_per_page`).
    #
    # Get the rest of the result pages, if there are any, and add each page's data
    # onto the end of our results DataFrame.

    incremented_offset = starting_offset + rows_per_page

    while paged_response_data_object.next_url is not None and len( paged_response_data_object.next_url ) > 0:
        
        log.debug( f"Pulling next paged result from API via next_url value from response: { paged_response_data_object.to_dict()['next_url'] }")

        paged_response_data_object = query_selector[table].sync(
            client=query_api_instance,
            body=query_object,
            offset=incremented_offset,
            limit=rows_per_page
        )

        # Forward error types known to be returned by the API.
        if isinstance( paged_response_data_object, ClientError ) or isinstance( paged_response_data_object, InternalError ):
            log.error( f"{paged_response_data_object.error_type}: {paged_response_data_object.message}" )

        next_result_batch = pd.json_normalize( paged_response_data_object.to_dict()['result'] )

        if not result_dataframe.empty and not next_result_batch.empty:
            
            # Silence a future deprecation warning about pd.concat and empty DataFrame columns.
            # 
            # Possiby relevant note: never fill in missing numeric values with 0!
            next_result_batch = next_result_batch.astype( result_dataframe.dtypes )
            result_dataframe = pd.concat( [result_dataframe, next_result_batch] )

        incremented_offset = incremented_offset + rows_per_page

    #############################################################################################################################
    # Postprocess API result data.

    log.debug( 'Organizing result data...' )

    # Collect data source information and populate our user-facing `data_source` result column summary,
    # unless its been repressed via exclude_columns=['data_source'].

    if not suppress_data_source_results:
        
        # Make a new column called 'data_source', populated with empty lists.
        result_dataframe['data_source'] = [ [] for _ in range( len( result_dataframe ) ) ]

        for row_index, result_record in result_dataframe.iterrows():
            for upstream_data_source in valid_data_sources:
                if result_record[ f"{table}_data_at_{upstream_data_source.lower()}" ] == True:
                    result_dataframe['data_source'].iloc[row_index].append( upstream_data_source )

    # Ensure the contents and ordering of the set of default columns for this endpoint
    # is the same whether or not additional column data (from other tables, or provenance
    # metadata for `table` rows) has been requested. Also make sure non-user-facing columns
    # (e.g. `subject_data_at_gdc`) are not passed through to the user unprocessed.

    added_columns = list()
    columns_to_suppress = list()

    for column in result_dataframe:
        
        if column != 'data_source':
            
            if re.search( r'^[^_]+_data_at_[^_]+$', column ) is not None:
                columns_to_suppress.append( column )

            elif column not in source_table_columns_in_order:
                added_columns.append( column )

    if len( columns_to_suppress ) > 0:
        log.debug( f"   -- filtering API columns: {columns_to_suppress}" )
        result_dataframe = result_dataframe.drop( columns=columns_to_suppress )

    # Resequence the output columns according to the sequence given by the columns() function.
    final_column_order = list()

    # First, order all the native fields from this endpoint that weren't explicitly excluded by the user, in the default (relative) order.
    for column in source_table_columns_in_order:
        if column in result_dataframe:
            final_column_order.append( column )

    # Then our `data_source` result summary, if it wasn't suppressed.
    if not suppress_data_source_results:
        final_column_order.append( 'data_source' )

    # Then the fields from other tables that the user added.
    for added_column in added_columns:
        final_column_order.append( added_column )

    if len( result_dataframe.columns ) > 0:
        
        result_dataframe = result_dataframe[ final_column_order ]

        log.debug( 'Handling missing values...' )

        result_column_names = result_dataframe.columns.to_list()

        for column in result_column_names:
            
            if column != 'data_source':
                
                # CDA has no float values. Cast all numeric data to integers.
                # print('name: ' + column + ' ' + str(type(result_dataframe[column])) + ' datatypes=' + str(column_data_types[column]))

                if column_data_types[column] in { 'integer', 'bigint' }:
                    
                    # Columns of type `float64` can contain NaN (missing) values, which cannot (for some reason)
                    # be stored in Pandas Series objects (i.e., DataFrame columns) of type `int` or `int64`.
                    # Pandas workaround: use extension type 'Int64' (note initial capital) -- itself an alias for numpy.int64 --
                    # which supports the storage of missing values. These will print as '<NA>'.

                    if result_dataframe[column].dtype == 'float64':
                        
                        result_dataframe[column] = pd.to_numeric( result_dataframe[column] ).round().astype( 'Int64' )

                    else:
                        
                        result_dataframe[column] = result_dataframe[column].apply( lambda cell_val: [ numpy.int64( round( element_val ) ) if element_val is not None else '<NA>' for element_val in cell_val ] if isinstance( cell_val, list ) else numpy.int64( round( cell_val ) ) if cell_val is not None else '<NA>' )

                elif column_data_types[column] in { 'text', 'boolean' }:
                    
                    # Replace values that are None (== null) with '<NA>' (to match what we['re forced to] use
                    # for null numeric values.

                    result_dataframe[column] = result_dataframe[column].fillna( '<NA>' )

                else:
                    
                    # This isn't anticipated. Yell if we get something unexpected.
                    log.critical( f"Unexpected data type `{column_data_types[column]}` received; aborting. Please report this event to the CDA development team." )
                    return

    if return_data_as == '' or return_data_as == 'dataframe':
        
        # Right now, the default is the same as if the user had specified return_data_as='dataframe'.
        return result_dataframe

    elif return_data_as == 'tsv':
        
        log.debug( f"Printing results to TSV file '{output_file}'" )

        # Write results to a user-specified TSV.

        try:
            result_dataframe.to_csv( output_file, sep='\t', index=False )
            return

        except Exception as error:
            log.critical( f"Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'." )
            return

    log.critical( 'Something has gone unexpectedly and disastrously wrong with result-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error.' )
    return

#############################################################################################################################
#
# END get_data
#
#############################################################################################################################


