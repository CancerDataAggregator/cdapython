import json
import numpy
import pandas as pd
import re

import cda_client

from cdapython.application_utilities import build_match_from_file_filter, get_api_url
from cdapython.discover import columns, release_metadata
from cdapython.logging_wrappers import get_logger
from cdapython.validation import normalize_to_list, validate_and_transform_match_filter_list, validate_parameter_values

from cda_client.api.data import file_fetch_rows_endpoint_data_file_post as file_data_endpoint
from cda_client.api.data import subject_fetch_rows_endpoint_data_subject_post as subject_data_endpoint
from cda_client.models.client_error import ClientError
from cda_client.models.internal_error import InternalError
from cda_client.models.data_request_body import DataRequestBody


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
    match_all=None,
    match_any=None,
    match_from_file={ 'input_file': '', 'input_column': '', 'cda_column_to_match': '' },
    data_source=None,
    add_columns=None,
    exclude_columns=None,
    expand_results=False,
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

        expand_results ( boolean; optional ):
            If True: for each result file, include a DataFrame collating
            results linked to that file from each non-file table that was
            queried. Otherwise, for each result file, include a list of
            unique values associated with that file from each non-file
            column that was queried.

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

    return get_data( table='file', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, provenance=False, expand_results=expand_results, return_data_as=return_data_as, output_file=output_file )

#############################################################################################################################
#
# get_subject_data( ): Get CDA subject data rows ('result rows') that match user-specified criteria.
#
#############################################################################################################################

def get_subject_data(
    *,
    match_all=None,
    match_any=None,
    match_from_file={ 'input_file': '', 'input_column': '', 'cda_column_to_match': '' },
    data_source=None,
    add_columns=None,
    exclude_columns=None,
    provenance=False,
    expand_results=False,
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

        expand_results ( boolean; optional ):
            If True: for each result subject, include a DataFrame collating
            results linked to that subject from each non-subject table that was
            queried. Otherwise, for each result subject, include a list of
            unique values associated with that subject from each non-subject
            column that was queried.

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

    return get_data( table='subject', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, provenance=provenance, expand_results=expand_results, return_data_as=return_data_as, output_file=output_file )

#############################################################################################################################
#
# get_data( table=`table` ): Get CDA data rows ('result rows') from `table` that match user-specified criteria.
#
#############################################################################################################################

def get_data(
    table=None,
    *,
    match_all=None,
    match_any=None,
    match_from_file={ 'input_file': '', 'input_column': '', 'cda_column_to_match': '' },
    data_source=None,
    add_columns=None,
    exclude_columns=None,
    provenance=False,
    expand_results=False,
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

        expand_results ( boolean; optional ):
            If True: for each result row, include a DataFrame collating
            results linked to that row from each foreign table that was
            queried. Otherwise, for each result row, include a list of
            unique values associated with that row from each foreign
            column that was queried.

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

    cached_release_metadata = release_metadata()

    valid_data_sources = set()

    for column_record in cached_release_metadata:
        record_data_source = column_record['data_source']
        if record_data_source != 'CDA':
            # Let's not care about case.
            valid_data_sources.add( record_data_source.upper() )

    # Validate user-supplied parameter data.

    try:
        validate_parameter_values(
            called_function='get_data',
            cached_column_metadata=cached_column_metadata,
            valid_data_sources=valid_data_sources,
            table=table,
            match_from_file=match_from_file,
            data_source=data_source,
            add_columns=add_columns,
            exclude_columns=exclude_columns,
            provenance=provenance,
            expand_results=expand_results,
            return_data_as=return_data_as,
            output_file=output_file,
            log=log
        )
    except Exception as e:
        log.error( e )
        return

    #############################################################################################################################
    # Preprocess CDA table metadata, to enable consistent processing (and reporting) throughout.
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
    # Validate and normalize match_all filter strings; save results as a list of statement strings.

    try:
        queries_for_match_all = validate_and_transform_match_filter_list( cached_column_metadata, match_all )
    except Exception as e:
        log.error( e )
        return

    # Update `queries_for_match_all` to restrict results to optionally-specified `data_source` values.

    for upstream_data_source in data_source:
        queries_for_match_all.append( f"{table}_data_at_{upstream_data_source.lower()} = true" )

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned rows must satisfy at least one (OR; union; 'at least one of these must apply').
    # 
    # Validate and normalize match_any filter strings; save results as a list of statement strings.

    try:
        queries_for_match_any = validate_and_transform_match_filter_list( cached_column_metadata, match_any )
    except Exception as e:
        log.error( e )
        return

    #############################################################################################################################
    # If not null, process match_from_file query information: load target values to match and check to see if records with
    # missing values in the target column should be included.

    if match_from_file['input_file'] != '':
        
        match_from_file_target_values = set()

        # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
        # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
        # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
        # anger and confusion when results don't match the input set along the given column.

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

        #### TO DO: BEGIN: Move following chunk to validation.py:

        # Parse `match_from_file` filter values: complain if
        #
        #     * filter values don't match the data types of the columns they're paired with
        #     * wildcards appear anywhere (they're not compatible with the IN keyword, and we don't currently support the construction of per-value LIKE filters)
        #
        # ...and save parse results as a combined filter expression in a Query object (to be combined with others later).

        # Identify the data type of the target CDA column.

        target_data_type = column_data_types[ match_from_file['cda_column_to_match'] ]

        processed_target_values = set()

        boolean_alias = {
            'true': 'true',
            't': 'true',
            'false': 'false',
            'f': 'false'
        }

        for target_value in match_from_file_target_values:
            
            # Validate value types and test for wildcards.

            if target_data_type == 'boolean':
                
                # If we're supposed to be in a boolean column, make sure we've got a true/false value.
                if target_value.lower() not in boolean_alias:
                    log.error( f"match_from_file: requested column {match_from_file['cda_column_to_match']} has data type 'boolean', requiring a true/false value; you specified '{target_value}', which is neither." )
                    return

                else:
                    target_value = boolean_alias[target_value]

            elif target_data_type in ['bigint', 'integer', 'numeric']:
                
                # If we're supposed to be in a numeric column, make sure we've got a number.
                if re.search( r'^[-+]?\d+(\.\d+)?$', target_value ) is None:
                    log.error( f"match_from_file: requested column {match_from_file['cda_column_to_match']} has data type '{target_data_type}', requiring a number value; you specified '{target_value}', which is not." )
                    return

            elif target_data_type == 'text':
                
                # Check for wildcards: if found, vomit.
                if re.search(r'\*', target_value) is not None:
                    log.error( f"match_from_file: wildcards (*) are disallowed here (only exact matches are supported for this option); value '{target_value}' is noncompliant. Please fix." )
                    return

            else:
                
                # Just to be safe. Types change.
                log.critical( f"match_from_file: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers." )
                return

            processed_target_values.add( target_value )

        #### TO DO: END:: Move preceding chunk to validation.py

        # Parse and normalize `match_from_file` filter data.

        match_from_file_filter_strings = set()

        if match_from_file_nulls_allowed:
            
            match_from_file_filter_strings.add( f"{match_from_file['cda_column_to_match']} is null" )

        if target_data_type == 'text' and len( processed_target_values ) > 0:
            
            match_from_file_filter_strings.add( f"{match_from_file['cda_column_to_match']} in [ '" + "', '".join( processed_target_values ) + "' ]" )

        # Add results to the queries_for_match_any Query object.

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
    # Process the `provenance` flag: if True, ask the API to include data from the `upstream_identifiers` table.

    provenance_columns = [
        'upstream_identifiers_data_source',
        'data_source_id_field_name',
        'data_source_id_value'
    ]

    user_facing_provenance_column = {
        'upstream_identifiers_data_source': 'data_source',
        'data_source_id_field_name': 'id_name',
        'data_source_id_value': 'id_value'
    }

    if provenance == True:
        
        # The user is prevented by the validation logic from asking for these directly because they're not exposed by columns(),
        # but the API will process them if asked to do so.

        for provenance_column in provenance_columns:
            
            columns_to_add.append( provenance_column )

    #############################################################################################################################
    # Build an object to represent our upcoming API query.

    query_object = DataRequestBody()
    query_object.match_all = queries_for_match_all
    query_object.match_some = queries_for_match_any
    query_object.add_columns = columns_to_add
    query_object.exclude_columns = columns_to_exclude

    # Two ways to activate this:
    #     1. The user asked for it via the `expand_results` parameter, and
    #     2. the user set `provenance` to True, in which case we need results grouped by row so we can properly combine
    #        upstream identifier records.

    if expand_results == True or provenance == True:
        
        query_object.expand_results = True

    else:
        
        query_object.expand_results = False

    #############################################################################################################################
    # Fetch data from the API.
    #############################################################################################################################

    # Support selection of the appropriate endpoint based on the value of `table`.

    query_selector = {
        'file': file_data_endpoint,
        'subject': subject_data_endpoint
    }

    # We return all results to users at once. Paging can occur internally, but is made
    # transparent to the user. Track offset and page size in case we have to handle paged
    # results.

    starting_offset = 0
    rows_per_page = 500000

    # Try to get data from the REST API.

    log.debug( f"Sending query to API '/data/{table}' endpoint:\n{json.dumps( query_object.to_dict(), indent=4 )}\n" )
    
    query_api_instance = cda_client.Client( base_url=get_api_url() )

    api_response_object = query_selector[table].sync(
        client=query_api_instance,
        body=query_object,
        limit=rows_per_page,
        offset=starting_offset
    )

    # Forward error types known to be returned by the API.
    if isinstance( api_response_object, ClientError ) or isinstance( api_response_object, InternalError ):
        log.error( f"{api_response_object.error_type}: {api_response_object.message}" )
        return

    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function. Example JSON responses ( note that
    # not all of these columns are returned by default: some were requested; others
    # induced by a non-null `data_source` parameter; still others included in response
    # to the user setting the `provenance` parameter to True; note also that this is
    # a cut/paste job from several responses, don't check it too hard for internal
    # consistency -- it's just meant to let readers know what to expect in terms of
    # field names and nesting structures):
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
    #             "subject_data_source_count": 3,
    #             "sex": [
    #                 "female"
    #             ],
    #             "upstream_identifiers_columns": [
    #                 {
    #                     "upstream_identifiers_data_source": "CDS",
    #                     "data_source_id_field_name": "participant.participant_id",
    #                     "data_source_id_value": "C3L-00447"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "CDS",
    #                     "data_source_id_field_name": "participant.uuid",
    #                     "data_source_id_value": "c238af7c-b7c2-52b8-83a4-fd66939db40b"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "GDC",
    #                     "data_source_id_field_name": "case.case_id",
    #                     "data_source_id_value": "8220be9e-ca4d-4a48-b48a-06c0b223700e"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "GDC",
    #                     "data_source_id_field_name": "case.submitter_id",
    #                     "data_source_id_value": "C3L-00447"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "IDC",
    #                     "data_source_id_field_name": "auxiliary_metadata.submitter_case_id",
    #                     "data_source_id_value": "C3L-00447"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "IDC",
    #                     "data_source_id_field_name": "dicom_all.PatientID",
    #                     "data_source_id_value": "C3L-00447"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "IDC",
    #                     "data_source_id_field_name": "dicom_all.idc_case_id",
    #                     "data_source_id_value": "23035925-a4b7-4093-887c-bfdeb6df251e"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "PDC",
    #                     "data_source_id_field_name": "Case.case_id",
    #                     "data_source_id_value": "c5f8631d-1fb8-11e9-b7f8-0a80fada099c"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "PDC",
    #                     "data_source_id_field_name": "Case.case_submitter_id",
    #                     "data_source_id_value": "C3L-00447"
    #                 },
    #                 {
    #                     "upstream_identifiers_data_source": "CDS",
    #                     "data_source_id_field_name": "participant.dbGaP_subject_id",
    #                     "data_source_id_value": "2125680"
    #                 }
    #             ]
    #         },
    #         
    #         ...
    #         
    #     ],
    #     "query_sql": "WITH subject_preselect AS (SELECT subject.id_alias AS id_alias FROM subject WHERE (EXISTS (SELECT 1 FROM observation WHERE subject.id_alias = observation.subject_alias AND coalesce(upper(observation.sex), :coalesce_2) = upper(:upper_1))) AND subject.year_of_birth < :year_of_birth_1 AND subject.data_at_gdc = true), observation_subject_columns AS (SELECT array_remove(array_agg(DISTINCT observation.sex), NULL) AS sex, observation.subject_alias AS subject_alias FROM observation WHERE observation.subject_alias IN (SELECT subject_preselect.id_alias FROM subject_preselect) GROUP BY observation.subject_alias) SELECT row_to_json(json_result) AS row_to_json_1 FROM (SELECT subject.id AS subject_id, subject.crdc_id AS subject_crdc_id, subject.species AS species, subject.year_of_birth AS year_of_birth, subject.year_of_death AS year_of_death, subject.cause_of_death AS cause_of_death, subject.race AS race, subject.ethnicity AS ethnicity, subject.year_of_birth AS year_of_birth, subject.data_at_gdc AS subject_data_at_gdc, subject.data_at_idc AS subject_data_at_idc, subject.data_at_cds AS subject_data_at_cds, subject.data_at_pdc AS subject_data_at_pdc, subject.data_at_gdc AS subject_data_at_gdc, subject.data_at_icdc AS subject_data_at_icdc, coalesce(observation_subject_columns.sex, :coalesce_1) AS sex FROM subject LEFT OUTER JOIN observation_subject_columns ON observation_subject_columns.subject_alias = subject.id_alias WHERE subject.id_alias IN (SELECT subject_preselect.id_alias FROM subject_preselect)) AS json_result",
    #     "total_row_count": 9,
    #     "next_url": ""
    # }

    # Report some metadata about the results we got back.

    log.debug( f"/data/{table} endpoint query SQL:\n{api_response_object.to_dict()['query_sql']}" )

    # This is stupidly verbose.

    # log.debug( f"Page one results:\n{json.dumps( api_response_object.to_dict()['result'], indent=4 )}\n" )
    
    # Convert response JSON into a DataFrame using pandas' json_normalize() function.

    result_dataframe = pd.json_normalize( api_response_object.to_dict()['result'] )

    # The data we've fetched so far might be just the first page (if the total number
    # of results is greater than `rows_per_page`).
    #
    # Get the rest of the result pages, if there are any, and add each page's data
    # onto the end of our results DataFrame.

    incremented_offset = starting_offset + rows_per_page

    while api_response_object.next_url is not None and len( api_response_object.next_url ) > 0:
        
        log.debug( f"Pulling next paged result from API with an offset of {incremented_offset} and a max page size of {rows_per_page}")

        api_response_object = query_selector[table].sync(
            client=query_api_instance,
            body=query_object,
            offset=incremented_offset,
            limit=rows_per_page
        )

        # Forward error types known to be returned by the API.
        if isinstance( api_response_object, ClientError ) or isinstance( api_response_object, InternalError ):
            log.error( f"{api_response_object.error_type}: {api_response_object.message}" )

        # Convert response JSON into a DataFrame using pandas' json_normalize() function.

        next_result_batch = pd.json_normalize( api_response_object.to_dict()['result'] )

        # Add data from this page to our full result set.

        if not result_dataframe.empty and not next_result_batch.empty:
            
            # Silence a future deprecation warning about pd.concat and empty DataFrame columns.
            # 
            # Possiby relevant note: never fill in missing numeric values with 0!

            next_result_batch = next_result_batch.astype( result_dataframe.dtypes )
            result_dataframe = pd.concat( [result_dataframe, next_result_batch] )

        incremented_offset = incremented_offset + rows_per_page

    #############################################################################################################################
    # Postprocess API result data.
    #############################################################################################################################

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

    # Collate upstream provenance metadata, if requested.

    if provenance == True:
        
        provenance_df_list = list()

        for row_index, result_record in result_dataframe.iterrows():
            
            provenance_data_by_column = dict()

            for identifier_record in result_record[ 'upstream_identifiers_columns' ]:
                for provenance_column in provenance_columns:
                    if provenance_column not in provenance_data_by_column:
                        provenance_data_by_column[provenance_column] = list()
                    provenance_data_by_column[provenance_column].append( identifier_record[provenance_column] )

            provenance_df_list.append( pd.DataFrame.from_dict( { user_facing_provenance_column[provenance_column] : provenance_data_by_column[provenance_column] for provenance_column in provenance_columns }, orient='columns' ) )

        # Make a new column called 'provenance', populated with DataFrames.
        result_dataframe['provenance'] = provenance_df_list

    # Ensure the contents and ordering of the set of default columns for this endpoint
    # is the same whether or not additional column data (from other tables, or provenance
    # metadata for `table` rows) has been requested. Also make sure non-user-facing columns
    # (e.g. `subject_data_at_gdc`) are not passed through to the user unprocessed.

    columns_to_suppress = list()
    added_columns = list()

    virtual_columns_to_add = dict()
    file_data_columns_to_add = dict()

    # Remove raw versions of virtual list data attached to the file table
    # and replace them with DataFrames or column-wise lists of unique values,
    # depending on whether or not `expand_results` is set to True.

    for column in { 'file_anatomic_site_columns', 'file_tumor_vs_normal_columns' }:
        
        if column in result_dataframe:
            
            columns_to_suppress.append( column )

            output_column_name = re.search( r'^file_(.*)_columns$', column ).group(1)
            
            if table == 'file':

                # If we're getting file data, we always want these transparently included as virtual file columns containing list values.
                
                virtual_column_list = list()

                for row_index, result_record in result_dataframe.iterrows():
                    
                    if result_record[column] is not None:
                        
                        observed_value_set = set()

                        for value_record in result_record[column]:
                            
                            observed_value_set.add( value_record[output_column_name] )

                        virtual_column_list.append( sorted( observed_value_set ) )

                    else:
                        
                        virtual_column_list.append( '<NA>' )

                virtual_columns_to_add[output_column_name] = virtual_column_list

            elif table == 'subject':
                
                # If we're getting subject data, then depending on the value of the `expand_results` parameter, we either
                # want this information incorporated (as list values) into `result_dataframe['file_data']`, a column of
                # DataFrames column containing tuples of linked file metadata, or instead rendered individually
                # as a foreign-result column containing lists of unique values assigned to all matching files associated with
                # each `result_dataframe` row's subject record.

                if expand_results == True:
                    
                    add_to_file_data_dataframes = list()

                    for row_index, result_record in result_dataframe.iterrows():
                        
                        if result_record[column] is not None:
                            
                            observed_value_set = set()

                            for value_record in result_record[column]:
                                observed_value_set.add( value_record[output_column_name] )

                            add_to_file_data_dataframes.append( sorted( observed_value_set ) )

                        else:
                            add_to_file_data_dataframes.append( '<NA>' )

                    file_data_columns_to_add[output_column_name] = add_to_file_data_dataframes

                else:
                    
                    virtual_column_list = list()

                    for row_index, result_record in result_dataframe.iterrows():
                        
                        if result_record[column] is not None:
                            
                            observed_value_set = set()

                            for value_record in result_record[column]:
                                observed_value_set.add( value_record[output_column_name] )

                            virtual_column_list.append( sorted( observed_value_set ) )

                        else:
                            virtual_column_list.append( '<NA>' )

                    virtual_columns_to_add[output_column_name] = virtual_column_list
                    added_columns.append( output_column_name )

    df_columns_to_add = dict()
    single_foreign_columns_to_add = dict()

    for column in result_dataframe:
        
        if column not in { 'data_source', 'provenance', 'file_anatomic_site_columns', 'file_tumor_vs_normal_columns', 'upstream_identifiers_columns' }:
            
            if re.search( r'^[^_]+_data_at_[^_]+$', column ) is not None or re.search( r'^[^_]+_data_source_count$', column ) is not None:
                columns_to_suppress.append( column )

            # Remove raw versions of aggregated result sets from foreign tables
            # and replace them with DataFrames or column-wise lists of unique values,
            # depending on whether or not `expand_results` is set to True.

            elif re.search( r'_columns$', column ) is not None:
                
                columns_to_suppress.append( column )

                foreign_table_name = re.search( r'^(.*)_columns$', column ).group(1)

                if expand_results == True:
                    
                    # Our result DataFrame's cells in a column named for `foreign_table_name` will
                    # contain DataFrames with linked values, row-wise, from `foreign_table_name`, describing
                    # all data from that table associated with with each top-level row's main entity record.

                    foreign_df_list = list()

                    for row_index, result_record in result_dataframe.iterrows():
                        
                        foreign_table_data_by_column = dict()

                        if result_record[column] is not None:
                            
                            for foreign_table_record in result_record[column]:
                                
                                for foreign_table_column in foreign_table_record:
                                    
                                    if foreign_table_column not in foreign_table_data_by_column:
                                        foreign_table_data_by_column[foreign_table_column] = list()

                                    # Encode nulls as '<NA>'.
                                    # (float) NaN != NaN
                                    # Testing values for None will miss NaN values, so we use the above truth to test for those too.

                                    if foreign_table_record[foreign_table_column] is None or foreign_table_record[foreign_table_column] != foreign_table_record[foreign_table_column]:
                                        
                                        foreign_table_data_by_column[foreign_table_column].append( '<NA>' )

                                    else:
                                        
                                        foreign_table_data_by_column[foreign_table_column].append( foreign_table_record[foreign_table_column] )

                                # Stitch in virtual file columns, processed in the previous block.

                                if foreign_table_name == 'file' and len( file_data_columns_to_add ) > 0:
                                    
                                    for virtual_file_column_name in file_data_columns_to_add:
                                        
                                        if virtual_file_column_name not in foreign_table_data_by_column:
                                            foreign_table_data_by_column[virtual_file_column_name] = list()

                                        foreign_table_data_by_column[virtual_file_column_name].append( file_data_columns_to_add[virtual_file_column_name][row_index] )

                        if len( foreign_table_data_by_column ) > 0:
                            
                            foreign_df_list.append( pd.DataFrame.from_dict( { foreign_table_column : foreign_table_data_by_column[foreign_table_column] for foreign_table_column in foreign_table_data_by_column }, orient='columns' ) )

                        else:
                            
                            foreign_df_list.append( None )

                    # Make a new column called '`foreign_table_name`_data', populated with DataFrames.
                    df_columns_to_add[f"{foreign_table_name}_data"] = foreign_df_list

                else:
                    
                    # `expand_results` == False : include results from foreign columns in `result_dataframe` one at a time, as sets of unique values.

                    foreign_column_lists = dict()

                    for row_index, result_record in result_dataframe.iterrows():
                        
                        if result_record[column] is not None:
                            
                            observed_value_sets = dict()

                            for foreign_table_record in result_record[column]:
                                
                                for foreign_table_column in foreign_table_record:
                                    
                                    if foreign_table_column not in observed_value_sets:
                                        observed_value_sets[foreign_table_column] = set()

                                    if foreign_table_column not in foreign_column_lists:
                                        foreign_column_lists[foreign_table_column] = list()

                                    # Ignore null values; if no non-null values are observed, we'll return <NA> instead of a list.
                                    # (float) NaN != NaN
                                    # Testing values for None will miss NaN values, so we use the above truth to test for those too.

                                    if foreign_table_record[foreign_table_column] is not None and foreign_table_record[foreign_table_column] == foreign_table_record[foreign_table_column]:
                                        
                                        observed_value_sets[foreign_table_column].add( foreign_table_record[foreign_table_column] )

                            for foreign_table_column in observed_value_sets:
                                
                                if len( observed_value_sets[foreign_table_column] ) > 0:
                                    
                                    foreign_column_lists[foreign_table_column].append( sorted( observed_value_sets[foreign_table_column] ) )

                                else:
                                    
                                    foreign_column_lists[foreign_table_column].append( '<NA>' )

                    for foreign_table_column in foreign_column_lists:
                        
                        single_foreign_columns_to_add[foreign_table_column] = foreign_column_lists[foreign_table_column]
                        added_columns.append( foreign_table_column )

            elif column not in source_table_columns_in_order:
                added_columns.append( column )

    for column in virtual_columns_to_add:
        result_dataframe[column] = virtual_columns_to_add[column]

    for column in df_columns_to_add:
        result_dataframe[column] = df_columns_to_add[column]

    for column in single_foreign_columns_columns_to_add:
        result_dataframe[column] = single_foreign_columns_columns_to_add[column]

    if len( columns_to_suppress ) > 0:
        log.debug( f"Filtering API columns: {columns_to_suppress}" )
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

    # Then our provenance metadata, if it was requested.
    if provenance == True:
        final_column_order.append( 'provenance' )

    # Then the fields from other tables that the user added.
    for added_column in added_columns:
        final_column_order.append( added_column )

    for added_column in df_columns_to_add:
        final_column_order.append( added_column )

    if len( result_dataframe.columns ) > 0:
        
        result_dataframe = result_dataframe[ final_column_order ]

        log.debug( 'Handling missing values...' )

        result_column_names = result_dataframe.columns.to_list()

        for column in result_column_names:
            
            if column != 'data_source' and column != 'provenance' and column not in df_columns_to_add:
                
                # CDA has no float values. Cast all numeric data to integers.

                if column_data_types[column] in { 'integer', 'bigint' }:
                    
                    # Columns of type `float64` can contain NaN (missing) values, which cannot (for some reason)
                    # be stored in Pandas Series objects (i.e., DataFrame columns) of type `int` or `int64`.
                    # Pandas workaround: use extension type 'Int64' (note initial capital) -- itself an alias for numpy.int64 --
                    # which supports the storage of missing values. These will print as '<NA>'.

                    if result_dataframe[column].dtype == 'float64':
                        
                        result_dataframe[column] = pd.to_numeric( result_dataframe[column] ).round().astype( 'Int64' )

                    # (float) NaN != NaN
                    # Testing cell values for None will miss NaN values, which will then generate an error if uncaught before trying to round them.

                    result_dataframe[column] = result_dataframe[column].apply( lambda cell_val: [ numpy.int64( round( element_val ) ) if ( element_val is not None and element_val == element_val ) else '<NA>' for element_val in cell_val ] if isinstance( cell_val, list ) else numpy.int64( round( cell_val ) ) if ( cell_val is not None and cell_val == cell_val ) else '<NA>' )

                elif column_data_types[column] in { 'text', 'boolean' }:
                    
                    # Replace values that are None (== null) with '<NA>' (to match what we['re forced to] use
                    # for null numeric values.

                    result_dataframe[column] = result_dataframe[column].fillna( '<NA>' )

                else:
                    
                    # This isn't anticipated. Yell if we get something unexpected.
                    log.critical( f"Unexpected data type `{column_data_types[column]}` received; aborting. Please report this event to the CDA development team." )
                    return

    #############################################################################################################################
    # Return our response to the user.
    #############################################################################################################################

    if return_data_as == '' or return_data_as == 'dataframe':
        
        # Right now, the default is the same as if the user had specified return_data_as='dataframe'.
        return result_dataframe

    elif return_data_as == 'tsv':
        
        log.debug( f"Printing results to TSV file '{output_file}'" )

        # Write results to a user-specified TSV.

        try:
            
            # We can't use DataFrame.to_csv() because it doesn't handle nested DataFrames the way we want.

            with open( output_file, 'w' ) as OUT:
                
                print( *result_dataframe.columns.to_list(), sep='\t', file=OUT )

                for row_index, result_record in result_dataframe.iterrows():
                    
                    row_data = list()

                    for column in result_dataframe.columns.to_list():
                        
                        if isinstance( result_record[column], pd.DataFrame ):
                            
                            dict_with_na_nulls = result_record[column].to_dict( orient='records' )

                            dict_with_empty_string_nulls = dict()

                            # This assumes 2D DataFrames, which is safe at time of writing (2025-05-07).

                            for key in dict_with_na_nulls:
                                
                                if dict_with_na_nulls[key] == '<NA>':
                                    
                                    dict_with_empty_string_nulls[key] = ''

                                else:
                                    
                                    dict_with_empty_string_nulls[key] = dict_with_na_nulls[key]

                            row_data.append( dict_with_empty_string_nulls[key] )

                        elif result_record[column] is None or ( isinstance( result_record[column], str ) and result_record[column] == '<NA>' ):
                            
                            row_data.append( '' )

                        else:
                            
                            row_data.append( result_record[column] )

                    print( *row_data, sep='\t', file=OUT )

            return

        except Exception as error:
            log.error( f"Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'." )
            return

    log.critical( 'Something has gone unexpectedly and disastrously wrong with result-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error.' )
    return

#############################################################################################################################
#
# END get_data
#
#############################################################################################################################


