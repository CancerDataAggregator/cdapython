import json
import numpy
import pandas as pd
import re

import cda_client

from cdapython.application_utilities import get_api_url
from cdapython.discover import columns, release_metadata
from cdapython.logging_wrappers import get_logger
from cdapython.validation import normalize_to_list, validate_and_transform_match_filter_list, validate_and_transform_match_from_file_values, validate_parameter_values

from cda_client.api.data import file_fetch_rows_endpoint_data_file_post as file_data_endpoint
from cda_client.api.data import subject_fetch_rows_endpoint_data_subject_post as subject_data_endpoint
from cda_client.errors import UnexpectedStatus
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
    collate_results=False,
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

        collate_results ( boolean; optional ):
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

    return get_data( table='file', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, collate_results=collate_results, include_external_refs=False, return_data_as=return_data_as, output_file=output_file )

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
    collate_results=False,
    include_external_refs=False,
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

        collate_results ( boolean; optional ):
            If True: for each result subject, include a DataFrame collating
            results linked to that subject from each non-subject table that was
            queried. Otherwise, for each result subject, include a list of
            unique values associated with that subject from each non-subject
            column that was queried. Defaults to False.

        include_external_refs ( boolean; optional ):
            If True: for each result subject, include a DataFrame called
            'external_reference_data' that collates references to external
            resources containing data describing that subject. Defaults to False.

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

    return get_data( table='subject', match_all=match_all, match_any=match_any, match_from_file=match_from_file, data_source=data_source, add_columns=add_columns, exclude_columns=exclude_columns, collate_results=collate_results, include_external_refs=include_external_refs, return_data_as=return_data_as, output_file=output_file )

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
    collate_results=False,
    include_external_refs=False,
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

        collate_results ( boolean; optional ):
            If True: for each result row, include a DataFrame collating
            results linked to that row from each foreign table that was
            queried. Otherwise, for each result row, include a list of
            unique values associated with that row from each foreign
            column that was queried.

        include_external_refs ( boolean; optional ):
            If True: for each result row, include a DataFrame called
            'external_reference_data' that collates references to external
            resources containing data directly associated with that
            row. Defaults to False.

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
    # and leave the rest unmodified. Also convert everything to lowercase.

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
        # Let's not care about case.
        if record_data_source.upper() != 'CDA':
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
            collate_results=collate_results,
            include_external_refs=include_external_refs,
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
        queries_for_match_all = validate_and_transform_match_filter_list( cached_column_metadata, match_all, enforce_column_uniqueness=True )
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
            # If collate == True, we'll need to ask for provenance information from this column's
            # home table, if we haven't yet done so. This info is returned by default only for `file` and `subject`.
            if collate_results:
                # Identify this column's home table.
                foreign_table_name = ''
                if re.search( r'\.\*$', column_to_add ) is not None:
                    foreign_table_name = re.sub( r'\.\*$', r'', column_to_add )
                else:
                    foreign_table_name = cached_column_metadata.query( f"column == '{column_to_add}'" )['table'].iloc[0]
                # If we're asking for upstream_identifiers info, then skip this bit -- it has its own closer-bound data source info.
                if foreign_table_name != 'upstream_identifiers':
                    # Check to see if we're already asking for provenance info from that table: if not, do, unless we're asking for 'upstream_identifiers.*'.
                    for data_source in [ source_label.lower() for source_label in sorted( valid_data_sources ) ]:
                        provenance_field = f"{foreign_table_name}_data_at_{data_source}"
                        if provenance_field not in columns_to_add:
                            columns_to_add.append( provenance_field )

            columns_to_add.append( column_to_add )

    if collate_results:
        # Be sure to include requests for provenance information about foreign tables
        # included only implicitly (by filters, not by add_columns). Such info is
        # only included by default from `file` and `subject`.

        for filter_query in set( queries_for_match_all ) | set( queries_for_match_any ):
            # TO DO: Compartmentalize/parametrize this conditional check better: it repeats much that is already made explicit in validation.py.
            filter_is_ternary = False
            column_to_add = ''
            match_result = re.search( r'^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S.*)$', filter_query )
            if match_result is not None:
                left_numeric = match_result.group(1)
                left_operator = match_result.group(2)
                column_name = match_result.group(3)
                right_operator = match_result.group(4)
                right_numeric = match_result.group(5)
                if re.search( r'^[-+]?\d+(\.\d+)?$', left_numeric ) is not None and re.search( r'^[-+]?\d+(\.\d+)?$', right_numeric ) is not None and \
                    left_operator in { '<', '>', '<=', '>=' } and right_operator in { '<', '>', '<=', '>=' }:
                    filter_is_ternary = True
                    column_to_add = column_name
            if not filter_is_ternary:
                column_to_add = re.search( r'^(\S+)\s', filter_query ).group(1)
            # The user-exposed `data_source` parameter can add filters on e.g. `subject_data_at_pdc` -- don't bother with these.
            # They won't be in the reference structure we're about to search, and they don't on their own indicate
            # a need for any extra provenance info.
            if re.search( r'_data_at_', column_to_add ) is None:
                foreign_table_name = cached_column_metadata.query( f"column == '{column_to_add}'" )['table'].iloc[0]
                # Check to see if we're already asking for provenance info from that table: if not, do, unless {table} is 'upstream_identifiers'.
                if foreign_table_name != 'upstream_identifiers':
                    for data_source in [ source_label.lower() for source_label in sorted( valid_data_sources ) ]:
                        provenance_field = f"{foreign_table_name}_data_at_{data_source}"
                        if provenance_field not in columns_to_add:
                            columns_to_add.append( provenance_field )

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
            # If we're excluding {home_table}.*, automatically suppress 'data_source' results.
            if column_to_exclude == f"{table}.*":
                suppress_data_source_results = True

    #############################################################################################################################
    # Build an object to represent our upcoming API query.

    query_object = DataRequestBody()
    query_object.match_all = queries_for_match_all
    query_object.match_some = queries_for_match_any
    query_object.add_columns = columns_to_add
    query_object.exclude_columns = columns_to_exclude
    query_object.collate_results = collate_results
    query_object.external_reference = include_external_refs

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
    
    query_api_instance = cda_client.Client( base_url=get_api_url(), raise_on_unexpected_status=True )

    try:
        api_response_object = query_selector[table].sync(
            client=query_api_instance,
            body=query_object,
            limit=rows_per_page,
            offset=starting_offset
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

    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function. Example JSON responses ( note that
    # not all of these columns are returned by default: some were requested; others
    # induced by a non-null `data_source` parameter; note also that this is
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
    #             ]
    #         },
    #         
    #         ...
    #         
    #     ],
    #     "query_sql": "WITH subject_preselect AS ( ... ) AS json_result",
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

        try:
            api_response_object = query_selector[table].sync(
                client=query_api_instance,
                body=query_object,
                offset=incremented_offset,
                limit=rows_per_page
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
    # unless it's been suppressed via exclude_columns=['data_source'].

    if not suppress_data_source_results:
        
        # Make a new column called 'data_source', populated with empty lists.
        result_dataframe['data_source'] = [ [] for _ in range( len( result_dataframe ) ) ]

        for row_index, result_record in result_dataframe.iterrows():
            for upstream_data_source in sorted( valid_data_sources ):
                if result_record[ f"{table}_data_at_{upstream_data_source.lower()}" ] == True:
                    result_dataframe['data_source'].iloc[row_index].append( upstream_data_source )

    # Ensure the contents and ordering of the set of default columns for this endpoint
    # is the same whether or not additional column data (e.g. from other tables) has
    # been requested. Also make sure non-user-facing columns (e.g. `subject_data_at_gdc`)
    # are not passed through to the user unprocessed.

    columns_to_suppress = list()
    added_columns = list()

    df_columns_to_add = dict()

    for column in result_dataframe:
        
        if column not in { 'data_source' }:
            
            # TO DO: This is a terrible way to exclude columns. See similar comment on banned_columns in summarize.py. Also see below in this block for more explicit filters.

            if re.search( r'^[^_]+_data_at_[^_]+$', column ) is not None or re.search( r'^[^_]+_data_source_count$', column ) is not None or re.search( r'_id_alias$', column ) is not None or re.search( r'crdc_id$', column ) is not None:
                columns_to_suppress.append( column )

            # Remove raw versions of aggregated result sets from foreign tables
            # and replace them with DataFrames.

            elif re.search( r'_columns$', column ) is not None:
                
                # Note: collate_results will always be True in this case: there is no other way to get 'X_columns' lists back from the API.

                columns_to_suppress.append( column )

                foreign_table_name = re.search( r'^(.*)_columns$', column ).group(1)

                # Our result DataFrame's cells in a column named for `foreign_table_name` will
                # contain DataFrames with linked values, row-wise, from `foreign_table_name`, describing
                # all data from that table associated with each top-level row's main entity record.

                foreign_df_list = list()

                for row_index, result_record in result_dataframe.iterrows():
                    
                    foreign_table_data_by_column = dict()

                    # Summarize (row-wise) 'data_source' values as we do for top-level result rows, unless we're processing external_reference or upstream_identifiers, which encode this data differently or not at all.

                    foreign_table_data_by_column['data_source'] = list()

                    if result_record[column] is not None:
                        
                        for foreign_table_record in result_record[column]:
                            
                            upstream_data_source = ''

                            if foreign_table_name in [ 'project', 'subject' ]:
                                # project and subject records can have multiple upstream data sources.
                                upstream_data_source = set()

                            for foreign_table_column in foreign_table_record:
                                
                                match_result = re.search( r'^' + re.escape( foreign_table_name ) + r'_data_at_(.+)$', foreign_table_column )

                                if match_result is not None:
                                    
                                    if foreign_table_record[foreign_table_column] == True:
                                        
                                        detected_data_source = match_result.group(1).upper()

                                        if foreign_table_name in [ 'project', 'subject' ]:
                                            upstream_data_source.add( detected_data_source )

                                        elif upstream_data_source == '':
                                            upstream_data_source = detected_data_source

                                        elif upstream_data_source != detected_data_source:
                                            # There should only ever be one of these for non-subject records.
                                            log.error( f"Upstream data source clash: {detected_data_source} != {upstream_data_source}; {foreign_table_name} (partial) record: \"{foreign_table_record}\"; please notify the CDA devs of this event." )
                                            return

                                # TO DO: This is a terrible way to exclude columns. See similar comment on banned_columns in summarize.py. Also see above and below in this general block for more explicit filters.

                                elif re.search( r'^' + re.escape( foreign_table_name ) + r'_data_source_count$', foreign_table_column ) is None \
                                    and re.search( r'^' + re.escape( foreign_table_name ) + r'_id_alias$', foreign_table_column ) is None \
                                    and re.search( r'crdc_id$', foreign_table_column ) is None:
                                    
                                    if foreign_table_column not in foreign_table_data_by_column:
                                        foreign_table_data_by_column[foreign_table_column] = list()

                                    # Encode nulls as '<NA>'.
                                    # (float) NaN != NaN
                                    # Testing values for None will miss NaN values, so we use the above truth to test for those too.
                                    # Empty list values [] will be passed along unmodified.

                                    if foreign_table_record[foreign_table_column] is None or foreign_table_record[foreign_table_column] != foreign_table_record[foreign_table_column]:
                                        
                                        foreign_table_data_by_column[foreign_table_column].append( '<NA>' )

                                    else:
                                        
                                        foreign_table_data_by_column[foreign_table_column].append( foreign_table_record[foreign_table_column] )

                            if foreign_table_name in ['project', 'subject' ]:
                                upstream_data_source = sorted( upstream_data_source )

                            foreign_table_data_by_column['data_source'].append( upstream_data_source )

                    if len( foreign_table_data_by_column.keys() ) > 1:
                        
                        foreign_table_column_ordering = [ 'data_source' ]

                        # Summarize (row-wise) 'data_source' values as we do for top-level result rows, unless we're processing external_reference or upstream_identifiers, which encode this data differently or not at all.
                        if foreign_table_name in { 'external_reference', 'upstream_identifiers' }:
                            foreign_table_column_ordering = []

                        foreign_table_column_list = [ 'external_reference_type', 'external_reference_name', 'external_reference_short_name', 'last_updated', 'uri', 'external_reference_description', 'source_short_name', 'source_url' ]

                        if foreign_table_name != 'external_reference':
                            foreign_table_column_list = cached_column_metadata.query( f"table == '{foreign_table_name}'" ).column.to_list()
                            
                        for foreign_table_column in foreign_table_column_list:
                            if foreign_table_column in foreign_table_data_by_column:
                                foreign_table_column_ordering.append( foreign_table_column )

                        foreign_df_list.append( pd.DataFrame.from_dict( { re.sub( r'^external_reference_', r'', foreign_table_column ) : foreign_table_data_by_column[foreign_table_column] for foreign_table_column in foreign_table_column_ordering }, orient='columns' ) )

                    else:
                        
                        foreign_df_list.append( pd.DataFrame.from_dict( {} ) )

                # Make a new column called '`foreign_table_name`_data', populated with DataFrames.
                df_columns_to_add[f"{foreign_table_name}_data"] = foreign_df_list

            elif column not in source_table_columns_in_order:
                added_columns.append( column )

    for column in df_columns_to_add:
        result_dataframe[column] = df_columns_to_add[column]

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
            
            if column != 'data_source' and column not in df_columns_to_add and column not in added_columns:
                
                # Home-table columns.

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
                    # for null numeric values). Values that are empty lists [] will be passed along unmodified.

                    result_dataframe[column] = result_dataframe[column].fillna( '<NA>' )

                else:
                    
                    # This isn't anticipated. Yell if we get something unexpected.
                    log.critical( f"Unexpected data type `{column_data_types[column]}` received; aborting. Please report this event to the CDA development team." )
                    return

            elif column in added_columns:
                
                # * this column is from a foreign table: if it were a native column, it would never have been added to `added_columns`
                # 
                # * `collate_results` is False: if it were True, this data would've been kept in the context of its containing
                #   aggregated "X_data" structure and not added to `added_columns`
                # 
                # * THEREFORE, each cell's data is (by design) a (possibly empty) list of unique observed values

                # Handle missing values atom-wise, building a new column as we go, then swap the result into `result_dataframe`.

                processed_column_data = list()

                for row_index, result_record in result_dataframe.iterrows():
                    
                    current_cell_value = result_record[column]

                    if current_cell_value == '<NA>':
                        log.critical( f"Unexpected data modification of value in result column {column} (to '<NA>'). Cannot continue: please notify the CDA developers of this event and include any information needed to replicate this message." )
                        return

                    elif len( current_cell_value ) == 0:
                        # An empty list.
                        processed_column_data.append( list() )

                    else:
                        # We have a nonzero-length list of non-null data values.
                        processed_cell_value = list()

                        for list_element in current_cell_value:
                            
                            processed_list_element = list_element

                            if column_data_types[column] in { 'integer', 'bigint' }:
                                # CDA has no float values. Cast all numeric data to integers.
                                processed_list_element = round( processed_list_element )

                            elif column_data_types[column] not in { 'text', 'boolean' }:
                                # This isn't anticipated. Yell if we get something unexpected.
                                log.critical( f"Unexpected data type `{column_data_types[column]}` received; aborting. Please report this event to the CDA development team." )
                                return

                            processed_cell_value.append( processed_list_element )

                        processed_column_data.append( processed_cell_value )

                result_dataframe[column] = processed_column_data

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
                            
                            list_of_dicts_with_na_nulls = result_record[column].to_dict( orient='records' )

                            list_of_dicts_with_empty_string_nulls = list()

                            # This assumes 2D DataFrames, which is safe at time of writing (2025-05-07).

                            for dict_with_na_nulls in list_of_dicts_with_na_nulls:
                                
                                dict_with_empty_string_nulls = dict()

                                for key in dict_with_na_nulls:
                                    
                                    if dict_with_na_nulls[key] == '<NA>':
                                        
                                        dict_with_empty_string_nulls[key] = ''

                                    else:
                                        
                                        dict_with_empty_string_nulls[key] = dict_with_na_nulls[key]

                                list_of_dicts_with_empty_string_nulls.append( dict_with_empty_string_nulls )

                            if len( list_of_dicts_with_empty_string_nulls ) > 0:
                                
                                row_data.append( list_of_dicts_with_empty_string_nulls )

                            else:
                                
                                row_data.append( '' )

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


