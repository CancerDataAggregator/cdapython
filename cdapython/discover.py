import json
import os
import pandas as pd
import re

import cda_client
import cda_client.api.columns.columns_endpoint_columns_get
import cda_client.api.release_metadata.release_metadata_endpoint_release_metadata_get
import cda_client.api.column_values.column_values_endpoint_column_values_column_post

from cdapython.application_utilities import get_api_url
from cda_client.errors import UnexpectedStatus
from cdapython.logging_wrappers import get_logger

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
# cda_functions(): Return a list of user-facing cdapython functions useful for both scripting and interactive data sessions.
#
#############################################################################################################################

def cda_functions():
    """
    Returns a list of cdapython functions useful for both scripting and interactive data sessions.
    """

    # There doesn't seem to be a need to check invocation context before just printing to standard output,
    # here, as this function is primarily designed to be consulted in interactive sessions. If anyone ever
    # finds themselves checking this function's output programmatically, for some reason, and is annoyed
    # by the print statement, we will entertain pull requests to be more sensitive about our assumptions.

    print( '\nYou can get complete usage details for each function by calling "help( function_name )".', end='\n\n' )

    return [
            'tables',
            'columns',
            'column_values',
            'summarize_subjects',
            'summarize_files',
            'get_subject_data',
            'get_file_data',
            'intersect_subject_results',
            'intersect_file_results',
            'expand_subject_results',
            'expand_file_results',
            'get_valid_log_levels',
            'get_log_level',
            'set_log_level',
            'enable_console_logging',
            'disable_console_logging',
            'enable_file_logging',
            'disable_file_logging',
            'get_api_url',
            'set_api_url',
            'cda_functions'
    ]

#############################################################################################################################
#
# END cda_functions()
#
#############################################################################################################################

#############################################################################################################################
#
# tables(): Return a list of all searchable CDA data tables.
#
#############################################################################################################################

def tables():
    """
    Get a list of all searchable CDA data tables.

    Returns:
        list of strings: names of searchable CDA tables.
    """

    log = get_logger()

    # Call columns(), extract unique values from the `table` column of the
    # resulting DataFrame, and return those values to the user as a list.

    log.debug( 'Calling columns()' )

    columns_result_df = columns( return_data_as='dataframe' )

    if columns_result_df is None:
        log.error( 'Something went fatally wrong with columns( return_data_as="dataframe" ): got a null DataFrame back.' )
        return
    else:
        return sorted( columns_result_df['table'].unique() )

#############################################################################################################################
#
# END tables()
#
#############################################################################################################################

#############################################################################################################################
#
# columns(): Provide user with structured metadata describing searchable CDA columns:
#
#               (containing) table
#               column (name)
#               data_type (stored in column)
#               (values are) nullable(?)
#               (prose) description
#
#############################################################################################################################


def columns(
    *,
    return_data_as='',
    output_file='',
    sort_by='',
    **filter_arguments
):
    """
    Get structured metadata describing searchable CDA columns.

    Arguments:
        return_data_as ( string; optional: 'dataframe' or 'list' or 'tsv' ):
            Specify how columns() should return results: as a pandas DataFrame,
            a Python list, or as output written to a TSV file named by the user.
            If this argument is omitted, columns() will default to returning
            results as a DataFrame.

        output_file( string; optional ):
            If return_data_as='tsv' is specified, output_file should contain a
            resolvable path to a file into which columns() will write
            tab-delimited results.

        sort_by( string or list of strings; optional:
                    any combination of 'table', 'column', 'data_type',
                    and/or 'nullable'):
            Specify the column metadata field(s) on which to sort result data.
            Results will be sorted first by the first named field; groups of
            records sharing the same value in the first field will then be
            sub-sorted by the second field, and so on.

            Any field with a suffix of ':desc' appended to it will be sorted
            in reverse order; adding ':asc' will ensure ascending sort order.
            Example: sort_by=[ 'table', 'nullable:desc', 'column:asc' ]

    Filter arguments:
        table ( string or list of strings; optional ):
            Restrict returned data to columns from tables whose names match any
            of the given strings. A wildcard (asterisk) at either end (or both
            ends) of each string will allow partial matches. Case will be
            ignored.

        column ( string or list of strings; optional ):
            Restrict returned data to columns whose name matches any of the
            given strings. A wildcard (asterisk) at either end (or both ends)
            of each string will allow partial matches. Case will be ignored.

        data_type ( string or list of strings; optional ):
            Restrict returned data to columns whose data type matches any of
            the given strings. A wildcard (asterisk) at either end (or both
            ends) of each string will allow partial matches. Case will be
            ignored.

        nullable ( boolean; optional ):
            If set to True, restrict returned data to columns whose values are
            allowed to be empty; if False, return data only for columns
            requiring nonempty values.

        description ( string or list of strings; optional ):
            Restrict returned data to columns whose `description` field matches
            any of the given strings. Wildcards will be automatically applied
            if not provided, to support straightforward keyword searching of this
            field without requiring too much extra punctuation. Case will be
            ignored.

        exclude_table ( string or list of strings; optional ):
            Restrict returned data to columns from tables whose names do _not_
            match any of the given strings. A wildcard (asterisk) at either end
            (or both ends) of each string will allow partial matches. Case will
            be ignored.

    Returns:
        pandas.DataFrame where each row is a metadata record describing one
        searchable CDA column and is comprised of the following fields:

            `table` (string: name of the CDA table containing this column)
            `column` (string: name of this column)
            `data_type` (string: data type of this column)
            `nullable` (boolean: if True, this column can contain null values)`
            `description` (string: prose description of this column)

        OR list of column names

        OR returns nothing, but writes results to a user-specified TSV file
    """

    log = get_logger()

    #############################################################################################################################
    # Process return-type directives `return_data_as` and `output_file`.

    if not isinstance(return_data_as, str):
        
        log.error( f"Unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'." )
        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance( output_file, str ):
        
        log.error( f"The `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path." )
        return

    output_file = output_file.strip()

    allowed_return_types = { '', 'dataframe', 'tsv', 'list' }

    if return_data_as not in allowed_return_types:
        
        # Complain if we receive an unexpected `return_data_as` value.
        log.error( f"Unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'." )
        return

    elif return_data_as == 'tsv' and output_file == '':
        
        # If the user asks for a TSV, they also have to give us a path for that TSV. If they didn't, complain.
        log.error( "Return type 'tsv' was requested, but 'output_file' was not specified. Please specify output_file='some/path/string/to/write/your/tsv/to/your_tsv_output_file.tsv'." )
        return

    elif return_data_as != 'tsv' and output_file != '':
        
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        log.error( f"'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'." )
        log.error( '(Note that if you don\'t specify any value for \'return_data_as\', it defaults to \'dataframe\'.).' )
        return

    #############################################################################################################################
    # Process `sort_by` directives.

    if isinstance(sort_by, str):
        
        # Make `sort_by` a list, if it's not, so we don't have to split the way we
        # process this information into parallel distinct branches.

        if sort_by == '':
            sort_by = []
        else:
            sort_by = [ sort_by ]

    elif not isinstance(sort_by, list):
        
        # Also detect any disallowed incoming data types and complain if we find any.

        log.error( f"columns(): ERROR: 'sort_by' must be a string or a list of strings; you used '{sort_by}', which is neither." )
        return

    # Enumerate all allowed values that a user can specify using the `sort_by` parameter. ( 'X:asc' will be aliased immediately to just 'X'. )

    allowed_sort_by_arguments = [
        'table',
        'table:desc',
        'column',
        'column:desc',
        'data_type',
        'data_type:desc',
        'nullable',
        'nullable:desc',
    ]

    # Build two lists to pass to `DataFrame.sort_values()` to direct the sorting of our result data
    # according to user specifications:
    #
    # *   `by_list` will contain exact field names on which to sort, in order of precedence.
    #
    # *   `ascending_list` will be a series of boolean values, one for each field name in
    #     `by_list`, where each `False` value indicates that the corresponding field
    #     in `by_list` is to be sorted in reverse.

    by_list = list()

    ascending_list = list()

    seen_so_far = dict()

    for field_code in sort_by:
        
        if not isinstance(field_code, str):
            
            # Complain if we receive any unexpected data types instead of string directives.

            log.critical( f"columns(): ERROR: 'sort_by' must be a string or a list of strings; you used '{sort_by}', which is neither." )
            return

        # Let's not care about case.

        field_code = field_code.lower()

        # ':asc' is redundant. Remove it (politely).

        field_code = re.sub( r':asc$', r'', field_code )

        if field_code not in allowed_sort_by_arguments:
            
            # Complain if we receive any unexpected sort_by directives.

            log.critical( f"columns(): ERROR: '{field_code}' is not a valid directive for the 'sort_by' parameter. Please use one of [ '"
                            + "', '".join(allowed_sort_by_arguments)
                            + "' ] instead." )
            return

        code_basename = field_code

        if re.search( r':desc$', field_code ) is not None:
            code_basename = re.sub( r':desc$', '', field_code )

        if code_basename not in seen_so_far:
            
            seen_so_far[code_basename] = field_code

        else:
            
            # Complain if we receive multiple sort_by directives for the same output column.

            log.critical( f"columns(): ERROR: Multiple sort_by directives received for the same output column, including '{seen_so_far[code_basename]}' and '{field_code}': please specify only one directive per output column." )
            return

        by_list.append( code_basename )

        if re.search( r':desc$', field_code ) is not None:
            
            # Reverse the sort on this column.
            ascending_list.append( False )

        else:
            
            # Sort this column normally.
            ascending_list.append( True )

    # Report details of the final parsed sort logic.

    sort_dataframe = pd.DataFrame( { 'sort_by': by_list, 'ascending?': ascending_list } )

    if not sort_dataframe.empty:
        log.debug( f"Processed sort directives: {sort_dataframe}" )
    else:
        log.debug( f"Processed sort directives: <default>" )

    #############################################################################################################################
    # Process user-supplied filter directives.

    # Enumerate all allowed filters that a user can specify with named parameters.

    allowed_filter_arguments = [
        'table',
        'column',
        'data_type',
        'nullable',
        'description',
        'exclude_table'
    ]

    # Validate filter argument content.

    for filter_argument_name in filter_arguments:
        
        if filter_argument_name not in allowed_filter_arguments:
            
            # Complain if we receive any unexpected filter arguments.

            log.critical( f"columns(): ERROR: Received unexpected argument {filter_argument_name}; aborting." )
            return

        elif filter_argument_name == 'nullable':
            
            if not isinstance( filter_arguments[filter_argument_name], bool ):
                
                # Complain if we got a parameter value of the wrong data type.

                log.critical( f"columns(): ERROR: 'nullable' must be a Boolean value (True or False); you used '{filter_arguments[filter_argument_name]}', which is not." )
                return

        elif not ( isinstance( filter_arguments[filter_argument_name], str ) or isinstance( filter_arguments[filter_argument_name], list ) ):
            
            # Complain if we got a parameter value of the wrong data type.

            log.critical( f"columns(): ERROR: '{filter_argument_name}' must be a string or a list of strings; you used '{filter_arguments[filter_argument_name]}', which is neither." )
            return

        elif isinstance( filter_arguments[filter_argument_name], list ):
            
            for pattern in filter_arguments[filter_argument_name]:
                
                if not isinstance( pattern, str ):
                    
                    # Complain if we receive any unexpected data types inside a filter list (i.e. anything but strings).

                    log.critical( f"columns(): ERROR: '{filter_argument_name}' must be a string or a list of strings; you used '{filter_arguments[filter_argument_name]}', which is neither." )
                    return

    # Report details of fully processed user directives prior to querying.

    status_report = f"return_data_as='{return_data_as}', output_file='{output_file}'"

    status_report = status_report + f", sort_by={sort_by}"

    for filter_argument_name in filter_arguments:
        
        if isinstance( filter_arguments[filter_argument_name], str ):
            status_report = status_report + f", {filter_argument_name}='{filter_arguments[filter_argument_name]}'"
        else:
            status_report = status_report + f", {filter_argument_name}={filter_arguments[filter_argument_name]}"

    log.debug( f"Processed user directives: '{status_report}'" )

    #############################################################################################################################
    # Fetch data from the API.

    query_api_instance = cda_client.Client( base_url=get_api_url(), raise_on_unexpected_status=True )

    # Ask the columns endpoint for information. (It has no parameters.)

    try:
        columns_response_data_object = cda_client.api.columns.columns_endpoint_columns_get.sync( client=query_api_instance )
    except UnexpectedStatus as error:
        log.error( f"UnexpectedStatus error from API, status code {error.status_code}: {json.loads( error.content )['message']}" )
        return
    except Exception as error:
        log.error( f"{type(error)}: {error}" )
        return

    #############################################################################################################################
    # Postprocess API result data.

    # columns_response_data_object['result'] is an array of dicts, with
    # each dict containing a few named fields of metadata describing one column.
    #
    # Example:
    #
    #   "result": [
    #       {
    #           "table": "file",
    #           "column": "file_id",
    #           "data_type": "text",
    #           "nullable": false,
    #           "description": "A unique identifier for this file minted by CDA. May change release-to-release. Contains no semantically reliable content with one exception: in the case of a DICOM series from IDC, this field will contain the crdc_series_uuid assigned by IDC to that DICOM series. Note that this crdc_series_uuid may change from one IDC release version to the next, according to IDC's data processing and identification rules."
    #       },
    #       ...
    #   ]

    # Make a DataFrame from this array of dicts using DataFrame.from_records(), and explicitly specify the
    # column ordering for the resulting DataFrame using the `columns=[]` parameter.

    result_dataframe = pd.DataFrame.from_records( columns_response_data_object.to_dict()['result'], columns=[ 'table', 'column', 'data_type', 'nullable', 'description' ] )

    # Remove `table`_data_source_count and *_alias columns from output.
    # 
    # TO DO: Maybe put this list somewhere easier to find.

    banned_column_name_patterns = {
        r'_crdc_id$',
        r'^[^_]+_data_source_count$',
        r'^[^_]+_data_at_[^_]+$',
        r'_alias$'
    }

    banned_columns = set()

    for column_name in result_dataframe['column'].unique():
        for banned_pattern in banned_column_name_patterns:
            if re.search( banned_pattern, column_name ) is not None:
                banned_columns.add( column_name )

    for banned_column in banned_columns:
        result_dataframe = result_dataframe.loc[ result_dataframe['column'] != banned_column ]

    log.debug( 'Created result DataFrame' )

    #############################################################################################################################
    # Execute sorting directives, if we got any; otherwise perform the default sort on the result DataFrame.

    if len( sort_by ) == 0:
        
        # By default, we sort column records by column name, gathered into groups by table,
        # to facilitate predictable output patterns. For easy access, we'd also like each
        # table's ID column to show up first in its table's group.
        #
        # Temporarily prepend a '.' to `table`_id column names, so they float to the top of each
        # table's list of columns when we sort.

        result_dataframe = result_dataframe.replace( to_replace=r'^([^_]+_id)$', value=r'.\1', regex=True )

        # Sort all column records, first on table and then on column name.

        result_dataframe = result_dataframe.sort_values( by=['table', 'column'], ascending=[True, True] )

        # Remove the '.' characters we temporarily prepended to `table`_id column names
        # to force the sorting algorithm to place all such columns first within each
        # table's group of column records.

        result_dataframe = result_dataframe.replace( to_replace=r'^\.(.*)$', value=r'\1', regex=True )

    else:
        
        # Sort all column records according to the user-specified directives we've processed.

        result_dataframe = result_dataframe.sort_values( by=by_list, ascending=ascending_list )

    log.debug( 'Applied sort_by directives' )

    #############################################################################################################################
    # Iterate through whatever filters the user passed us and
    # apply them to the result data before sending it back.
    #
    # The value of filter_name, here, will be one of
    # 'table', 'column', 'data_type', 'nullable',
    # 'description' or 'exclude_table'.

    for filter_name in filter_arguments:
        
        # Grab the filters the user sent us.

        # Default behavior: all result values must be exact matches to at least one
        # filter (ignoring case). To match end-to-end, we use a ^ to represent
        # the beginning of each value and a $ to indicate the end. If the user
        # specifies wildcards on one or both ends of a filter, we'll remove one or both
        # restrictions as instructed for that filter.
        #
        # EXCEPTION ONE: the `nullable` filter argument will be a single Boolean
        # value, and we handle it separately.
        #
        # EXCEPTION TWO: For the `description` field, we've modified the
        # argument processing (because in this case users are searching an
        # abstract-sized block of text, not a short string representing
        # a name or a concept) so that filters will always be processed
        # as if they have wildcards on both ends. (Search will still be
        # case-insensitive at all times.)
        #
        # EXCEPTION THREE: In the case of `exclude_table`, all result values must
        # _not_ match _any_ of the specified filters.

        if filter_name == 'nullable':
            
            return_if_nullable = filter_arguments[filter_name]

            if not isinstance( return_if_nullable, bool ):
                log.critical( f"columns(): ERROR: Please specify either nullable=True or nullable=False, not (what you sent) nullable='{return_if_nullable}'." )
                return

            result_dataframe = result_dataframe.loc[ result_dataframe['nullable'] == return_if_nullable ]

        else:
            
            filters = filter_arguments[filter_name]

            filter_patterns = list()

            # If the filter list wasn't a list at all but a (nonempty) string, we just have
            # one filter. Listify it (so we don't have to care downstream about how many there are).
            # Otherwise, just start with the list they sent us.

            if isinstance( filters, str ) and filters != '':
                filter_patterns = [filters]
            elif isinstance( filters, list ):
                filter_patterns = filters

            # (If neither of the above conditions was met, `filter_patterns` will remain an
            # empty list, and the rest of this filter-processing section will (by design) have no effect.

            # Parse filter_name to establish which columns() field is being targeted
            # for filtration and adjust default filtration logic as necessary according to the result.

            target_field = filter_name

            if filter_name == 'description':
                
                # Never match end-to-end for query strings applied to description text (see discussion above).

                updated_pattern_list = list()

                for original_filter_pattern in filter_patterns:
                    
                    updated_filter_pattern = f"*{original_filter_pattern}*"
                    updated_pattern_list.append( updated_filter_pattern )

                filter_patterns = updated_pattern_list

            elif filter_name == 'exclude_table':
                
                target_field = 'table'

            match_pattern_string = ''

            for filter_pattern in filter_patterns:
                
                # Process wildcard characters.

                if re.search( r'^\*', filter_pattern ) is not None:
                    
                    # Any prefix will do, now.
                    #
                    # Strip leading '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                    filter_pattern = re.sub( r'^\*+', r'', filter_pattern )

                else:
                    
                    # No wildcard at the beginning of `filter_pattern` --> require all successful matches to _begin_ with `filter_pattern` by prepending a ^ character to `filter_pattern`:
                    #
                    # ...I know this looks weird, but it's just tacking a '^' character onto the beginning of `filter_pattern`.

                    filter_pattern = re.sub( r'^', r'^', filter_pattern )

                if re.search( r'\*$', filter_pattern ) is not None:
                    
                    # Any suffix will do, now.
                    #
                    # Strip trailing '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                    filter_pattern = re.sub( r'\*+$', r'', filter_pattern )

                else:
                    
                    # No wildcard at the end of `filter_pattern` --> require all successful matches to _end_ with `filter_pattern` by appending a '$' character to `filter_pattern`:
                    #
                    # ...I know this looks weird, but it's just tacking a '$' character onto the end of `filter_pattern`.

                    filter_pattern = re.sub( r'$', r'$', filter_pattern )

                # Build the overall match pattern as we go, one (processed) `filter_pattern` at a time.

                match_pattern_string = match_pattern_string + filter_pattern + '|'

            # Strip final trailing |.

            match_pattern_string = re.sub( r'\|$', r'', match_pattern_string )

            if filter_name == 'exclude_table':
                
                # Retain all rows where the value of `target_field` (in this case, the value of `table`) does _not_ match any of the given filter patterns.

                result_dataframe = result_dataframe.loc[ ~( result_dataframe[target_field].str.contains( match_pattern_string, case=False ) ) ]

            else:
                
                # Retain all rows where the value of `target_field` matches any of the given filter patterns.

                result_dataframe = result_dataframe.loc[ result_dataframe[target_field].str.contains( match_pattern_string, case=False ) ]

    log.debug( 'Applied value-filtration directives' )

    #############################################################################################################################
    # Send the results back to the user.

    # Reindex DataFrame rows to match their final sort order.

    result_dataframe = result_dataframe.reset_index( drop=True )

    if return_data_as == '':
        
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        # The following, for the dubiously useful record, is a somewhat worse alternative default thing to do.
        #
        # print( result_dataframe.to_string( index=False, justify='right', max_rows=25, max_colwidth=50 ), file=sys.stdout )

        log.debug( 'Returning results in default form (pandas.DataFrame)' )

        return result_dataframe

    elif return_data_as == 'dataframe':
        
        # Give the user back the results DataFrame.

        log.debug( 'Returning results as pandas.DataFrame' )

        return result_dataframe

    elif return_data_as == 'list':
        
        # Give the user back a list of column names.

        log.debug( 'Returning results as list of column names' )

        return result_dataframe['column'].to_list()

    else:
        
        # Write the results DataFrame to a user-specified TSV file.

        log.debug( f"Printing results to TSV file '{output_file}'" )

        try:
            
            result_dataframe.to_csv( output_file, sep='\t', index=False )
            return

        except Exception as error:
            raise RuntimeError( f"Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'." )

#############################################################################################################################
#
# END columns()
#
#############################################################################################################################

#############################################################################################################################
#
# column_values( column=`column` ): Show all distinct values present in `column`, along with a count of occurrences for each value.
#
#############################################################################################################################

def column_values(
    column='',
    *,
    return_data_as='',
    output_file='',
    sort_by='',
    filters=None,
    data_source='',
    force=False
):
    """
    Show all distinct values present in `column`, along with a count
    of occurrences for each value.

    Arguments:
        column ( string; required ):
            The column to fetch values from.

        return_data_as ( string; optional: 'dataframe' or 'list' or 'tsv' ):
            Specify how column_values() should return results: as a pandas
            DataFrame, a Python list, or as output written to a TSV file named
            by the user.  If this argument is omitted, column_values() will default
            to returning results as a DataFrame.

        output_file( string; optional ):
            If return_data_as='tsv' is specified, output_file should contain a
            resolvable path to a file into which column_values() will write
            tab-delimited results.

        sort_by( string; optional:
                'count' ( default for return_data_as='dataframe' and
                return_data_as='tsv' ) or 'value' ( default for
                return_data_as='list' ) or 'count:desc' or 'value:desc'
                or 'count:asc' or 'value:asc' ):
            Specify the primary column to sort when preparing result data: on
            values, or on counts of values.

            A column name with a suffix of ':desc' appended to it will be
            sorted in reverse order; adding ':asc' will ensure ascending sort
            order. Example: sort_by='value:desc'

            Secondary sort order is automatic: if the results are to be
            primarily sorted by count, then the automatic behavior will be to
            also (alphabetically) sort by value within each group of values
            that all share the same count. If results are primarily sorted by
            value, then there is no secondary sort -- each value is unique by
            design, so results don't contain groups with the same value but
            different counts, so there's nothing to arrange once the primary
            sort has been applied.

        filters ( string or list of strings; optional ):
            Restrict returned values to those matching any of the given strings.
            A wildcard (asterisk) at either end (or both ends) of each string
            will allow partial matches. Case will be ignored. Specify an empty
            filter string '' to match and count missing (null) values.

        data_source ( string; optional ):
            Restrict returned values to the given upstream data source. Current
            valid values are 'GDC', 'PDC', 'IDC', 'CDS' and 'ICDC'.
            Defaults to '' (no filter).

        force( boolean; optional ):
            Force execution of high-overhead queries on columns (like IDs)
            flagged as having large numbers of values. Defaults to False,
            in which case attempts to retrieve values for flagged columns
            will result in a warning.


    Returns:
        pandas.DataFrame OR list OR returns nothing, but writes retrieved
        data to a user-specified TSV file
    """

    log = get_logger()

    #############################################################################################################################
    # Check for our one required parameter: column.

    if ( not isinstance( column, str ) ) or column == '':
        log.critical( 'column_values(): ERROR: parameter \'column\' cannot be omitted. Please specify a column from which to fetch a list of distinct values.')
        return

    # Let's not care about case, and if there's whitespace in our column name, remove it before it does any damage.
    column = re.sub( r'\s+', r'', column ).lower()

    # See if columns() agrees that the requested column exists. Note: cdapython.columns() and
    # the API's /columns endpoint give different sets of columns, by design. Here we want our output
    # to match the former, because we postprocess some of the columns offered by the API instead of
    # exposing them directly.

    if len( columns( column=column, return_data_as='list' ) ) == 0:
        log.critical( f"column_values(): ERROR: parameter 'column' must be a searchable CDA column name. You supplied '{column}', which is not." )
        return

    #############################################################################################################################
    # Check the data_source parameter.

    if not isinstance( data_source, str ):
        log.error( f"The 'data_source' parameter must be a string (e.g. 'GDC'); you specified '{data_source}', which is not." )
        return

    # Let's not care about case, and remove any whitespace before it can do any damage.
    data_source = re.sub( r'\s+', r'', data_source ).upper()

    # TEMPORARY: enumerate valid `data_source` values and warn the user if they supplied something else.
    #
    # This should be replaced ASAP with a fetch from the /release_metadata endpoint.

    allowed_data_source_values = {
        'GDC',
        'PDC',
        'IDC',
        'CDS',
        'ICDC'
    }

    if data_source != '' and data_source not in allowed_data_source_values:
        log.error( f"The 'data_source' parameter must be one of [ 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' ]. You supplied '{data_source}', which is not." )
        return

    #############################################################################################################################
    # Check in advance for columns flagged as high-overhead.

    expensive_columns = {
        'file_id',
        'description',
        'drs_uri',
        'file_name',
        'size',
        'case_id',
        'hgnc_id',
        'transcript_id',
        'aliquot_barcode_normal',
        'aliquot_barcode_tumor',
        'case_barcode',
        'dbsnp_rs',
        'entrez_gene_id',
        'gene',
        'hugo_symbol',
        'matched_norm_aliquot_uuid',
        'normal_submitter_uuid',
        'reference_allele',
        'sample_barcode_normal',
        'sample_barcode_tumor',
        'tumor_aliquot_uuid',
        'tumor_seq_allele1',
        'tumor_seq_allele2',
        'tumor_submitter_uuid',
        'subject_id'
    }

    # Warn the user if an override hasn't been requested.

    if not force and column in expensive_columns:
        log.warning( f"'{column}' has a very large number of values; retrieval is blocked by default. To perform this query, use column_values( ..., 'force=True' )." )
        return

    #############################################################################################################################
    # Listify `filters`, if it's a string, so we can process it in a uniform way later on.

    if filters is None:
        filters = list()
    elif isinstance( filters, str ):
        filters = [ filters ]

    #############################################################################################################################
    # Process return_data_as and output_file directives.

    allowed_return_types = {
        '',
        'dataframe',
        'tsv',
        'list'
    }

    if not isinstance( return_data_as, str ):
        log.critical( f"column_values(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'." )
        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'
    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance( output_file, str ):
        log.critical( f"column_values(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path." )
        return

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        
        log.critical( f"column_values(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'." )
        return

    elif return_data_as == 'tsv' and output_file == '':
        
        log.critical( 'column_values(): ERROR: return type \'tsv\' requested, but \'output_file\' not specified. Please specify output_file=\'some/path/string/to/write/your/tsv/to\'.')
        return

    elif return_data_as != 'tsv' and output_file != '':
        
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        log.error( f"'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'.\n(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.)." )
        return

    #############################################################################################################################
    # Process sorting directives.

    # Enumerate all allowed values that a user can specify using the `sort_by` parameter. ( 'X:asc' will be aliased immediately to just 'X'. )

    allowed_sort_by_options = {
        'list': {
            '',
            'value',
            'value:desc'
        },
        'dataframe_or_tsv': {
            '',
            'count',
            'count:desc',
            'value',
            'value:desc'
        }
    }

    if not isinstance( sort_by, str ):
        
        # Complain if we receive any unexpected data types instead of string directives.

        log.critical( f"column_values(): ERROR: 'sort_by' must be a string; you used '{sort_by}', which is not." )
        return

    # Let's not care about case. Also, ':asc' is redundant: remove it (politely).
    sort_by = re.sub( r':asc$', r'', sort_by ).lower()

    if return_data_as == 'list':
        
        # Restrict sorting options for lists.

        if sort_by == '':
            sort_by = 'value'
        elif sort_by not in allowed_sort_by_options['list']:
            log.critical( f"column_values(): ERROR: return_data_as='list' can only be processed with sort_by='value' or sort_by='value:desc' (or omitting sort_by altogether). Please modify unsupported sort_by directive '{sort_by}' and try again." )
            return

    else:
        
        # For TSV output files and DataFrames, we support more user-configurable options (defaulting to sort_by='count:desc'):

        if sort_by == '':
            sort_by = 'count:desc'
        elif sort_by not in allowed_sort_by_options['dataframe_or_tsv']:
            log.critical( f"column_values(): ERROR: unrecognized sort_by '{sort_by}'. Please use one of 'count', 'value', 'count:desc', 'value:desc', 'count:asc' or 'value:asc' (or omit the sort_by parameter altogether)." )
            return

    #############################################################################################################################
    # Report details of the final parsed parameters.

    parameter_dict = {
        'column': column,
        'return_data_as': return_data_as,
        'output_file': output_file,
        'sort_by': sort_by,
        'filters': filters,
        'data_source': data_source,
        'force': force,
    }

    log.debug( f"Processed all parameter directives. Calling API to fetch data for\n{json.dumps( parameter_dict, indent=4 )}\n" )

    #############################################################################################################################
    # Fetch data from the API.

    query_api_instance = cda_client.Client( base_url=get_api_url(), raise_on_unexpected_status=True )
    
    starting_offset = 0
    records_per_page = 500000

    try:
        paged_response_data_object = (
            cda_client.api.column_values.column_values_endpoint_column_values_column_post.sync(
                client=query_api_instance,
                column=column,
                data_source=data_source,
                limit=records_per_page,
                offset=starting_offset
            )
        )
    except UnexpectedStatus as error:
        log.error( f"UnexpectedStatus error from API, status code {error.status_code}: {json.loads( error.content )['message']}" )
        return
    except Exception as error:
        log.error( f"{type(error)}: {error}" )
        return

    log.debug( f"Sending query to API:\n{json.dumps( { 'columnname': column, 'system': data_source, 'count': True, 'total_count': True, 'limit': records_per_page, 'offset': starting_offset }, indent=4 )}\n" )

    # Report some metadata about the results we got back.

    log.debug( f"Number of result rows: {paged_response_data_object.total_row_count}" )

    log.debug( f"Query SQL: '{paged_response_data_object.query_sql}'" )

    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function. Example JSON response:
    #
    # {
    #     "result": [
    #             {
    #                 "sex": "female",
    #                 "value_count": 31215
    #             },
    #             {
    #                 "sex": "male",
    #                 "value_count": 28571
    #             },
    #             {
    #                 "sex": null,
    #                 "value_count": 64240
    #             }
    #     ],
    #     "query_sql": "SELECT row_to_json(column_json) AS row_to_json_1 FROM (SELECT observation.sex AS sex, count(*) AS value_count FROM observation WHERE observation.data_at_gdc IS true GROUP BY observation.sex ORDER BY observation.sex) AS column_json",
    #     "total_row_count": 3,
    #     "next_url": null
    # }

    log.debug( f"Page one results ( NOTE: pattern filters have not yet been applied ):\n{json.dumps( paged_response_data_object.to_dict(), indent=4 )}\n" )

    result_dataframe = pd.json_normalize( paged_response_data_object.to_dict()['result'] )

    # The data we've fetched so far might be just the first page (if the total number
    # of results is greater than `records_per_page`).
    #
    # Get the rest of the result pages, if there are any, and add each page's data
    # onto the end of our results DataFrame.

    incremented_offset = starting_offset + records_per_page

    more_than_one_result_page = False

    if paged_response_data_object.next_url is not None:
        log.debug( 'Fetching remaining results in pages...' )
        more_than_one_result_page = True

    while paged_response_data_object.next_url is not None and len( paged_response_data_object.next_url ) > 0:
        
        log.debug( f"   ...fetching {paged_response_data_object.next_url}..." )

        try:
            paged_response_data_object = (
                cda_client.api.column_values.column_values_endpoint_column_values_column_post.sync(
                    client=query_api_instance,
                    column=column,
                    data_source=data_source,
                    limit=records_per_page,
                    offset=incremented_offset
                )
            )
        except UnexpectedStatus as error:
            log.error( f"UnexpectedStatus error from API, status code {error.status_code}: {json.loads( error.content )['message']}" )
            return
        except Exception as error:
            log.error( f"{type(error)}: {error}" )
            return

        next_result_batch = pd.json_normalize( paged_response_data_object.to_dict()['result'] )

        if not result_dataframe.empty and not next_result_batch.empty:
            
            # Silence a future deprecation warning about pd.concat and empty DataFrame columns.

            next_result_batch = next_result_batch.astype( result_dataframe.dtypes )

            result_dataframe = pd.concat( [result_dataframe, next_result_batch] )

        incremented_offset = incremented_offset + records_per_page

    if more_than_one_result_page:
        log.debug( '...done.' )

    #############################################################################################################################
    # Postprocess API result data, if there is any.

    if len( result_dataframe ) == 0:
        return result_dataframe

    log.debug( 'Postprocessing results' )

    log.debug( 'Casting counts to integers and fixing symmetry for returned column labels...' )

    # Sanity check on expected result column 'value_count':

    if 'value_count' not in result_dataframe.columns:
        log.error( 'Expected column \'value_count\' not present in API response.' )
        return

    # Term-count values come in as floats. Make them not that.

    result_dataframe['value_count'] = result_dataframe['value_count'].astype( int )

    log.debug( 'Handling missing values...' )

    # CDA has no float values. If the API gives us some, cast them to integers.
    
    if result_dataframe[column].dtype == 'float64':
        
        # Columns of type `float64` can contain NaN (missing) values, which cannot (for some reason)
        # be stored in Pandas Series objects (i.e., DataFrame columns) of type `int` or `int64`.
        # Pandas workaround: use extension type 'Int64' (note initial capital), which supports the
        # storage of missing values. These will print as '<NA>'.

        result_dataframe[column] = result_dataframe[column].round().astype( 'Int64' )

    elif result_dataframe[column].dtype == 'object':
        
        # String data comes through as a column with dtype 'object', based on something involving
        # the variability inherent in string lengths.
        #
        # See https://stackoverflow.com/questions/33957720/how-to-convert-column-with-dtype-as-object-to-string-in-pandas-dataframe

        # Replace term values that are None (== null) with empty strings.

        result_dataframe = result_dataframe.fillna( '' )

    elif result_dataframe[column].dtype == 'bool':
        
        result_dataframe = result_dataframe.fillna( '' )

    else:
        
        # This isn't anticipated. Yell if we get something unexpected.

        log.critical( f"column_values(): ERROR: Unexpected data type `{result_dataframe[column].dtype}` received; aborting. Please report this event to the CDA development team." )
        return

    #############################################################################################################################
    # Filter returned values according to user specifications.

    # Default behavior: all result values must be exact matches to at least one
    # filter (ignoring case). To match end-to-end, we use a ^ to represent
    # the beginning of each value and a $ to indicate the end. If the user
    # specifies wildcards on one or both ends of a filter, we'll remove one or both
    # restrictions as instructed for that filter.

    match_pattern_string = ''

    # If the user includes an empty string in the filters list, make sure we return
    # a count for empty (null) values in addition to any values matching other filters.

    include_null_count = False

    for filter_pattern in filters:
        
        if filter_pattern == '':
            
            include_null_count = True

        else:
            
            # Process wildcard characters.

            if re.search( r'^\*', filter_pattern ) is not None:
                
                # Any prefix will do, now.
                #
                # Strip leading '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                filter_pattern = re.sub( r'^\*+', r'', filter_pattern )

            else:
                
                # No wildcard at the beginning of `filter_pattern` --> require all successful matches to _begin_ with `filter_pattern` by prepending a ^ character to `filter_pattern`:
                #
                # ...I know this looks weird, but it's just tacking a '^' character onto the beginning of `filter_pattern`.

                filter_pattern = re.sub( r'^', r'^', filter_pattern )

            if re.search( r'\*$', filter_pattern ) is not None:
                
                # Any suffix will do, now.
                #
                # Strip trailing '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                filter_pattern = re.sub( r'\*+$', r'', filter_pattern )

            else:
                
                # No wildcard at the end of `filter_pattern` --> require all successful matches to _end_ with `filter_pattern` by appending a '$' character to `filter_pattern`:
                #
                # ...I know this looks weird, but it's just tacking a '$' character onto the end of `filter_pattern`.

                filter_pattern = re.sub( r'$', r'$', filter_pattern )

            # Build the overall match pattern as we go, one (processed) `filter_pattern` at a time.

            match_pattern_string = match_pattern_string + filter_pattern + '|'

    # Strip the final trailing '|' character from the end of the last `filter_pattern`.

    match_pattern_string = re.sub( r'\|$', r'', match_pattern_string )

    print_regex = match_pattern_string

    if include_null_count:
        if print_regex == '':
            print_regex = '(missing values)'
        else:
            print_regex = print_regex + '|(missing values)'

    if print_regex == '':
        print_regex = '(none)'
    else:
        print_regex = f"/{print_regex}/"

    log.debug( f"Applying pattern filters: {print_regex}" )

    # Filter results to match the full aggregated regular expression in `match_pattern_string`.

    if include_null_count and match_pattern_string != '':
        
        result_dataframe = result_dataframe.loc[
            result_dataframe[column].astype( str ).str.contains( match_pattern_string, case=False )
            | result_dataframe[column].astype( str ).str.contains( r'^$' )
            | result_dataframe[column].isna()
        ]

    elif include_null_count:
        
        result_dataframe = result_dataframe.loc[ result_dataframe[column].astype( str ).str.contains( r'^$' ) | result_dataframe[column].isna() ]

    else:
        
        # This will return unfiltered results if `match_pattern_string` is empty (i.e. if the user asked for no filters to be applied),
        # and will filter results according to `match_pattern_string` if not.

        result_dataframe = result_dataframe.loc[ result_dataframe[column].astype( str ).str.contains( match_pattern_string, case=False ) ]

    # Sort results. Default (note that the final value of `sort_by` is determined earlier in this function) is to sort by term count, descending.

    log.debug( f"Applying sort directive '{sort_by}'..." )

    if sort_by == 'count':

        # Sort by count; break ties among groups of values with identical counts by sub-sorting each such group alphabetically by value.
        result_dataframe = result_dataframe.sort_values( by=['value_count', column], ascending=[True, True] )

    elif sort_by == 'count:desc':
        
        # Sort by count, descending; break ties among groups of values with identical counts by sub-sorting each such group alphabetically by value.
        result_dataframe = result_dataframe.sort_values( by=['value_count', column], ascending=[False, True] )

    elif sort_by == 'value':
        
        # No need for a sub-sort, here, since values aren't repeated.
        result_dataframe = result_dataframe.sort_values( by=column, ascending=True )

    elif sort_by == 'value:desc':
        
        # No need for a sub-sort, here, since values aren't repeated.
        result_dataframe = result_dataframe.sort_values( by=column, ascending=False )

    else:
        
        log.error( 'Something has gone horribly wrong; we should never get here.' )
        return

    #############################################################################################################################
    # Send the results back to the user.

    # Reindex DataFrame rows to match their final sort order.

    result_dataframe = result_dataframe.reset_index( drop=True )

    # Pretty-print missing values.

    if result_dataframe[column].dtype == 'object':
        
        # String data comes through as a column with dtype 'object', based on something involving
        # the variability inherent in string lengths.
        #
        # See https://stackoverflow.com/questions/33957720/how-to-convert-column-with-dtype-as-object-to-string-in-pandas-dataframe

        # Replace null string values with <NA> to match what we['re forced to] use for numeric data.

        result_dataframe = result_dataframe.replace( r'^$', r'<NA>', regex=True )

    elif result_dataframe[column].dtype == 'bool':
        
        # Replace null boolean values with <NA> to match what we['re forced to] use for numeric data.
        result_dataframe = result_dataframe.replace(r'^$', r'<NA>', regex=True)

    if return_data_as == '':
        
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        # The following, for the dubiously useful record, is a somewhat worse alternative default thing to do.
        #
        # print( result_dataframe.to_string( index=False, justify='right', max_rows=25, max_colwidth=50 ), file=sys.stdout )

        log.debug( 'Returning results in default form (pandas.DataFrame)' )

        return result_dataframe

    elif return_data_as == 'dataframe':
        
        # Give the user back the results DataFrame.

        log.debug( 'Returning results as pandas.DataFrame' )
        return result_dataframe

    elif return_data_as == 'list':
        
        # Strip the term-values column out of the results DataFrame and give them to the user as a Python list.

        log.debug( 'Returning results as list of column values' )
        return result_dataframe[column].to_list()

    else:
        
        # Write the results DataFrame to a user-specified TSV file.

        log.debug( f"Printing results to TSV file '{output_file}'" )

        try:
            result_dataframe.to_csv( output_file, sep='\t', index=False )
            return

        except Exception as error:
            raise RuntimeError( f"Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'." )

#############################################################################################################################
#
# END column_values()
#
#############################################################################################################################

#############################################################################################################################
#
# release_metadata(): Return a list of metadata dicts describing the current CDA release, columnwise.
#
#############################################################################################################################

def release_metadata():
    """
    Return a list of metadata dicts describing the current CDA release, columnwise.
    """

    log = get_logger()

    log.debug( 'Querying /release_metadata endpoint' )

    query_api_instance = cda_client.Client( base_url=get_api_url(), raise_on_unexpected_status=True )

    try:
        release_metadata_response_data_object = cda_client.api.release_metadata.release_metadata_endpoint_release_metadata_get.sync( client=query_api_instance )
    except UnexpectedStatus as error:
        raise RuntimeError( f"UnexpectedStatus error from API, status code {error.status_code}: {json.loads( error.content )['message']}" )
    except Exception as error:
        raise RuntimeError( f"{type(error)}: {error}" )

    return release_metadata_response_data_object.to_dict()['result']

#############################################################################################################################
#
# END release_metadata()
#
#############################################################################################################################


