import json
import pandas as pd
import os
import re
import sys

import tabulate
from cda_client import openapi_client
from cda_client.openapi_client.models import ColumnResponseObj
from cda_client.openapi_client import ApiException

# Nomenclature notes:
# 
# * try to standardize all potential user-facing synonyms for basic database data structures
#   (field, entity, endpoint, cell, value, term, etc.) to "table", "column", "row" and "value".

class CdaApiQueryEncoder( json.JSONEncoder ):

    def default( self, o ):
        
        if type( o ) == 'mappingproxy':
            
            return None

        tmp_dict = vars( o )

        if "query" in tmp_dict:
            
            return tmp_dict["query"]

        if "_data_store" in tmp_dict:
            
            return tmp_dict["_data_store"]

        return None

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

    # Wrap columns(), scrape out the relevant results, and return as a sorted list.
    # 
    # We are aware that this is inefficient. At time of writing we're trying very hard not to perturb
    # the existing API logic wherever possible: getting a more streamlined list of table names from
    # the database via the API would involve altering existing endpoints or creating a new one, and columns()
    # isn't experience-damagingly expensive to run. Hence the current compromise.

    # Call columns(), extract unique values from the `table` column of the
    # resulting DataFrame, and return those values to the user as a list.

    columns_result_df = columns( return_data_as='dataframe' )

    if columns_result_df is None:
        
        print( f"tables(): ERROR: Something went fatally wrong with columns(); can't complete tables(), aborting.", file=sys.stderr )

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
    return_data_as = '',
    output_file = '',
    sort_by = '',
    debug = False,
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

        debug( boolean; optional ):
            If set to True, print internal process details to the standard
            error stream.

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

    #############################################################################################################################
    # TEMPORARY ban list: filtering on these columns is problematic at the API level, and we don't have a consistent modeling
    # structure for them either way. Disabling any mention of them until (a) we update to the CRDC Common Model, with its
    # dedicated `project` entity, or (b) we decide to fix the API issues [specifically that it won't correctly apply
    # filters on these columns unless requested from their home endpoints, i.e. a `subjects` query will correctly filter
    # results on `subject_associated_project`, but no other endpoints will filter their own results properly using
    # `subject_associated_project`]. Drawback to doing (b) before (a) is that users would have to deal with
    # chaotically inconsistent project-name modeling (associative auxiliary tables for `file` and `subject`,
    # an atomic in-table field for `researchsubject`, a semicolon-separated list in a text field for `specimen`;
    # nothing direct at all for `diagnosis` or `treatment`; and whatever ISB-CGC populates the `somatic_mutation`
    # `project_short_name` field with.

    banned_columns = [
        'file_associated_project',
        'subject_associated_project'
    ]

    #############################################################################################################################
    # Ensure nothing untoward got passed into the `debug` parameter.
    # 
    # Fun exercise for reader: compare results if `isinstance( debug, bool )` is used.

    if debug != True and debug != False:
        
        print( f"columns(): ERROR: The `debug` parameter must be set to True or False; you specified '{debug}', which is neither.", file=sys.stderr )

        return

    #############################################################################################################################
    # Process return-type directives `return_data_as` and `output_file`.

    allowed_return_types = {
        '',
        'dataframe',
        'tsv',
        'list'
    }

    if not isinstance( return_data_as, str ):
        
        print( f"columns(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'.", file=sys.stderr )

        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance( output_file, str ):
        
        print( f"columns(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path.", file=sys.stderr )

        return

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        
        # Complain if we receive an unexpected `return_data_as` value.

        print( f"columns(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'.", file=sys.stderr )

        return

    elif return_data_as == 'tsv' and output_file == '':
        
        # If the user asks for a TSV, they also have to give us a path for that TSV. If they didn't, complain.

        print( f"columns(): ERROR: return type 'tsv' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/tsv/to'.", file=sys.stderr )

        return

    elif return_data_as != 'tsv' and output_file != '':
        
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        print( f"columns(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'.", file=sys.stderr )
        print( f"(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.).", file=sys.stderr )

        return

    #############################################################################################################################
    # Process `sort_by` directives.

    if isinstance( sort_by, str ):
        
        # Make `sort_by` a list, if it's not, so we don't have to split the way we
        # process this information into parallel distinct branches.

        if sort_by == '':
            
            sort_by = []

        else:
            
            sort_by = [ sort_by ]

    elif not isinstance( sort_by, list ):
        
        # Also detect any disallowed incoming data types and complain if we find any.

        print( f"columns(): ERROR: 'sort_by' must be a string or a list of strings; you used '{sort_by}', which is neither.", file=sys.stderr )

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
        'nullable:desc'
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
        
        if not isinstance( field_code, str ):
            
            # Complain if we receive any unexpected data types instead of string directives.

            print( f"columns(): ERROR: 'sort_by' must be a string or a list of strings; you used '{sort_by}', which is neither.", file=sys.stderr )

            return

        # Let's not care about case.

        field_code = field_code.lower()

        # ':asc' is redundant. Remove it (politely).

        field_code = re.sub( r':asc$', r'', field_code )

        if field_code not in allowed_sort_by_arguments:
            
            # Complain if we receive any unexpected sort_by directives.

            print( f"columns(): ERROR: '{field_code}' is not a valid directive for the 'sort_by' parameter. Please use one of [ '" + "', '".join( allowed_sort_by_arguments ) + "' ] instead.", file=sys.stderr )

            return

        code_basename = field_code

        if re.search( r':desc$', field_code ) is not None:
            
            code_basename = re.sub( r':desc$', '', field_code )

        if code_basename not in seen_so_far:
            
            seen_so_far[code_basename] = field_code

        else:
            
            # Complain if we receive multiple sort_by directives for the same output column.

            print( f"columns(): ERROR: Multiple sort_by directives received for the same output column, including '{seen_so_far[code_basename]}' and '{field_code}': please specify only one directive per output column.", file=sys.stderr )

            return

        if re.search( r':desc$', field_code ) is not None:
            
            by_list.append( code_basename )

            ascending_list.append( False )

        else:
            
            by_list.append( field_code )

            ascending_list.append( True )

    if debug == True:
        
        # Report details of the final parsed sort logic.

        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: columns(): Processed sort directives", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

        sort_dataframe = pd.DataFrame(
            
            {
                'sort_by': by_list,
                'ascending?': ascending_list
            }
        )

        print( sort_dataframe, end='\n\n', file=sys.stderr )

        print( '-' * 80, file=sys.stderr )

        print( f"END   DEBUG MESSAGE: columns(): Processed sort directives", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

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

    for filter_argument_name in filter_arguments:
        
        if filter_argument_name not in allowed_filter_arguments:
            
            # Complain if we receive any unexpected filter arguments.

            print( f"columns(): ERROR: Received unexpected argument {filter_argument_name}; aborting.", file=sys.stderr )

            return

        elif filter_argument_name == 'nullable':
            
            if not isinstance( filter_arguments[filter_argument_name], bool ):
                
                # Complain if we got a parameter value of the wrong data type.

                print( f"columns(): ERROR: 'nullable' must be a Boolean value (True or False); you used '{filter_arguments[filter_argument_name]}', which is not.", file=sys.stderr )

                return

        elif not ( isinstance( filter_arguments[filter_argument_name], str ) or isinstance( filter_arguments[filter_argument_name], list ) ):
            
            # Complain if we got a parameter value of the wrong data type.

            print( f"columns(): ERROR: '{filter_argument_name}' must be a string or a list of strings; you used '{filter_arguments[filter_argument_name]}', which is neither.", file=sys.stderr )

            return

        elif isinstance( filter_arguments[filter_argument_name], list ):
            
            for pattern in filter_arguments[filter_argument_name]:
                
                if not isinstance( pattern, str ):
                    
                    # Complain if we receive any unexpected data types inside a filter list (i.e. anything but strings).

                    print( f"columns(): ERROR: '{filter_argument_name}' must be a string or a list of strings; you used '{filter_arguments[filter_argument_name]}', which is neither.", file=sys.stderr )

                    return

        # Validate 


    if debug == True:
        
        # Report details of fully processed user directives prior to querying.

        status_report = f"return_data_as='{return_data_as}', output_file='{output_file}'"

        status_report = status_report + f", sort_by={sort_by}"

        for filter_argument_name in filter_arguments:
            
            if isinstance( filter_arguments[filter_argument_name], str ):
                
                status_report = status_report + f", {filter_argument_name}='{filter_arguments[filter_argument_name]}'"

            else:
                
                status_report = status_report + f", {filter_argument_name}={filter_arguments[filter_argument_name]}"

        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: columns(): Processed filter directives; summary", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

        print( 'running data_exploration.py columns( ' + status_report + ' )', end='\n\n', file=sys.stderr )

        print( '-' * 80, file=sys.stderr )

        print( f"END   DEBUG MESSAGE: columns(): Processed filter directives; summary", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    #############################################################################################################################
    # Fetch data from the API.

    # Make an ApiClient object containing the information necessary to connect to the CDA database.

    # Allow users to override the system-default URL for the CDA API by setting their CDA_API_URL
    # environment variable.


    # Make a QueryApi object using the connection information in the ApiClient object.

    url = "http://localhost:8000"
    url_override = os.environ.get( 'CDA_API_URL' )
    if url_override is not None and len( url_override ) > 0:
        url = url_override
    configuration = openapi_client.Configuration(host = url)

    # Enter a context with an instance of the API client
    api_client = openapi_client.ApiClient(configuration)
    query_api_instance = openapi_client.ColumnsApi(api_client)

    #try:
        # Columns Endpoint
    columns_response_data_object = query_api_instance.columns_endpoint_columns_post()

    #except openapi_client.ApiException as e:
    #    print("Exception when calling ColumnsApi->columns_endpoint_columns_post: %s\n" % e)


    #query_api_instance = QueryApi( api_client_instance )

    # Use the QueryApi instance object's `columns` endpoint-accessor
    # function to get data from the REST API.

    #columns_response_data_object = query_api_instance.columns( async_req=True )

    # Gracefully fetch asynchronously-generated results once they're ready.





    #############################################################################################################################
    # Postprocess API result data.

    # columns_response_data_object['result'] is an array of dicts, with
    # each dict containing a few named fields of metadata describing one column.
    # 
    # API terminology translation (API property name -> local convention):
    #     
    #     fieldName -> column name
    #     endpoint  -> table
    # 
    # Example:
    # 
    #   "result": [
    #       {
    #           "fieldName": "days_to_treatment_start",
    #           "endpoint": "treatment",
    #           "description": "The timepoint at which the treatment started.",
    #           "type": "integer",
    #           "isNullable": true
    #       },
    #       ...
    #   ]

    # Make a DataFrame from this array of dicts using DataFrame.from_records(), and explicitly specify the
    # column ordering for the resulting DataFrame using the `columns=[]` parameter.

    #result_dataframe = pd.DataFrame.from_records(data = [{'table': 'subject', 'column':'sex', 'data_type':'text', 'nullable': False, 'description':'boringdesc'}])
    result_dataframe = pd.DataFrame.from_records( columns_response_data_object.to_dict()['result'])


    # Filter banned columns.

    for banned_column in banned_columns:
        
        result_dataframe = result_dataframe.loc[ result_dataframe['column'] != banned_column ]

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( '      DEBUG MESSAGE: columns(): Created result DataFrame', file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    #############################################################################################################################
    # Execute sorting directives, if we got any; otherwise perform the default sort on the result DataFrame.

    if len( sort_by ) == 0:
        
        # By default, we sort column records by column name, gathered into groups by table,
        # to facilitate predictable output patterns. For easy access, we'd also like each
        # ID column to show up first in its table's group.
        # 
        # Temporarily prepend a '.' to all *_id column names, so they float to the top of each
        # table's list of columns when we sort.

        result_dataframe = result_dataframe.replace( to_replace=r'(.*_id)$', value=r'.\1', regex=True )

        # Sort all column records, first on table and then on column name.

        result_dataframe = result_dataframe.sort_values( by=[ 'table', 'column' ], ascending=[ True, True ] )

        # Remove the '.' characters we temporarily prepended to *_id column names
        # to force the sorting algorithm to place all such columns first within each
        # table's group of column records.

        result_dataframe = result_dataframe.replace( to_replace=r'^\.(.*_id)$', value=r'\1', regex=True )

    else:
        
        # Sort all column records according to the user-specified directives we've processed.

        result_dataframe = result_dataframe.sort_values( by=by_list, ascending=ascending_list )

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( '      DEBUG MESSAGE: columns(): Applied sort_by directives', file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

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
        # _not_ match any of the specified filters.

        if filter_name == 'nullable':
            
            return_if_nullable = filter_arguments[filter_name]

            if not isinstance( return_if_nullable, bool ):
                
                print( f"columns(): ERROR: Please specify either nullable=True or nullable=False, not (what you sent) nullable='{return_if_nullable}'.", file=sys.stderr )

                return

            result_dataframe = result_dataframe.loc[ result_dataframe['nullable'] == return_if_nullable ]

        else:
            
            filters = filter_arguments[filter_name]

            filter_patterns = list()

            # If the filter list wasn't a list at all but a (nonempty) string, we just have
            # one filter. Listify it (so we don't have to care downstream about how many there are).

            if isinstance( filters, str ) and filters != '':
                
                filter_patterns = [ filters ]

            # Otherwise, just start with the list they sent us.

            elif isinstance( filters, list ):
                
                filter_patterns = filters

            # (If neither of the above conditions was met, `filter_patterns` will remain an
            # empty list, and the rest of this filter-processing section will (by design) have no effect.

            target_field = filter_name

            if filter_name == 'description':
                
                updated_pattern_list = list()

                for original_filter_pattern in filter_patterns:
                    
                    updated_filter_pattern = f'*{original_filter_pattern}*'

                    updated_pattern_list.append( updated_filter_pattern )

                filter_patterns = updated_pattern_list

                target_field = 'description'

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

            # Strip trailing |.

            match_pattern_string = re.sub( r'\|$', r'', match_pattern_string )

            if filter_name == 'exclude_table':
                
                # Retain all rows where the value of `target_field` (in this case, the value of `table`) does _not_ match any of the given filter patterns.

                result_dataframe = result_dataframe.loc[ ~( result_dataframe[target_field].str.contains( match_pattern_string, case=False ) ) ]

            else:
                
                # Retain all rows where the value of `target_field` matches any of the given filter patterns.

                result_dataframe = result_dataframe.loc[ result_dataframe[target_field].str.contains( match_pattern_string, case=False ) ]

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( '      DEBUG MESSAGE: columns(): Applied value-filtration directives', file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

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

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( '      DEBUG MESSAGE: columns(): Returning results in default form (pandas.DataFrame)', file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        return result_dataframe

    elif return_data_as == 'dataframe':
        
        # Give the user back the results DataFrame.

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( '      DEBUG MESSAGE: columns(): Returning results as pandas.DataFrame', file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        return result_dataframe

    elif return_data_as == 'list':
        
        # Give the user back a list of column names.

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( '      DEBUG MESSAGE: columns(): Returning results as list of column names', file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        return result_dataframe['column'].to_list()

    else:
        
        # Write the results DataFrame to a user-specified TSV file.

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( f"      DEBUG MESSAGE: columns(): Printing results to TSV file '{output_file}'", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        try:
            
            result_dataframe.to_csv( output_file, sep='\t', index=False )

            return

        except Exception as error:
            
            print( f"columns(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'.", file=sys.stderr )

            return

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
    column = '',
    *,
    return_data_as = '',
    output_file = '',
    sort_by = '',
    filters = None,
    data_source = '',
    force = False,
    debug = False
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
            valid values are 'GDC', 'IDC', 'PDC', 'CDS' and 'ICDC'.
            Defaults to '' (no filter).

        force( boolean; optional ):
            Force execution of high-overhead queries on columns (like IDs)
            flagged as having large numbers of values. Defaults to False,
            in which case attempts to retrieve values for flagged columns
            will result in a warning.

        debug( boolean; optional ): If set to True, print internal process
        details to the standard error stream.

    Returns:
        pandas.DataFrame OR list OR returns nothing, but writes retrieved
        data to a user-specified TSV file
    """

    #############################################################################################################################
    # Ensure nothing untoward got passed into the `debug` or `force` parameters.

    if debug != True and debug != False:
        
        print( f"column_values(): ERROR: The `debug` parameter must be set to True or False; you specified '{debug}', which is neither.", file=sys.stderr )

        return

    if force != True and force != False:
        
        print( f"column_values(): ERROR: The `force` parameter must be set to True or False; you specified '{force}', which is neither.", file=sys.stderr )

        return

    #############################################################################################################################
    # Check for our one required parameter.

    if ( not isinstance( column, str ) ) or column == '':
        
        print( f"column_values(): ERROR: parameter 'column' cannot be omitted. Please specify a column from which to fetch a list of distinct values.", file=sys.stderr )

        return

    # If there's whitespace in our column name, remove it before it does any damage.

    column = re.sub( r'\s+', r'', column )

    # Let's not care about case.

    column = column.lower()

    # See if columns() agrees that the requested column exists.

    if len( columns( column=column, return_data_as='list' ) ) == 0:
        
        print( f"column_values(): ERROR: parameter 'column' must be a searchable CDA column name. You supplied '{column}', which is not.", file=sys.stderr )

        return

    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which describes user-specified filtration on upstream data
    # sources.

    if not isinstance( data_source, str ):
        
        print( f"column_values(): ERROR: value assigned to 'data_source' parameter must be a string (e.g. 'GDC'); you specified '{data_source}', which is not.", file=sys.stderr )

        return

    # TEMPORARY: enumerate valid `data_source` values and warn the user if they supplied something else.
    # At time of writing this is too expensive to retrieve dynamically from the API,
    # so the valid value list is hard-coded here and in the docstring for this function.
    # 
    # This should be replaced ASAP with a fetch from a 'release metadata' table or something
    # similar.

    allowed_data_source_values = {
        
        'GDC',
        'PDC',
        'IDC',
        'CDS',
        'ICDC'
    }

    if data_source != '':
        
        # Let us not care about case, and remove any whitespace before it can do any damage.

        data_source = re.sub( r'\s+', r'', data_source ).upper()

        if data_source not in allowed_data_source_values:
            
            print( f"column_values(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{data_source}', which is not.", file=sys.stderr )

            return

    #############################################################################################################################
    # Check in advance for columns flagged as high-overhead.

    expensive_columns = {
        
        'file_id',
        'byte_size',
        'checksum',
        'drs_uri',
        'file_integer_id_alias',
        'label'
    }

    if not force and column in expensive_columns:
        
        print( f"column_values(): WARNING: '{column}' has a very large number of values; retrieval is blocked by default. To perform this query, use column_values( ..., 'force=True' ).", file=sys.stderr )

        return

    #############################################################################################################################
    # Listify `filters`, if it's a string, so we can process it in a uniform way later on.

    if filters is None:
        
        filters = list()

    elif isinstance( filters, str ):
        
        filters = [ filters ]

    #############################################################################################################################
    # Process return-type directives.

    allowed_return_types = {
        '',
        'dataframe',
        'tsv',
        'list'
    }

    if not isinstance( return_data_as, str ):
        
        print( f"column_values(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'.", file=sys.stderr )

        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance( output_file, str ):
        
        print( f"column_values(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path.", file=sys.stderr )

        return

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        
        print( f"column_values(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'.", file=sys.stderr )

        return

    elif return_data_as == 'tsv' and output_file == '':
        
        print( f"column_values(): ERROR: return type 'tsv' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/tsv/to'.", file=sys.stderr )

        return

    elif return_data_as != 'tsv' and output_file != '':
        
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        print( f"column_values(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'.", file=sys.stderr )
        print( f"(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.).", file=sys.stderr )

        return

    #############################################################################################################################
    # Process sorting directives.

    # Enumerate all allowed values that a user can specify using the `sort_by` parameter. ( 'X:asc' will be aliased immediately to just 'X'. )

    allowed_sort_by_options = {
        
        'list' : {
            '',
            'value',
            'value:desc'
        },
        'dataframe_or_tsv' : {
            '',
            'count',
            'count:desc',
            'value',
            'value:desc'
        }
    }

    if not isinstance( sort_by, str ):
        
        # Complain if we receive any unexpected data types instead of string directives.

        print( f"column_values(): ERROR: 'sort_by' must be a string; you used '{sort_by}', which is not.", file=sys.stderr )

        return

    # Let's not care about case.

    sort_by = sort_by.lower()

    # ':asc' is redundant. Remove it (politely).

    sort_by = re.sub( r':asc$', r'', sort_by )

    if return_data_as == 'list':
        
        # Restrict sorting options for lists.

        if sort_by == '':
            
            sort_by = 'value'

        elif sort_by not in allowed_sort_by_options['list']:
            
            print( f"column_values(): ERROR: return_data_as='list' can only be processed with sort_by='value' or sort_by='value:desc' (or omitting sort_by altogether). Please modify unsupported sort_by directive '{sort_by}' and try again.", file=sys.stderr )

            return

    else:
        
        # For TSV output files and DataFrames, we support more user-configurable options (defaulting to sort_by='count:desc'):

        if sort_by == '':
            
            sort_by = 'count:desc'

        elif sort_by not in allowed_sort_by_options['dataframe_or_tsv']:
            
            print( f"column_values(): ERROR: unrecognized sort_by '{sort_by}'. Please use one of 'count', 'value', 'count:desc', 'value:desc', 'count:asc' or 'value:asc' (or omit the sort_by parameter altogether).", file=sys.stderr )

            return

    if debug == True:
        
        # Report details of the final parsed sort logic.

        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: column_values(): Processed all parameter directives. Calling API to fetch data for:", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

        parameter_dict = {
            
            'column': column,
            'return_data_as': return_data_as,
            'output_file': output_file,
            'sort_by': sort_by,
            'filters': filters,
            'data_source': data_source,
            'force': force,
            'debug': debug
        }

        print( parameter_dict, end='\n\n', file=sys.stderr )

        print( '-' * 80, file=sys.stderr )

        print( f"END   DEBUG MESSAGE: column_values(): Processed sort directives", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    #############################################################################################################################
    # Fetch data from the API.

    # Make an ApiClient object containing the information necessary to connect to the CDA database.
    # 
    # Allow users to override the system-default URL for the CDA API by setting their CDA_API_URL
    # environment variable.

    url = "http://localhost:8000"
    url_override = os.environ.get( 'CDA_API_URL' )
    if url_override is not None and len( url_override ) > 0:
        url = url_override
    configuration = openapi_client.Configuration(host = url)


    api_client = openapi_client.ApiClient(configuration)
    # Create an instance of the API class
    query_api_instance = openapi_client.UniqueValuesApi(api_client)
    columnname = column # str | 
    system = data_source # str |  (optional) (default to '')
    count = False # bool |  (optional) (default to False)
    total_count = False # bool |  (optional) (default to False)
    records_per_page = 56 # int |  (optional)
    starting_offset = 56 # int |  (optional)

    #try:
        # Unique Values Endpoint
    paged_response_data_object = query_api_instance.unique_values_endpoint_unique_values_columnname_post(columnname, system=system, count=count, total_count=total_count, limit=records_per_page, offset=starting_offset)
    print("The response of UniqueValuesApi->unique_values_endpoint_unique_values_columnname_post:\n")
    print(str(paged_response_data_object))
    #except Exception as e:
    #    print("Exception when calling UniqueValuesApi->unique_values_endpoint_unique_values_columnname_post: %s\n" % e)

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: column_values(): Querying CDA API 'unique_values' endpoint", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

   
    if debug == True:
        
        # Report some metadata about the results we got back.
    
        print( f"Number of result rows: {paged_response_data_object.total_row_count}", file=sys.stderr )

        print( f"Query SQL: '{paged_response_data_object.query_sql}'", end='\n\n', file=sys.stderr )

    # Make a Pandas DataFrame out of the first batch of results.
    # 
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function.

    result_dataframe = pd.json_normalize( paged_response_data_object.result )

    # The data we've fetched so far might be just the first page (if the total number
    # of results is greater than `records_per_page`).
    # 
    # Get the rest of the result pages, if there are any, and add each page's data
    # onto the end of our results DataFrame.

    incremented_offset = starting_offset + records_per_page

    more_than_one_result_page = False

    
    if debug == True:
        
        if more_than_one_result_page:
            
            print( '...done.', end='\n\n', file=sys.stderr )

        print( '-' * 80, file=sys.stderr )

        print( f"END   DEBUG MESSAGE: column_values(): Queried CDA API 'unique_values' endpoint and created result DataFrame", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    #############################################################################################################################
    # Postprocess API result data, if there is any.

    if len( result_dataframe ) == 0:
        
        return result_dataframe

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( 'BEGIN DEBUG MESSAGE: column_values(): Postprocessing results', file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

        print( 'Casting counts to integers and fixing symmetry for returned column labels...', file=sys.stderr )

    # Term-count values come in as floats. Make them not that.

    result_dataframe['count'] = result_dataframe['count'].astype( int )

    # `X_id` columns come back labeled just as `id`. Fix.

    if re.search( r'_id$', column ) is not None:
        
        result_dataframe = result_dataframe.rename( columns = { 'id': column } )

    # `X_integer_id_alias` columns come back labeled just as `integer_id_alias`. Fix.

    elif re.search( r'_integer_id_alias$', column ) is not None:
        
        result_dataframe = result_dataframe.rename( columns = { 'integer_id_alias': column } )

    # `X_associated_project` columns come back labeled just as `associated_project`. Fix.

    elif re.search( r'_associated_project$', column ) is not None:
        
        result_dataframe = result_dataframe.rename( columns = { 'associated_project': column } )

    # `X_identifier_Y` columns come back labeled just as `Y`. Fix.

    elif re.search( r'^(.*_identifier_)(.+)$', column ) is not None:
        
        suffix = re.sub( r'^.*_identifier_(.+)$', r'\1', column )

        # Adjust the header the API sent us for the values column.

        result_dataframe = result_dataframe.rename( columns = { suffix: column } )

    if debug == True:
        
        print( 'Handling missing values...', file=sys.stderr )

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

        print( f"column_values(): ERROR: Unexpected data type `{result_dataframe[column].dtype}` received; aborting. Please report this event to the CDA development team.", file=sys.stderr )

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

    # Strip the trailing '|' character from the end of the last `filter_pattern`.

    match_pattern_string = re.sub( r'\|$', r'', match_pattern_string )

    if debug == True:
        
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

        print( f"Applying pattern filters: {print_regex}", file=sys.stderr )

    # Filter results to match the full aggregated regular expression in `match_pattern_string`.

    if include_null_count and match_pattern_string != '':
        
        result_dataframe = result_dataframe.loc[ result_dataframe[column].astype(str).str.contains( match_pattern_string, case=False ) | result_dataframe[column].astype(str).str.contains( r'^$' ) | result_dataframe[column].isna() ]

    elif include_null_count:
        
        result_dataframe = result_dataframe.loc[ result_dataframe[column].astype(str).str.contains( r'^$' ) | result_dataframe[column].isna() ]

    else:
        
        # This will return unfiltered results if `match_pattern_string` is empty (i.e. if the user asked for no filters to be applied),
        # and will filter results according to `match_pattern_string` if not.

        result_dataframe = result_dataframe.loc[ result_dataframe[column].astype(str).str.contains( match_pattern_string, case=False ) ]

    # Sort results. Default (note that the final value of `sort_by` is determined earlier in this function) is to sort by term count, descending.

    if debug == True:
        
        print( f"Applying sort directive '{sort_by}'...", end='\n\n', file=sys.stderr )

    if sort_by == 'count':
        
        # Sort by count; break ties among groups of values with identical counts by sub-sorting each such group alphabetically by value.

        result_dataframe = result_dataframe.sort_values( by=[ 'count', column ], ascending=[ True, True ] )

    elif sort_by == 'count:desc':
        
        # Sort by count, descending; break ties among groups of values with identical counts by sub-sorting each such group alphabetically by value.

        result_dataframe = result_dataframe.sort_values( by=[ 'count', column ], ascending=[ False, True ] )

    elif sort_by == 'value':
        
        # No need for a sub-sort, here, since values aren't repeated.

        result_dataframe = result_dataframe.sort_values( by=column, ascending=True )

    elif sort_by == 'value:desc':
        
        # No need for a sub-sort, here, since values aren't repeated.

        result_dataframe = result_dataframe.sort_values( by=column, ascending=False )

    else:
        
        print( f"column_values(): ERROR: something has gone horribly wrong; we should never get here.", file=sys.stderr )

        return

    if debug == True:

        print( '-' * 80, file=sys.stderr )

        print( 'END   DEBUG MESSAGE: column_values(): Postprocessed results', file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

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

        # Replace term values that are None (== null) with empty strings.

        result_dataframe = result_dataframe.replace( r'^$', r'<NA>', regex=True )

    elif result_dataframe[column].dtype == 'bool':
        
        result_dataframe = result_dataframe.replace( r'^$', r'<NA>', regex=True )

    if return_data_as == '':
        
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        # The following, for the dubiously useful record, is a somewhat worse alternative default thing to do.
        # 
        # print( result_dataframe.to_string( index=False, justify='right', max_rows=25, max_colwidth=50 ), file=sys.stdout )

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( '      DEBUG MESSAGE: column_values(): Returning results in default form (pandas.DataFrame)', file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        return result_dataframe

    elif return_data_as == 'dataframe':
        
        # Give the user back the results DataFrame.

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( '      DEBUG MESSAGE: column_values(): Returning results as pandas.DataFrame', file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        return result_dataframe

    elif return_data_as == 'list':
        
        # Strip the term-values column out of the results DataFrame and give them to the user as a Python list.

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( '      DEBUG MESSAGE: column_values(): Returning results as list of column values', file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        return result_dataframe[column].to_list()

    else:
        
        # Write the results DataFrame to a user-specified TSV file.

        if debug == True:
            
            print( '-' * 80, file=sys.stderr )

            print( f"      DEBUG MESSAGE: column_values(): Printing results to TSV file '{output_file}'", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        try:
            
            result_dataframe.to_csv( output_file, sep='\t', index=False )

            return

        except Exception as error:
            
            print( f"column_values(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'.", file=sys.stderr )

            return

#############################################################################################################################
# 
# END column_values() 
# 
#############################################################################################################################
