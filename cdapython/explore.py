import json
import pandas as pd
import os
import re
import sys

import tabulate
from cda_client_new import openapi_client
from cda_client_new.openapi_client.models import ColumnResponseObj
from cda_client_new.openapi_client import ApiException

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