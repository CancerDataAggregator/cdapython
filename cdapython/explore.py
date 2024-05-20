import json
import pandas as pd
import os
import re
import sys

import tabulate

from cda_client.api_client import ApiClient
from cda_client.api.query_api import QueryApi

from cda_client.exceptions import ApiException

from cda_client.model.columns_response_data import ColumnsResponseData
from cda_client.model.paged_response_data import PagedResponseData
from cda_client.model.query import Query

# MAYBE TO DO: Eliminate this and just use the Configuration base class instead.
from cdapython.cda_configuration import CdaConfiguration

from multiprocessing.pool import ApplyResult

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

    url_override = os.environ.get( 'CDA_API_URL' )

    if url_override is not None and len( url_override ) > 0:
        
        api_configuration = CdaConfiguration( host=url_override, verify=True, verbose=True )

        api_client_instance = ApiClient( configuration=api_configuration )

        if debug == True:
            
            # Report that we're pulling in a hostname from the CDA_API_URL environment variable.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: columns(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: columns(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    else:
        
        api_configuration = CdaConfiguration( verify=True, verbose=True )

        api_client_instance = ApiClient( configuration=api_configuration )

        if debug == True:
            
            # Report the default location data for the CDA API, as loaded from the CdaConfiguration class.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: columns(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: columns(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: columns(): Querying CDA API 'columns' endpoint", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    # Make a QueryApi object using the connection information in the ApiClient object.

    query_api_instance = QueryApi( api_client_instance )

    # Use the QueryApi instance object's `columns` endpoint-accessor
    # function to get data from the REST API.

    columns_response_data_object = query_api_instance.columns( async_req=True )

    # Gracefully fetch asynchronously-generated results once they're ready.

    if isinstance( columns_response_data_object, ApplyResult ):
        
        while columns_response_data_object.ready() is False:
            
            columns_response_data_object.wait( 5 )

        try:

            columns_response_data_object = columns_response_data_object.get()

        except ApiException as e:
            
            if e.body is not None:
                
                # Ordinarily, this exception represents a structured complaint
                # from the API service that something went wrong. In this case,
                # the `body` property of the ApiException object will contain
                # a JSON-encoded message generated by the API describing the
                # unfortunate circumstance.

                error_message = json.loads( e.body )['message']

            else:
                
                # Unfortunately, if something goes wrong at the level of the
                # HTTP service on which the API relies -- that is, when we
                # can't actually communicate with the API as such because
                # something's gone wrong with our ability to talk to the web
                # server -- the ApiException class is overloaded to encode
                # that HTTP protocol error (and not throw any further exceptions),
                # instead of handling such events somewhere more appropriate
                # (like via a different exception class altogether).

                error_message = str( e )

            print( f"columns(): ERROR: error message from API: '{error_message}'.", file=sys.stderr )

            return

        except BaseException as e:
            
            if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:
                
                print( "columns(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

            else:
                
                print( f"columns(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

            return

    if debug == True:
        
        print( f"Query complete: initial (unfiltered) result set contains records describing { len( columns_response_data_object['result'] ) } columns.", end='\n\n', file=sys.stderr )

        print( '-' * 80, file=sys.stderr )

        print( f"END   DEBUG MESSAGE: columns(): Queried CDA API 'columns' endpoint", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

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

    result_dataframe = pd.DataFrame.from_records( columns_response_data_object['result'], columns=[ 'endpoint', 'fieldName', 'type', 'isNullable', 'description' ] )

    # Standardize user-facing terminology.

    rename_dataframe_columns = {
        
        'endpoint': 'table',
        'fieldName': 'column',
        'type': 'data_type',
        'isNullable': 'nullable',
        'description': 'description'
    }

    result_dataframe = result_dataframe.rename( columns=rename_dataframe_columns )

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
    filters = '',
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
            will allow partial matches. Case will be ignored. Use an empty
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

    if isinstance( filters, str ):
        
        if filters == '':
            
            filters = list()

        else:
            
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

    url_override = os.environ.get( 'CDA_API_URL' )

    if url_override is not None and len( url_override ) > 0:
        
        api_configuration = CdaConfiguration( host=url_override, verify=True, verbose=True )

        api_client_instance = ApiClient( configuration=api_configuration )

        if debug == True:
            
            # Report that we're pulling in a hostname from the CDA_API_URL environment variable.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: column_values(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: column_values(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    else:
        
        api_configuration = CdaConfiguration( verify=True, verbose=True )

        api_client_instance = ApiClient( configuration=api_configuration )

        if debug == True:
            
            # Report the default location data for the CDA API, as loaded from the CdaConfiguration class.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: column_values(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: column_values(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: column_values(): Querying CDA API 'unique_values' endpoint", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    # Make a QueryApi object using the connection information in the ApiClient object.

    query_api_instance = QueryApi( api_client_instance )

    # We're always returning all results to users. Paging occurs, but is transparent to the user.
    # These two variables are coded according to CDA performance needs. They should ultimately be
    # moved to a central system-parameter store for easier access: right now, they're
    # replicated everywhere a fetch is performed, which is error-prone when it comes to
    # long-term maintenance.

    starting_offset = 0

    records_per_page = 50000

    # Use the QueryApi instance object's `unique_values` endpoint-accessor
    # function to get data from the REST API.

    paged_response_data_object = query_api_instance.unique_values(
        body=column,
        system=data_source,
        count=True,
        async_req=True,
        offset=starting_offset,
        limit=records_per_page,
        include_count=True
    )

    # Gracefully fetch asynchronously-generated results once they're ready.

    if isinstance( paged_response_data_object, ApplyResult ):
        
        while paged_response_data_object.ready() is False:
            
            paged_response_data_object.wait( 5 )

        try:
            
            paged_response_data_object = paged_response_data_object.get()

        except ApiException as e:
            
            if e.body is not None:
                
                # Ordinarily, this exception represents a structured complaint
                # from the API service that something went wrong. In this case,
                # the `body` property of the ApiException object will contain
                # a JSON-encoded message generated by the API describing the
                # unfortunate circumstance.

                error_message = json.loads( e.body )['message']

            else:
                
                # Unfortunately, if something goes wrong at the level of the
                # HTTP service on which the API relies -- that is, when we
                # can't actually communicate with the API as such because
                # something's gone wrong with our ability to talk to the web
                # server -- the ApiException class is overloaded to encode
                # that HTTP protocol error (and not throw any further exceptions),
                # instead of handling such events somewhere more appropriate
                # (like via a different exception class altogether).

                error_message = str( e )

            print( f"column_values(): ERROR: error message from API: '{error_message}'.", file=sys.stderr )

            return

        except BaseException as e:
            
            if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:
                
                print( "column_values(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

            else:
                
                print( f"column_values(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

            return

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

    if debug == True and paged_response_data_object['next_url'] is not None:
        
        print( f"Fetching remaining results in pages...", file=sys.stderr )

        more_than_one_result_page = True

    while paged_response_data_object['next_url'] is not None:
        
        if debug == True:
            
            # Show the `next_url` address returned to us by the API.

            print( f"   ...fetching {paged_response_data_object['next_url']}...", file=sys.stderr )

        # Note that the API doesn't preserve all the query parameters we included
        # in our original request, e.g.:
        # 
        # (original request)
        #     http://localhost:8080/api/v1/unique-values?count=true&includeCount=true&offset=0&limit=100
        # 
        # vs
        # 
        # (the `next_url` value in the response to the above)
        #     http://localhost:8080/api/v1/unique-values?offset=100&limit=100
        # 
        # ...so we have to put the lost parameters back, in the form of arguments to
        # the `unique_values` endpoint call just below. Note that we're not actually using
        # the `next_url` value in the following call, because it's incomplete. We're just
        # checking to see if it exists (in the while-loop condition governing this block),
        # so we can determine whether or not to continue fetching more pages:

        paged_response_data_object = query_api_instance.unique_values(
            body=column,
            system=data_source,
            count=True,
            async_req=True,
            offset=incremented_offset,
            limit=records_per_page,
            include_count=True
        )

        if isinstance( paged_response_data_object, ApplyResult ):
            
            while paged_response_data_object.ready() is False:
                
                paged_response_data_object.wait( 5 )

            try:
                
                paged_response_data_object = paged_response_data_object.get()

            except ApiException as e:
                
                if e.body is not None:
                    
                    # Ordinarily, this exception represents a structured complaint
                    # from the API service that something went wrong. In this case,
                    # the `body` property of the ApiException object will contain
                    # a JSON-encoded message generated by the API describing the
                    # unfortunate circumstance.

                    error_message = json.loads( e.body )['message']

                else:
                    
                    # Unfortunately, if something goes wrong at the level of the
                    # HTTP service on which the API relies -- that is, when we
                    # can't actually communicate with the API as such because
                    # something's gone wrong with our ability to talk to the web
                    # server -- the ApiException class is overloaded to encode
                    # that HTTP protocol error (and not throw any further exceptions),
                    # instead of handling such events somewhere more appropriate
                    # (like via a different exception class altogether).

                    error_message = str( e )

                print( f"column_values(): ERROR: error message from API: '{error_message}'.", file=sys.stderr )

                return

            except BaseException as e:
                
                if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:
                    
                    print( "column_values(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

                else:
                    
                    print( f"column_values(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

                return

        next_result_batch = pd.json_normalize( paged_response_data_object.result )

        if not result_dataframe.empty and not next_result_batch.empty:
            
            # Silence a future deprecation warning about pd.concat and empty DataFrame columns.
            
            next_result_batch = next_result_batch.astype( result_dataframe.dtypes )

            result_dataframe = pd.concat( [ result_dataframe, next_result_batch ] )

        incremented_offset = incremented_offset + records_per_page

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

#############################################################################################################################
# 
# summary_counts(): For a set of rows in a user-specified table that all match a user-specified set of filters, get
#                   a report showing counts of values present in that set of rows, profiled across a small set of
#                   pre-selected columns.
# 
#############################################################################################################################

def summary_counts(
    table = '',
    *,
    return_data_as = '',
    output_file = '',
    match_all = [],
    match_any = [],
    data_source = [],
    debug = False
):
    """
    For a set of rows in a user-specified table that all match a user-specified set of filters, get
    a report showing counts of values present in that set of rows, profiled across a small set of
    pre-selected columns.

    Arguments:
        table ( string; required ):
            The table whose rows are to be filtered and counted. (Run the tables()
            function to get a list.)

        return_data_as ( string; optional: 'dataframe_list' or 'dict' or 'json' ):
            Specify how summary_counts() should return results: as a list
            of pandas DataFrames, as a Python dictionary, or as output written to a
            JSON file named by the user.  If this argument is omitted,
            summary_counts() will, for each DataFrame that would have been returned
            by the 'dataframe_list' option, print a table to the standard output
            stream (and nothing will be returned).

        output_file( string; optional ):
            If return_data_as='json' is specified, output_file should contain a
            resolvable path to a file into which summary_counts() will write
            JSON-formatted results.

        match_all ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            ALL of which must be met by all result rows.

        match_any ( string or list of strings; optional ):
            One or more conditions, expressed as filter strings (see below),
            AT LEAST ONE of which must be met by all result rows.

        data_source ( string or list of strings; optional ):
            Restrict results to those deriving from the given upstream
            data source(s). Current valid values are 'GDC', 'IDC', 'PDC',
            'CDS' and 'ICDC'. (Default: no filter.)

        debug ( boolean; optional ):
            If set to True, internal process details will be printed to the standard error
            stream as summary_counts() is running. If False (the default), ...they won't.

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
            
            summary_counts( table='subject', match_all=[ 'primary_disease_type = *duct*', 'sex = F*' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get a count summary for rows where the `sex` field is missing data,
        we can write:
            
            summary_counts( table='subject', match_all=[ 'sex = NULL' ] )

    Returns:
        
        list of pandas DataFrames, with one DataFrame for each of a small set of
        pre-selected columns, enumerating counts of all of that column's data values
        appearing in any of the rows of the user-specified `table` that match the
        user-specified filter critera (the 'result rows'). One or two DataFrames
        in this list -- titled 'total_`table`_matches' and sometimes also
        'total_related_files', where appropriate -- will contain integers representing
        the number of result rows and the number of files related to those rows,
        respectively. All other DataFrames in the list will each be titled with
        a CDA column name and contain counts for all observed values from that
        column in the result row set.

        OR Python dictionary enumerating counts of all data values (from a small set of pre-selected columns)
        appearing in any of the rows of the user-specified `table` that match the user-specified filter criteria
        (the 'result rows'). One or two summary keys in this dictionary -- 'total_`table`_matches', and
        sometimes 'total_related_files', where appropriate -- will point to integers representing
        the number of result rows and the number of files related to those rows, respectively. All other keys
        in the dictionary will each contain a CDA column name; each corresponding value will itself be a
        dictionary enumerating all the specific values appearing in the result rows for the CDA column
        named in the key. Each value in that (sub-)dictionary will represent the total number of times
        that its corresponding key appears in the result rows.

        OR JSON-formatted text representing the same structure as the `return_data_as='dict'`
        option, written to `output_file`.

        OR returns nothing, but displays a series of tables to standard output
        describing the same data returned by the other `return_data_as` options.

        And yes, we know how those first two paragraphs look. We apologize to the entire English language.
    """

    #############################################################################################################################
    # Ensure nothing untoward got passed into the `debug` parameter.

    if debug != True and debug != False:
        
        print( f"summary_counts(): ERROR: The `debug` parameter must be set to True or False; you specified '{debug}', which is neither.", file=sys.stderr )

        return

    #############################################################################################################################
    # Ensure our one required argument exists and is a valid table name.

    if not isinstance( table, str ) or table == '':
        
        print( f"summary_counts(): ERROR: parameter 'table' is required and must be a nonempty string; you supplied '{table}', which is not.", file=sys.stderr )

        return

    valid_tables = tables()

    if table not in valid_tables:
        
        print( f"summary_counts(): ERROR: parameter 'table' must be a searchable CDA table; you supplied '{table}', which is not.", file=sys.stderr )

        return

    #############################################################################################################################
    # Process return-type directives `return_data_as` and `output_file`.

    allowed_return_types = {
        '',
        'dataframe_list',
        'dict',
        'json'
    }

    if not isinstance( return_data_as, str ):
        
        print( f"summary_counts(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe_list', 'dict' or 'json' (or omit the 'return_data_as' parameter altogether).", file=sys.stderr )

        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame_LIsT' or return_data_as='JSON'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance( output_file, str ):
        
        print( f"summary_counts(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path.", file=sys.stderr )

        return

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        
        # Complain if we receive an unexpected `return_data_as` value.

        print( f"summary_counts(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe_list', 'dict' or 'json' (or omit the 'return_data_as' parameter altogether).", file=sys.stderr )

        return

    elif return_data_as == 'json' and output_file == '':
        
        # If the user asks for JSON, they also have to give us a path for the output file. If they didn't, complain.

        print( f"summary_counts(): ERROR: return type 'json' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/json/to'.", file=sys.stderr )

        return

    elif return_data_as != 'json' and output_file != '':
        
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='json'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        print( f"summary_counts(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'json'. You requested return_data_as='{return_data_as}'.", file=sys.stderr )
        print( f"(Note that if you don't specify any value for 'return_data_as', it defaults to printing tables to the standard output stream and not to an output file.).", file=sys.stderr )

        return

    #############################################################################################################################
    # Define the list of supported filter-string operators.

    allowed_operators = {
        
        '>',
        '>=',
        '<',
        '<=',
        '=',
        '!='
    }

    #############################################################################################################################
    # Enumerate restrictions on operator use to appropriate data types.

    operators_by_data_type = {
        
        'bigint': allowed_operators,
        'boolean': { '=', '!=' },
        'integer': allowed_operators,
        'numeric': allowed_operators,
        'text': { '=', '!=' }
    }

    #############################################################################################################################
    # Enable aliases for various ways to say "True" and "False". (Case will be lowered as soon as each literal is received.)

    boolean_alias = {
        
        'true': 'true',
        't': 'true',
        'false': 'false',
        'f': 'false'
    }

    #############################################################################################################################
    # Manage basic validation for the `match_all` parameter, which enumerates user-specified requirements that returned
    # records must all be simultaneously satisfied (AND; intersection; 'all of these must apply').

    if isinstance( match_all, str ):
        
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        match_all = [ match_all ]

    if not isinstance( match_all, list ):
        
        print( f"summary_counts(): ERROR: value assigned to 'match_all' parameter must be a filter string or a list of filter strings; you specified '{match_all}', which is neither.", file=sys.stderr )

        return

    for item in match_all:

        if not isinstance( item, str ) or len( item ) == 0:
            
            print( f"summary_counts(): ERROR: value assigned to 'match_all' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_all}', which is neither.", file=sys.stderr )

            return

        # Check overall format.

        if re.search( r'^\S+\s+\S+\s+\S.*$', item ) is None:
            
            print( f"summary_counts(): ERROR: match_all: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.", file=sys.stderr )

            return

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned records must satisfy at least one (OR; union; 'at least one of these must apply').

    if isinstance( match_any, str ):
        
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        match_any = [ match_any ]

    if not isinstance( match_any, list ):
        
        print( f"summary_counts(): ERROR: value assigned to 'match_any' parameter must be a filter string or a list of filter strings; you specified '{match_any}', which is neither.", file=sys.stderr )

        return

    for item in match_any:
        
        if not isinstance( item, str ) or len( item ) == 0:
            
            print( f"summary_counts(): ERROR: value assigned to 'match_any' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_any}', which is neither.", file=sys.stderr )

            return

        # Check overall format.

        if re.search( r'^\S+\s+\S+\s+\S.*$', item ) is None:
            
            print( f"summary_counts(): ERROR: match_any: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.", file=sys.stderr )

            return

    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which enumerates user-specified filters on upstream data
    # sources.

    if isinstance( data_source, str ):
        
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        data_source = [ data_source ]

    elif not isinstance( data_source, list ):
        
        print( f"summary_counts(): ERROR: value assigned to the 'data_source' parameter must be a string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.", file=sys.stderr )

        return

    for item in data_source:
        
        if not isinstance( item, str ) or len( item ) == 0:
            
            print( f"summary_counts(): ERROR: value assigned to the 'data_source' parameter must be a nonempty string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.", file=sys.stderr )

            return

    # Let us not care about case, and remove any whitespace before it can do any damage.

    data_source = [ re.sub( r'\s+', r'', item ).lower() for item in data_source ]

    # TEMPORARY: enumerate valid values and warn the user if they supplied something else.
    # At time of writing this is too expensive to retrieve dynamically from the API,
    # so the valid value list is hard-coded here and in the docstring for this function.
    # 
    # This should be replaced ASAP with a fetch from a 'release metadata' table or something
    # similar.

    allowed_data_source_values = {
        
        'gdc',
        'pdc',
        'idc',
        'cds',
        'icdc'
    }

    for item in data_source:
        
        if item not in allowed_data_source_values:
            
            print( f"summary_counts(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{item}', which is not.", file=sys.stderr )

            return

    #############################################################################################################################
    # Parse `match_all` filter expressions: complain if
    # 
    #     * requested columns don't exist
    #     * illegal or type-inappropriate operators are used
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere but at the ends of a filter string
    # 
    # ...and save parse results for each filter expression as a separate Query object (to be combined later).

    queries_for_match_all = []

    for item in match_all:
        
        # Try to extract a column name from this filter expression. Don't be case-sensitive.

        filter_column_name = re.sub( r'^([\S]+)\s.*', r'\1', item ).lower()

        # Let's see if this thing exists.

        filter_column_metadata = columns( column=filter_column_name )

        matching_column_count = len( filter_column_metadata )

        if matching_column_count != 1:
            
            print( f"summary_counts(): ERROR: match_all: requested column '{filter_column_name}' is not a searchable CDA column.", file=sys.stderr )

            return

        # See what the operator is.

        filter_operator = re.sub( r'^\S+\s+(\S+)\s.*', r'\1', item )

        if filter_operator == '==':
            
            # Be kind to computer scientists.

            filter_operator = '='

        elif filter_operator not in allowed_operators:
            
            print( f"summary_counts(): ERROR: match_all: operator '{filter_operator}' is not supported.", file=sys.stderr )

            return

        # Identify the data type in the column being filtered.

        target_data_type = filter_column_metadata['data_type'][0]

        # Make sure the operator specified is allowed for the data type of the column being filtered.

        if filter_operator not in operators_by_data_type[target_data_type]:
            
            print( f"summary_counts(): ERROR: match_all: operator '{filter_operator}' is not usable for values of type '{target_data_type}'.", file=sys.stderr )

            return

        # We said quotes weren't required for string values. Doesn't technically mean they can't be used. Remove them.

        filter_value = re.sub( r'^\S+\s+\S+\s+(\S.*)$', r'\1', item )

        filter_value = re.sub( r"""^['"]+""", r'', filter_value )
        filter_value = re.sub( r"""['"]+$""", r'', filter_value )

        # Validate VALUE types and process wildcards.

        if target_data_type != 'text':
            
            # Ignore leading and trailing whitespace unless we're dealing with strings.

            filter_value = re.sub( r'^\s+', r'', filter_value )
            filter_value = re.sub( r'\s+$', r'', filter_value )

        if filter_value.lower() != 'null':
            
            if target_data_type == 'boolean':
                
                # If we're supposed to be in a boolean column, make sure we've got a true/false value.

                filter_value = filter_value.lower()

                if filter_value not in boolean_alias:
                    
                    print( f"summary_counts(): ERROR: match_all: requested column {filter_column_name} has data type 'boolean', requiring a true/false value; you specified '{filter_value}', which is neither.", file=sys.stderr )

                    return

                else:
                    
                    filter_value = boolean_alias[filter_value]

            elif target_data_type in [ 'bigint', 'integer', 'numeric' ]:
                
                # If we're supposed to be in a numeric column, make sure we've got a number.

                if re.search( r'^[-+]?\d+(\.\d+)?$', filter_value ) is None:
                    
                    print( f"summary_counts(): ERROR: match_all: requested column {filter_column_name} has data type '{target_data_type}', requiring a number value; you specified '{filter_value}', which is not.", file=sys.stderr )

                    return

            elif target_data_type == 'text':
                
                # Check for wildcards: if found, adjust operator and
                # wildcard syntax to match API expectations on incoming queries.

                original_filter_value = filter_value

                if re.search( r'^\*', filter_value ) is not None or re.search( r'\*$', filter_value ) is not None:
                    
                    filter_value = re.sub( r'^\*+', r'%', filter_value )

                    filter_value = re.sub( r'\*+$', r'%', filter_value )

                    if filter_operator == '!=':
                        
                        filter_operator = 'NOT LIKE'

                    else:
                        
                        filter_operator = 'LIKE'

                if re.search( r'\*', filter_value ) is not None:
                    
                    print( f"summary_counts(): ERROR: match_all: wildcards (*) are only allowed at the ends of string values; string '{original_filter_value}' is noncompliant (it has one in the middle). Please fix.", file=sys.stderr )

                    return

            else:
                
                # Just to be safe. Types change.

                print( f"summary_counts(): ERROR: match_all: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.", file=sys.stderr )

                return

        # Build a Query object for this filter and add it to the match_all list.

        filter_query = Query()

        filter_query.node_type = filter_operator

        filter_query.l = Query()

        filter_query.l.node_type = 'column'

        filter_query.l.value = filter_column_name

        filter_query.r = Query()

        if filter_value.lower() == 'null':
            
            if filter_operator == '=':
                
                filter_query.node_type = 'IS'

            elif filter_operator == '!=':
                
                filter_query.node_type = 'IS NOT'

            else:
                
                print( f"summary_counts(): ERROR: match_all: filter query '{filter_column_name} {filter_operator} {filter_value}' is malformed: please use only = and != when filtering on NULL.", file=sys.stderr )

                return

            filter_query.r.node_type = 'unquoted'

            filter_query.r.value = 'NULL'

        else:
            
            if target_data_type == 'text':
                
                filter_query.r.node_type = 'quoted'

            else:
                
                filter_query.r.node_type = 'unquoted'

            filter_query.r.value = filter_value

        queries_for_match_all.append( filter_query )

    #############################################################################################################################
    # Parse `match_any` filter expressions: complain if
    # 
    #     * requested columns don't exist
    #     * illegal or type-inappropriate operators are used
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere but at the ends of a filter string
    # 
    # ...and save parse results for each filter expression as a separate Query object (to be combined later).

    queries_for_match_any = []

    for item in match_any:
        
        # Try to extract a column name from this filter expression. Don't be case-sensitive.

        filter_column_name = re.sub( r'^([\S]+)\s.*', r'\1', item ).lower()

        # Let's see if this thing exists.

        filter_column_metadata = columns( column=filter_column_name )

        matching_column_count = len( filter_column_metadata )

        if matching_column_count != 1:
            
            print( f"summary_counts(): ERROR: match_any: requested column '{filter_column_name}' is not a searchable CDA column.", file=sys.stderr )

            return

        # See what the operator is.

        filter_operator = re.sub( r'^\S+\s+(\S+)\s.*', r'\1', item )

        if filter_operator == '==':
            
            # Be kind to computer scientists.

            filter_operator = '='

        elif filter_operator not in allowed_operators:
            
            print( f"summary_counts(): ERROR: match_any: operator '{filter_operator}' is not supported.", file=sys.stderr )

            return

        # Identify the data type in the column being filtered.

        target_data_type = filter_column_metadata['data_type'][0]

        # Make sure the operator specified is allowed for the data type of the column being filtered.

        if filter_operator not in operators_by_data_type[target_data_type]:
            
            print( f"summary_counts(): ERROR: match_any: operator '{filter_operator}' is not usable for values of type '{target_data_type}'.", file=sys.stderr )

            return

        # We said quotes weren't required for string values. Doesn't technically mean they can't be used. Remove them.

        filter_value = re.sub( r'^\S+\s+\S+\s+(\S.*)$', r'\1', item )

        filter_value = re.sub( r"""^['"]+""", r'', filter_value )
        filter_value = re.sub( r"""['"]+$""", r'', filter_value )

        # Validate VALUE types and process wildcards.

        if target_data_type != 'text':
            
            # Ignore leading and trailing whitespace unless we're dealing with strings.

            filter_value = re.sub( r'^\s+', r'', filter_value )
            filter_value = re.sub( r'\s+$', r'', filter_value )

        if filter_value.lower() != 'null':
            
            if target_data_type == 'boolean':
                
                # If we're supposed to be in a boolean column, make sure we've got a true/false value.

                filter_value = filter_value.lower()

                if filter_value not in boolean_alias:
                    
                    print( f"summary_counts(): ERROR: match_any: requested column {filter_column_name} has data type 'boolean', requiring a true/false value; you specified '{filter_value}', which is neither.", file=sys.stderr )

                    return

                else:
                    
                    filter_value = boolean_alias[filter_value]

            elif target_data_type in [ 'bigint', 'integer', 'numeric' ]:
                
                # If we're supposed to be in a numeric column, make sure we've got a number.

                if re.search( r'^[-+]?\d+(\.\d+)?$', filter_value ) is None:
                    
                    print( f"summary_counts(): ERROR: match_any: requested column {filter_column_name} has data type '{target_data_type}', requiring a number value; you specified '{filter_value}', which is not.", file=sys.stderr )

                    return

            elif target_data_type == 'text':
                
                # Check for wildcards: if found, adjust operator and
                # wildcard syntax to match API expectations on incoming queries.

                original_filter_value = filter_value

                if re.search( r'^\*', filter_value ) is not None or re.search( r'\*$', filter_value ) is not None:
                    
                    filter_value = re.sub( r'^\*+', r'%', filter_value )

                    filter_value = re.sub( r'\*+$', r'%', filter_value )

                    if filter_operator == '!=':
                        
                        filter_operator = 'NOT LIKE'

                    else:
                        
                        filter_operator = 'LIKE'

                if re.search( r'\*', filter_value ) is not None:
                    
                    print( f"summary_counts(): ERROR: match_any: wildcards (*) are only allowed at the ends of string values; string '{original_filter_value}' is noncompliant (it has one in the middle). Please fix.", file=sys.stderr )

                    return

            else:
                
                # Just to be safe. Types change.

                print( f"summary_counts(): ERROR: match_any: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.", file=sys.stderr )

                return

        # Build a Query object for this filter and add it to the `match_any` list.

        filter_query = Query()

        filter_query.node_type = filter_operator

        filter_query.l = Query()

        filter_query.l.node_type = 'column'

        filter_query.l.value = filter_column_name

        filter_query.r = Query()

        if filter_value.lower() == 'null':
            
            if filter_operator == '=':
                
                filter_query.node_type = 'IS'

            elif filter_operator == '!=':
                
                filter_query.node_type = 'IS NOT'

            else:
                
                print( f"summary_counts(): ERROR: match_any: filter query '{filter_column_name} {filter_operator} {filter_value}' is malformed: please use only = and != when filtering on NULL.", file=sys.stderr )

                return

            filter_query.r.node_type = 'unquoted'

            filter_query.r.value = 'NULL'

        else:
            
            if target_data_type == 'text':
                
                filter_query.r.node_type = 'quoted'

            else:
                
                filter_query.r.node_type = 'unquoted'

            filter_query.r.value = filter_value

        queries_for_match_any.append( filter_query )

    #############################################################################################################################
    # Parse `data_source` filter expressions: complain if any are nonconformant, and (for now) save parse results for each
    # filter expression as a separate Query object.

    queries_for_data_source = []

    for item in data_source:
        
        # Build a Query object for this data source and add it to the data_source list.

        data_source_query = Query()

        data_source_query.node_type = '='

        data_source_query.l = Query()

        data_source_query.l.node_type = 'column'

        data_source_query.l.value = f"{table}_from_{item}"

        data_source_query.r = Query()

        data_source_query.r.node_type = 'unquoted'

        data_source_query.r.value = 'true'

        queries_for_data_source.append( data_source_query )

    #############################################################################################################################
    # Combine all match_all filter queries into one big AND-linked query.

    combined_match_all_query = Query()

    if len( queries_for_match_all ) == 0:
        
        # No filters in this group: nothing to do here.

        combined_match_all_query = None

    elif len( queries_for_match_all ) == 1:
        
        # No fancy combination needed. Just pass the API the one filter we've got.

        combined_match_all_query = queries_for_match_all[0]

    else:
        
        # Hook up the first two queries in `queries_for_match_all` with an `AND`.

        combined_match_all_query.node_type = 'AND'

        combined_match_all_query.l = queries_for_match_all[0]

        combined_match_all_query.r = queries_for_match_all[1]

        if len( queries_for_match_all ) > 2:
            
            # Add any remaining queries in `queries_for_match_all`, one at a time.

            for i in range( 2, len( queries_for_match_all ) ):
                
                bigger_query = Query()

                bigger_query.node_type = 'AND'

                bigger_query.l = combined_match_all_query

                bigger_query.r = queries_for_match_all[i]

                combined_match_all_query = bigger_query

    #############################################################################################################################
    # Combine all `match_any` filter queries into one big OR-linked query.

    combined_match_any_query = Query()

    if len( queries_for_match_any ) == 0:
        
        # No filters in this group: nothing to do here.

        combined_match_any_query = None

    elif len( queries_for_match_any ) == 1:
        
        # No fancy combination needed. Just pass the API the one filter we've got.

        combined_match_any_query = queries_for_match_any[0]

    else:
        
        # Hook up the first two queries in `queries_for_match_any` with an `OR`.

        combined_match_any_query.node_type = 'OR'

        combined_match_any_query.l = queries_for_match_any[0]

        combined_match_any_query.r = queries_for_match_any[1]

        if len( queries_for_match_any ) > 2:
            
            # Add any remaining queries in `queries_for_match_any`, one at a time.

            for i in range( 2, len( queries_for_match_any ) ):
                
                bigger_query = Query()

                bigger_query.node_type = 'OR'

                bigger_query.l = combined_match_any_query

                bigger_query.r = queries_for_match_any[i]

                combined_match_any_query = bigger_query

    #############################################################################################################################
    # Combine all data_source queries into one big AND-linked query.

    combined_data_source_query = Query()

    if len( queries_for_data_source ) == 0:
        
        # No filters in this group: nothing to do here.

        combined_data_source_query = None

    elif len( queries_for_data_source ) == 1:
        
        # No fancy combination needed. Just pass the API the one filter we've got.

        combined_data_source_query = queries_for_data_source[0]

    else:
        
        # Hook up the first two queries in `queries_for_data_source` with an `AND`.

        combined_data_source_query.node_type = 'AND'

        combined_data_source_query.l = queries_for_data_source[0]

        combined_data_source_query.r = queries_for_data_source[1]

        if len( queries_for_data_source ) > 2:
            
            # Add any remaining queries in `queries_for_data_source`, one at a time.

            for i in range( 2, len( queries_for_data_source ) ):
                
                bigger_query = Query()

                bigger_query.node_type = 'AND'

                bigger_query.l = combined_data_source_query

                bigger_query.r = queries_for_data_source[i]

                combined_data_source_query = bigger_query

    #############################################################################################################################
    # Combine all the filters we've processed.

    final_query = Query()

    if combined_match_all_query is None and combined_match_any_query is None and combined_data_source_query is None:
        
        # We got no filters. Retrieve everything: make the query we pass to the API
        # equivalent to 'id IS NOT NULL', which should match everything.

        final_query.node_type = 'IS NOT'

        final_query.l = Query()

        final_query.l.node_type = 'column'

        final_query.l.value = f"{table}_id"

        final_query.r = Query()

        final_query.r.node_type = 'unquoted'

        final_query.r.value = 'NULL'

        # `somatic_mutation` has no ID field.

        if table == 'somatic_mutation':
            
            final_query.node_type = 'OR'

            final_query.l = Query()
            final_query.l.node_type = 'IS'

            final_query.l.l = Query()
            final_query.l.l.node_type = 'column'
            final_query.l.l.value = 'hotspot'

            final_query.l.r = Query()
            final_query.l.r.node_type = 'unquoted'
            final_query.l.r.value = 'NULL'

            final_query.r = Query()
            final_query.r.node_type = 'IS NOT'

            final_query.r.l = Query()
            final_query.r.l.node_type = 'column'
            final_query.r.l.value = 'hotspot'

            final_query.r.r = Query()
            final_query.r.r.node_type = 'unquoted'
            final_query.r.r.value = 'NULL'

    elif combined_data_source_query is None:
        
        if combined_match_all_query is not None and combined_match_any_query is None:
            
            final_query = combined_match_all_query

        elif combined_match_all_query is None and combined_match_any_query is not None:
            
            final_query = combined_match_any_query

        else:
            
            # Join both non-null filter groups with an `AND`.

            final_query.node_type = 'AND'

            final_query.l = combined_match_all_query

            final_query.r = combined_match_any_query

    else:
        
        if combined_match_all_query is None and combined_match_any_query is None:
            
            final_query = combined_data_source_query

        elif combined_match_all_query is not None and combined_match_any_query is None:
            
            # Join both non-null filter groups with an `AND`.

            final_query.node_type = 'AND'

            final_query.l = combined_data_source_query

            final_query.r = combined_match_all_query

        elif combined_match_all_query is None and combined_match_any_query is not None:
            
            # Join both non-null filter groups with an `AND`.

            final_query.node_type = 'AND'

            final_query.l = combined_data_source_query

            final_query.r = combined_match_any_query

        else:
            
            # Join all three filter groups via 'AND'.

            final_query.node_type = 'AND'

            final_query.l = combined_match_all_query

            final_query.r = combined_match_any_query

            actually_final_query = Query()

            actually_final_query.node_type = 'AND'

            actually_final_query.l = final_query

            actually_final_query.r = combined_data_source_query

            final_query = actually_final_query

    if debug == True:
        
        # Dump JSON describing the full combined query structure.

        print( json.dumps( final_query, indent=4, cls=CdaApiQueryEncoder ) )

    #############################################################################################################################
    # Fetch data from the API.

    # Make an ApiClient object containing the information necessary to connect to the CDA database.

    # Allow users to override the system-default URL for the CDA API by setting their CDA_API_URL
    # environment variable.

    url_override = os.environ.get( 'CDA_API_URL' )

    if url_override is not None and len( url_override ) > 0:
        
        api_configuration = CdaConfiguration( host=url_override, verify=True, verbose=True )

        api_client_instance = ApiClient( configuration=api_configuration )

        if debug == True:
            
            # Report that we're pulling in a hostname from the CDA_API_URL environment variable.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: summary_counts(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: summary_counts(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    else:
        
        api_configuration = CdaConfiguration( verify=True, verbose=True )

        api_client_instance = ApiClient( configuration=api_configuration )

        if debug == True:
            
            # Report the default location data for the CDA API, as loaded from the CdaConfiguration class.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: summary_counts(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: summary_counts(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: summary_counts(): Querying CDA API '{table}/counts' endpoint", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    # Make a QueryApi object using the connection information in the ApiClient object.

    query_api_instance = QueryApi( api_client_instance )

    # Use the QueryApi instance object's `{table}_counts_query` endpoint-accessor
    # function to get data from the REST API.

    query_selector = {
        
        'subject': query_api_instance.subject_counts_query,
        'diagnosis': query_api_instance.diagnosis_counts_query,
        'researchsubject': query_api_instance.research_subject_counts_query,
        'specimen': query_api_instance.specimen_counts_query,
        'treatment': query_api_instance.treatment_counts_query,
        'file': query_api_instance.file_counts_query,
        'somatic_mutation': query_api_instance.mutation_counts_query
    }

    paged_response_data_object = query_selector[table](
        query=final_query,
        dry_run=False,
        async_req=True
    )

    ### TEMPORARY TRAP: Remove this when mutations/counts/ is fixed
    ###                 and everything should just work; all the
    ###                 downstream processing is already in place.

    if table == 'somatic_mutation':
        
        print( f"summary_counts(): ERROR_WITH_APOLOGIES: summary counts for somatic_mutation are not available at present. Please select any of our other fine tables.", file=sys.stderr )

        return

    # Gracefully fetch asynchronously-generated results once they're ready.

    if isinstance( paged_response_data_object, ApplyResult ):
        
        while paged_response_data_object.ready() is False:
            
            paged_response_data_object.wait( 5 )

        try:
            
            paged_response_data_object = paged_response_data_object.get()

        except ApiException as e:
            
            if e.body is not None:
                
                # Ordinarily, this exception represents a structured complaint
                # from the API service that something went wrong. In this case,
                # the `body` property of the ApiException object will contain
                # a JSON-encoded message generated by the API describing the
                # unfortunate circumstance.

                error_message = json.loads( e.body )['message']

            else:
                
                # Unfortunately, if something goes wrong at the level of the
                # HTTP service on which the API relies -- that is, when we
                # can't actually communicate with the API as such because
                # something's gone wrong with our ability to talk to the web
                # server -- the ApiException class is overloaded to encode
                # that HTTP protocol error (and not throw any further exceptions),
                # instead of handling such events somewhere more appropriate
                # (like via a different exception class altogether).

                error_message = str( e )

            print( f"summary_counts(): ERROR: error message from API: '{error_message}'", file=sys.stderr )

            return

        except BaseException as e:
            
            if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:
                
                print( "summary_counts(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

            else:
                
                print( f"summary_counts(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

            return

    # Report some metadata about the results we got back.
    # 
    # print( f"Total row count in result: {paged_response_data_object.total_row_count}", file=sys.stderr )
    # 
    # print( f"Query SQL: {paged_response_data_object.query_sql}", file=sys.stderr )

    """
    # This is immensely verbose, sometimes.

    if debug == True:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: summary_counts(): First page of '{table}/counts' endpoint response", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

        print( json.dumps( paged_response_data_object.result, indent=4 ) )

        print( '-' * 80, file=sys.stderr )

        print( f"END DEBUG MESSAGE: summary_counts(): First page of '{table}/counts' endpoint response", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    """

    # Make a Pandas DataFrame out of the first batch of results.
    # 
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function.

    result_dataframe = pd.json_normalize( paged_response_data_object.result )

    #############################################################################################################################
    # Postprocess API result data.

    if debug == True:
        
        print( 'Organizing result data...', file=sys.stderr )

    #############################################################################################################################
    # Postprocess API result data.

    # This column duplicates `total_count` when querying somatic_mutation. Remove it.

    if 'somatic_mutation_id' in result_dataframe:
        
        result_dataframe = result_dataframe.drop( columns=[ 'somatic_mutation_id' ] )

    # For some reason, the highest-level summary counts come through as floats. Fix that
    # (and rename them while we're at it).

    toplevel_columns_to_fix = {
        
        'total_count': f"total_{table}_matches",
        'file_id': 'total_related_files'
    }

    for result_column in toplevel_columns_to_fix:
        
        if result_column in result_dataframe:
            
            result_dataframe[result_column] = result_dataframe[result_column].round().astype( int )

            result_dataframe = result_dataframe.rename( columns={ result_column: toplevel_columns_to_fix[result_column] } )

    if return_data_as == '' or return_data_as == 'dataframe_list':
        
        # Right now, the default is to print one table to standard output
        # for each DataFrame that would be returned had they requested
        # `return_data_as='dataframe_list'`.

        result_list = list()

        for toplevel_column in [ f"total_{table}_matches", 'total_related_files' ]:
            
            if toplevel_column in result_dataframe:
                
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                result_list.append( result_dataframe[[toplevel_column]].copy() )

        for result_column in result_dataframe.columns:
            
            if result_column not in [ f"total_{table}_matches", 'total_related_files' ]:
                
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                if result_dataframe[result_column].dtype == 'object':
                    
                    source_pair_keyword = result_column
                    dest_pair_keyword = result_column

                    if re.search( r'_identifier_system$', result_column ) is not None:
                        
                        source_pair_keyword = 'system'
                        dest_pair_keyword = f"{table}_data_source"

                    result_column_dict = {
                        
                        dest_pair_keyword: list(),
                        'count': list()
                    }

                    if result_dataframe[result_column][0] is not None:
                        
                        # These should be arrays of Python dicts, with each dict containing two entries:
                        # 
                        #    data column label and value:
                        #       keyword: `result_column`, except for when `result_column` == 'X_identifier_system', in which case it's just 'system'
                        #       value: one of { 'GDC', 'CDS', 'ICDC', ... }
                        #    observed count of the given value:
                        #       keyword: 'count'
                        #       value: (int) number of times the given data value (described in the previous dictionary entry) was observed in this set of result data

                        for dict_pair in result_dataframe[result_column][0]:
                            
                            print_value = '<NA>'

                            if dict_pair[source_pair_keyword] is not None and dict_pair[source_pair_keyword] != '':
                                
                                print_value = dict_pair[source_pair_keyword]

                            result_column_dict[dest_pair_keyword].append( print_value )

                            result_column_dict['count'].append( dict_pair['count'] )

                    result_list.append( pd.DataFrame.from_dict( result_column_dict ).sort_values( by=[ 'count' ], ascending=[ False ] ).reset_index( drop=True ) )

                else:
                    
                    print( f"summary_counts(): ERROR: unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event.", file=sys.stderr )

                    return

        if return_data_as == '':
            
            if debug == True:
                
                print( '-' * 80, file=sys.stderr )

                print( '      DEBUG MESSAGE: summary_counts(): Returning results in default form (printing list of tables to standard output)', file=sys.stderr )

                print( '-' * 80, end='\n\n', file=sys.stderr )

            with pd.option_context( 'display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', 65 ):
                
                for dataframe in result_list:
                    
                    # Put the count values first in the display.

                    max_col_width = 80

                    maxcolwidths_list = [ None, max_col_width ]

                    colalign_list = [ 'right', 'left' ]

                    if len( dataframe.columns ) == 1:
                        
                        maxcolwidths_list = [ None ]

                        colalign_list = [ 'left' ]

                    else:
                        
                        # Truncate displayed values manually. The `tabulate` library can't handle this on its own (as Pandas does).
                        
                        dataframe[dataframe.columns[0]] = dataframe[dataframe.columns[0]].apply( lambda x: re.sub( f"^(.{{{max_col_width-3}}}).*", r'\1...', x ) if ( x is not None and len( x ) > max_col_width ) else x )

                    new_column_ordering = list( reversed( dataframe.columns.tolist() ) )

                    dataframe = dataframe[new_column_ordering]

                    # Suppress output of confusing row-index column when displaying DataFrame contents and get some control over cell alignment.

                    print( tabulate.tabulate( dataframe, showindex=False, headers=dataframe.columns, tablefmt="double_outline", colalign=colalign_list, maxcolwidths=maxcolwidths_list ) )

            return

        elif return_data_as == 'dataframe_list':
            
            if debug == True:
                
                print( '-' * 80, file=sys.stderr )

                print( '      DEBUG MESSAGE: summary_counts(): Returning results as a list of pandas.DataFrame objects', file=sys.stderr )

                print( '-' * 80, end='\n\n', file=sys.stderr )

            return result_list

    elif return_data_as == 'dict' or return_data_as == 'json':

        # Build a Python dictionary to shape returned results.

        result_dict = dict()

        for result_column in result_dataframe.columns:
            
            if result_dataframe[result_column].dtype == 'int64':
                
                result_dict[result_column] = int( result_dataframe[result_column][0] )

            elif result_dataframe[result_column].dtype == 'object':
                
                result_dict[result_column] = None

                if result_dataframe[result_column][0] is not None:
                    
                    # These should be arrays of Python dicts, with each dict containing two entries:
                    # 
                    #    data column label and value:
                    #       keyword: `result_column`, except for when `result_column` == 'X_identifier_system', in which case it's just 'system'
                    #       value: one of { 'GDC', 'CDS', 'ICDC', ... }
                    #    observed count of the given value:
                    #       keyword: 'count'
                    #       value: (int) number of times the given data value (described in the previous dictionary entry) was observed in this set of result data

                    result_dict[result_column] = dict()

                    pair_keyword = result_column

                    if re.search( r'_identifier_system$', result_column ) is not None:
                        
                        pair_keyword = 'system'

                    for dict_pair in result_dataframe[result_column][0]:
                        
                        result_dict[result_column][dict_pair[pair_keyword]] = dict_pair['count']

            else:
                
                print( f"summary_counts(): ERROR: unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event.", file=sys.stderr )

                return

        if return_data_as == 'dict':
            
            if debug == True:
                
                print( '-' * 80, file=sys.stderr )

                print( f"      DEBUG MESSAGE: summary_counts(): Returning results as a Python dictionary", file=sys.stderr )

                print( '-' * 80, end='\n\n', file=sys.stderr )

            return result_dict

        elif return_data_as == 'json':
            
            # Write the results to a user-specified JSON file.

            if debug == True:
                
                print( '-' * 80, file=sys.stderr )

                print( f"      DEBUG MESSAGE: summary_counts(): Printing results to JSON file '{output_file}'", file=sys.stderr )

                print( '-' * 80, end='\n\n', file=sys.stderr )

            try:
                
                with open( output_file, 'w' ) as OUT:
                    
                    json.dump( result_dict, OUT, indent=4, ensure_ascii=True )

                return

            except Exception as error:
                
                print( f"summary_counts(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'.", file=sys.stderr )

                return

    print( f"summary_counts(): ERROR: Something has gone unexpectedly and disastrously wrong with return-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error.", file=sys.stderr )

    return

#############################################################################################################################
# 
# END summary_counts()
# 
#############################################################################################################################




