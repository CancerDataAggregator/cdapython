import re
import sys

from cdapython.discover import tables

#############################################################################################################################
#
# normalize_to_list( parameter_name, user_supplied_parameter_value, value_type ):
# 
# Covert single bare values to one-element lists, and convert all values to lowercase, to support
# strong downstream assumptions about parameter structures.
# 
# Fail if:
#
#     * `user_supplied_parameter_value` is neither a list of `value_type` elements nor a single `value_type` value
#
#############################################################################################################################

def normalize_to_list( parameter_name, user_supplied_parameter_value, value_type ):
    """
    For any user-supplied parameter data that represents a single value
    for a parameter that can in general take multiple concurrent values,
    convert that data into a one-element list, so we don't have to care
    downstream about whether the parameter value was receievd as a
    single bare value or as a list of values. Also convert all values to
    lowercase.

    Arguments:
        parameter_name ( string; required ):
            The name of the user-facing parameter whose value we're checking.

        user_supplied_parameter_value ( unknown type; required ):
            The value of `parameter_name` as supplied by the user.

        value_type ( Python class name; required ):
            The expected data type against which we're going to validate
            `user_supplied_parameter_value`.
    """

    # Start by assuming everything's fine, and that we have the most general case,
    # namely a list of elements of the expected type.

    list_to_return = user_supplied_parameter_value

    if user_supplied_parameter_value is None:
        
        # This parameter was not set by the user: make it an empty list.
        list_to_return = []

    elif isinstance( user_supplied_parameter_value, value_type ):
        # We have a single value of the correct type. Convert it into a one-element list to return.
        if value_type == str:
            list_to_return = [ user_supplied_parameter_value.lower() ]
        else:
            list_to_return = [ user_supplied_parameter_value ]

    elif not isinstance( user_supplied_parameter_value, list ):
        # We have neither a value of the right type nor a list: can't continue.
        raise RuntimeError( f"User-supplied parameter '{parameter_name}' was assigned a non-list value of unexpected type '{type(user_supplied_parameter_value)}'; should be '{value_type}' or 'list({value_type})'. Please fix." )

    elif not all( isinstance( element, value_type ) for element in user_supplied_parameter_value ):
        # We have a list, but not all of its elements are of the expected type: can't continue.
        raise RuntimeError( f"User-supplied parameter '{parameter_name}' was assigned a list containing elements of unexpected type '{type(user_supplied_parameter_value)}'; elements should all be '{value_type}'. Please fix." )

    if value_type == str:
        return [ element.lower() for element in list_to_return ]
    else:
        return list_to_return

#############################################################################################################################
#
# validate_and_transform_match_filter_list( cached_column_metadata, match_statement ):
# 
# Fail if:
#
#     * requested columns don't exist
#     * illegal or type-inappropriate operators are used
#     * filter values don't match the data types of the columns they're paired with
#     * wildcards appear anywhere but at the ends of a filter string
#
# ...and recombine elements for use in querying
#
#############################################################################################################################

def validate_and_transform_match_filter_list( cached_column_metadata, match_statement_list, enforce_column_uniqueness=False ):
    """
    Parse `match_*` filter expressions and transform for syntax validity with the API.

    Arguments:
        cached_column_metadata ( DataFrame; required ):
            Column metadata from the API, cached by the calling function for downstream reuse without further network disturbance.
            The data structure is a DataFrame with columns [ 'table', 'column', 'data_type', 'nullable', 'description' ].

        match_statement_list ( list of strings; optional ):
            One or more conditions, expressed as filter strings

        enforce_column_uniqueness ( boolean; optional ):
            Should we only allow each column to be filtered exactly once? (for match_all)
            Default: False (for match_any)

    Returns:
        List of transformed and cleaned up match statements, or an empty list if no inputs were given

    """
    normalized_match_statement_list = []

    if len( match_statement_list ) == 0:
        return normalized_match_statement_list

    #############################################################################################################################
    # Define and categorize lists of supported filter-string operators.

    comparison_operators = {
        '>',
        '>=',
        '<',
        '<='
    }

    flip_comparison_operator = {
        '>' : '<',
        '>=' : '<=',
        '<' : '>',
        '<=' : '>='
    }

    equality_operators = {
        '=',
        '!='
    }

    allowed_operators = comparison_operators | equality_operators

    #############################################################################################################################
    # Enumerate restrictions on operator use to appropriate data types.

    operators_by_data_type = {
        'bigint': allowed_operators,
        'boolean': equality_operators,
        'integer': allowed_operators,
        'numeric': allowed_operators,
        'text': equality_operators,
    }

    #############################################################################################################################
    # Enable aliases for various ways to say "True" and "False". (Case will be lowered as soon as each literal is received.)

    boolean_alias = {
        'true': 'true',
        't': 'true',
        'false': 'false',
        'f': 'false'
    }

    seen_filter_column_names = set()

    for filter_expression in match_statement_list:
        
        if not isinstance( filter_expression, str ) or len( filter_expression ) == 0:
            raise RuntimeError( f"Match parameters must be nonempty filter strings: you specified '{filter_expression}' (from match list {match_statement_list}), which is not." )

        #############################################################################################################################
        # Enforce the simplified cdapython query syntax as described in the docs, but quietly allow synonyms if received. Map them
        # first back to the canonical operator in the simplified query syntax, then validate, then normalize for API request syntax
        # as if they'd come in as their canonical versions.

        # Take care of two-word operators first: downstream, unmodified, they break simplifying assumptions
        # about filter string tokenization that should safe to make given the spec of the cdapython query syntax.
        if re.search( r'^(\S+)\s+IS\s+NOT\s+(\S.*)$', filter_expression, flags=re.IGNORECASE ) is not None:
            filter_expression = re.sub( r'^(\S+)\s+IS\s+NOT\s+(\S.*)$', r'\1 != \2', filter_expression, flags=re.IGNORECASE )

        elif re.search( r'^(\S+)\s+NOT\s+LIKE\s+(\S.*)$', filter_expression, flags=re.IGNORECASE ) is not None:
            filter_expression = re.sub( r'^(\S+)\s+NOT\s+LIKE\s+(\S.*)$', r'\1 != \2', filter_expression, flags=re.IGNORECASE )

        # Normalize the rest of the known operators (used for null and fuzzy matches) to conform to cdapython query syntax.
        elif re.search( r'^(\S+)\s+IS\s+(\S.*)$', filter_expression, flags=re.IGNORECASE ) is not None:
            filter_expression = re.sub( r'^(\S+)\s+IS\s+(\S.*)$', r'\1 = \2', filter_expression, flags=re.IGNORECASE )

        elif re.search( r'^(\S+)\s+LIKE\s+(\S.*)$', filter_expression, flags=re.IGNORECASE ) is not None:
            filter_expression = re.sub( r'^(\S+)\s+LIKE\s+(\S.*)$', r'\1 = \2', filter_expression, flags=re.IGNORECASE )

        # Handle ternary comparisons and mark matching filters as such.
        filter_is_ternary = False
        filter_column_name = ''
        match_result = re.search( r'^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S.*)$', filter_expression )
        if match_result is not None:
            left_numeric = match_result.group(1)
            left_operator = match_result.group(2)
            column_name = match_result.group(3)
            right_operator = match_result.group(4)
            right_numeric = match_result.group(5)
            if re.search( r'^[-+]?\d+(\.\d+)?$', left_numeric ) is not None and re.search( r'^[-+]?\d+(\.\d+)?$', right_numeric ) is not None and \
                left_operator in comparison_operators and right_operator in comparison_operators:
                # This is a numeric comparison of the type we seek.
                filter_is_ternary = True
                filter_column_name = column_name.lower()

        #############################################################################################################################
        # Validate (non-ternary) minimal filter string format: <non-whitespace string (column name)><whitespace><non-whitespace string (operator)><whitespace><non-whitespace string (beginning of value to match)><any mix of whitespace and non-whitespace characters (end of value to match)>

        if not filter_is_ternary and re.search( r'^\S+\s+\S+\s+\S.*$', filter_expression ) is None:
            raise RuntimeError( f"Filter string '{filter_expression}' does not conform to 'COLUMN_NAME OP VALUE' format. See the help text for details." )

        #############################################################################################################################
        # Now parse the filter expression and validate COLUMN and OP tokens.

        # Try to extract a column name from this filter expression. Don't be case-sensitive. For
        # ternary filters, we've already extracted `filter_column_name`, above.
        if not filter_is_ternary:
            filter_column_name = re.sub( r'^([\S]+)\s.*', r'\1', filter_expression ).lower()

        # Have we seen this before (and do we care)?
        if enforce_column_uniqueness:
            if filter_column_name in seen_filter_column_names:
                raise RuntimeError( f"Requested column '{filter_column_name}' cannot be used twice in a 'match_all' list." )
            else:
                seen_filter_column_names.add( filter_column_name )

        # Let's see if this thing exists.
        filter_column_metadata = cached_column_metadata[ cached_column_metadata['column'] == filter_column_name ]

        # We should see a one-row DataFrame.
        if filter_column_metadata is None or len( filter_column_metadata ) != 1:
            raise RuntimeError( f"Requested column '{filter_column_name}' is not a searchable CDA column." )

        # Identify the data type in the column being filtered.
        target_data_type = filter_column_metadata['data_type'].iloc[0]

        # Ensure that we recognize this data type.
        if target_data_type not in operators_by_data_type:
            raise RuntimeError( f"Requested column '{filter_column_name}' is of unknown data_type '{target_data_type}': cannot continue, please contact the CDA devs with a description of this event." )

        normalized_filter_expression = filter_expression

        if not filter_is_ternary:
            
            # See what the operator is.
            filter_operator = re.sub( r'^\S+\s+(\S+)\s.*', r'\1', filter_expression )

            # Be kind to computer scientists.
            if filter_operator == '==':
                filter_operator = '='

            # Make sure the operator specified is allowed for the data type of the column being filtered.
            if filter_operator not in operators_by_data_type[target_data_type]:
                raise RuntimeError( f"Operator '{filter_operator}' is not usable for values of type '{target_data_type}'." )

            # Extract the filter value/pattern.
            filter_value = re.sub( r'^\S+\s+\S+\s+(\S.*)$', r'\1', filter_expression )

            # We said quotes weren't required for string values. Doesn't technically mean they can't be used. Remove them.
            filter_value = re.sub( r'''^['"]*(.*)['"]*$''', r'\1', filter_value)

            #############################################################################################################################
            # Validate VALUE types and process wildcards.

            # Ignore leading and trailing whitespace unless we're dealing with strings.

            if target_data_type != 'text':
                filter_value = filter_value.strip()

            if filter_value.lower() != 'null':
                
                if target_data_type == 'boolean':
                    
                    # If we're supposed to be in a boolean column, make sure we've got a true/false value. Normalize recognized synonyms for valid values.

                    filter_value = filter_value.lower()

                    if filter_value not in boolean_alias:
                        raise RuntimeError( f"Requested column {filter_column_name} has data type 'boolean', requiring a true/false value; you specified '{filter_value}', which is not valid." )
                    else:
                        filter_value = boolean_alias[filter_value]

                elif target_data_type in [ 'bigint', 'integer', 'numeric' ]:
                    
                    # If we're supposed to be in a numeric column, make sure we've got a number.

                    if re.search( r'^[-+]?\d+(\.\d+)?$', filter_value ) is None:
                        raise RuntimeError( f"Requested column {filter_column_name} has data type '{target_data_type}', requiring a number value; you specified '{filter_value}', which is not." )

                elif target_data_type == 'text':
                    
                    # Check for wildcards: if found, adjust operator and
                    # wildcard syntax to match API expectations on incoming queries.

                    original_filter_value = filter_value

                    if re.search( r'^\*', filter_value ) is not None or re.search( r'\*$', filter_value ) is not None:
                        
                        # API expects lowercase operators.
                        if filter_operator == '!=':
                            filter_operator = 'not like'
                        else:
                            filter_operator = 'like'

                    if re.search(r'.\*.', filter_value) is not None:
                        raise RuntimeError( f"Wildcards (*) are only allowed at the ends of string values; string '{original_filter_value}' is noncompliant (it has one in the middle). Please fix." )

                    # API expects percent signs.
                    filter_value = re.sub( r'\*', r'%', filter_value )

                    # API needs strings quoted. Minimal replication case: match a string that contains only numbers. Need to distinguish from integer input.
                    filter_value = f"'{filter_value}'"

                else:

                    # Just to be safe. Types change.
                    raise RuntimeError( f"Unanticipated data_type '{target_data_type}' encountered for filter-target column '{filter_column_name}'; cannot continue. Please report this event to CDA developers." )

            else:
                
                if filter_operator == '=':
                    filter_operator = 'is'
                elif filter_operator == '!=':
                    filter_operator = 'is not'
                else:
                    raise RuntimeError( f"Unexpected operator encountered for NULL: '{filter_operator}' (from '{filter_expression}') -- please use = or != instead." )

                filter_value = 'null'

            normalized_filter_expression = filter_column_name + ' ' + filter_operator + ' ' + filter_value

        normalized_match_statement_list.append( normalized_filter_expression )

    return normalized_match_statement_list

#############################################################################################################################
# 
# END validate_and_transform_match_filter_list
# 
#############################################################################################################################

#############################################################################################################################
# 
# validate_and_transform_match_from_file_values( target_data_type, input_values ):
# 
# Parse `match_from_file` filter values: complain if
#
#     * filter values don't match the data types of the columns they're paired with
#     * wildcards appear anywhere (they're not compatible with the IN keyword, and we don't currently support the construction of per-value LIKE filters)
#
# ...strip apostrophes, and save parse results as a processed set of valid values.
# 
#############################################################################################################################

def validate_and_transform_match_from_file_values( cda_column_to_match, target_data_type, input_values ):
    
    processed_values = set()

    boolean_alias = {
        'true': 'true',
        't': 'true',
        'false': 'false',
        'f': 'false'
    }

    for target_value in input_values:
            
        # Validate value types and test for wildcards.

        if target_data_type == 'boolean':
            
            # If we're supposed to be in a boolean column, make sure we've got a true/false value.
            if target_value.lower() not in boolean_alias:
                raise RuntimeError( f"match_from_file: requested column {cda_column_to_match} has data type 'boolean', requiring a true/false value; you specified '{target_value}', which is neither." )

            else:
                target_value = boolean_alias[target_value]

        elif target_data_type in ['bigint', 'integer', 'numeric']:
            
            # If we're supposed to be in a numeric column, make sure we've got a number.
            if re.search( r'^[-+]?\d+(\.\d+)?$', target_value ) is None:
                raise RuntimeError( f"match_from_file: requested column {cda_column_to_match} has data type '{target_data_type}', requiring a number value; you specified '{target_value}', which is not." )

        elif target_data_type == 'text':
            
            # Check for wildcards: if found, vomit.
            if re.search(r'\*', target_value) is not None:
                raise RuntimeError( f"match_from_file: wildcards (*) are disallowed here (only exact matches are supported for this option); value '{target_value}' is noncompliant. Please fix." )

        else:
            
            # Just to be safe. Types change.
            raise RuntimeError( f"match_from_file: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers." )

        processed_values.add( re.sub( r"'", r'', target_value ) )

    return processed_values

#############################################################################################################################
#
# END validate_and_transform_match_from_file_values
# 
#############################################################################################################################

#############################################################################################################################
#
# validate_parameter_values( called_function, cached_column_metadata, valid_data_sources, table, match_from_file, data_source, add_columns, exclude_columns, return_data_as, output_file, log ):
# 
# Validate user-supplied parameters as passed to `called_function`, after first
# having passed relevant parameters (`data_source`, `add_columns`, `exclude_columns`)
# through normalize_to_list().
# 
# Fail if:
# 
#     * `called_function` is not in [ 'column_values', 'get_data', 'summarize' ]
#     * `table` is not a CDA table
#     * `match_from_file` isn't a dict which (if non-null) specifies an accessible input file
#       containing a user-specified column, whose values are to be matched against a CDA column
#       that exists
#     * `data_source` isn't a single valid upstream data source label (for `called_function`=='column_values')
#       or a list of valid upstream data source labels (for `called_function` in [ 'get_data', 'summarize' ])
#     * `add_columns` or `exclude_columns` contain invalid CDA column names ("{table}.*" macros are allowed)
#     * `collate_results` isn't a boolean value or None, depending on `called_function`
#     * `include_external_refs` isn't a boolean value or None, depending on `called_function`
#     * `return_data_as` isn't one of the allowable types for `called_function`
#     * The value of `output_file` isn't consistent with the directive in `return_data_as` for `called_function`
#
#############################################################################################################################

def validate_parameter_values(
    called_function,
    cached_column_metadata,
    valid_data_sources,
    table,
    match_from_file,
    data_source,
    add_columns,
    exclude_columns,
    collate_results,
    include_external_refs,
    return_data_as,
    output_file,
    log
):
    # The data structure coming back from columns() is a DataFrame with columns [ 'table', 'column', 'data_type', 'nullable', 'description' ].

    # Make sure `table` exists.

    if table is None or not isinstance( table, str ) or table not in cached_column_metadata['table'].unique():
        raise RuntimeError( f"The required parameter 'table' must be a searchable CDA table; you supplied '{table}', which is not. Please run tables() for a list." )

    if called_function in [ 'get_data', 'summarize' ]:
        
        # Check `match_from_file` data for sanity.

        # Is `match_from_file` a dict with the expected set of keys?
        if not isinstance( match_from_file, dict ) or set( match_from_file.keys() ) != { 'input_file', 'input_column', 'cda_column_to_match' }:
            raise RuntimeError( f"'match_from_file' must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not." )

        # Does `match_from_file`['cda_column_to_match'] exist?
        if match_from_file['cda_column_to_match'] != '' and match_from_file['cda_column_to_match'] not in cached_column_metadata['column'].unique():
            raise RuntimeError( f"'match_from_file['cda_column_to_match']' must be a valid CDA column; you supplied {match_from_file['cda_column_to_match']}, which is not." )

        # Does `match_from_file`['input_file'] exist and does it have a column named `match_from_file`['input_column']?
        if match_from_file['input_file'] != '':
            
            try:
                with open( match_from_file['input_file'] ) as IN:
                    input_file_column_names = next( IN ).rstrip( '\n' ).split( '\t' )
                    if match_from_file['input_column'] not in input_file_column_names:
                        raise RuntimeError( f"'match_from_file['input_column']' must specify a column that exists in 'match_from_file['input_file']'. You specified '{match_from_file['input_column']}', which is not present in '{match_from_file['input_file']}'." )

            except Exception as error:
                raise RuntimeError( f"Couldn't read from match_from_file input file '{match_from_file['input_file']}': got error of type '{type(error)}', with error message '{error}'." )

        # Are the values given in `match_from_file` internally consistent? (Strange results might occur if not.)
        if match_from_file['cda_column_to_match'] == '':
            if match_from_file['input_file'] != '' or match_from_file['input_column'] != '':
                raise RuntimeError( f"If the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that." )
        elif match_from_file['input_file'] == '':
            if match_from_file['cda_column_to_match'] != '' or match_from_file['input_column'] != '':
                raise RuntimeError( f"If the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that." )
        elif match_from_file['input_column'] == '':
            if match_from_file['cda_column_to_match'] != '' or match_from_file['input_file'] != '':
                raise RuntimeError( f"If the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that." )

    if match_from_file['input_file'] != '' and match_from_file['input_file'] == output_file:
        raise RuntimeError( f"You specified the same file ('{output_file}') as both a source of filter values (via 'match_from_file') and the target output file ( via 'output_file'). Please make sure these two files are different." )

    # Check that `data_source` is a single valid upstream data source label (for `called_function`=='column_values')
    # or a list of valid upstream data source labels (for `called_function` in [ 'get_data', 'summarize' ]).
    # `valid_data_sources` was normalized to uppercase when it was constructed by the caller.

    normalized_data_source = list()

    if called_function == 'column_values' and len( data_source ) > 1:
        raise RuntimeError( f"The 'data_source' parameter must be one valid data source name (e.g. 'GDC'); you specified '{data_source}', which is not." )

    for ds in data_source:
        if ds.upper() not in valid_data_sources:
            raise RuntimeError( f"The 'data_source' parameter must be a list containing one or more of [ {', '.join( sorted( valid_data_sources ) )} ]. You supplied '{data_source}', which is not that." )
        normalized_data_source.append( ds.upper() )

    data_source = normalized_data_source

    # Make sure CDA columns named in `add_columns` exist.

    for column_name in add_columns:
        match_result = re.search( r'^(.+)\.\*$', column_name )

        if match_result is not None:
            foreign_table = match_result.group(1)
            if foreign_table not in tables():
                raise RuntimeError( f"'add_columns' can only contain valid CDA column names, or macros for whole tables like 'treatment.*'. You specified '{column_name}', which is neither." )
        elif column_name not in cached_column_metadata['column'].unique():
            raise RuntimeError( f"'add_columns' can only contain valid CDA column names, or macros for whole tables like 'treatment.*'. You specified '{column_name}', which is neither." )

    # Make sure CDA columns named in `exclude_columns` exist.

    for column_name in exclude_columns:
        match_result = re.search( r'^(.+)\.\*$', column_name )

        if match_result is not None:
            foreign_table = match_result.group(1)
            if foreign_table not in tables():
                raise RuntimeError( f"'exclude_columns' can only contain valid CDA column names, or macros for whole tables like 'treatment.*'. You specified '{column_name}', which is neither." )
        elif column_name not in cached_column_metadata['column'].unique():
            raise RuntimeError( f"'exclude_columns' can only contain valid CDA column names. You specified '{column_name}', which is not that." )

    # Check that `collate_results` is a boolean (for get_data) or None (for column_values, summarize) and that `called_function` has an expected value.

    if called_function == 'get_data':
        if collate_results != True and collate_results != False:
            raise RuntimeError( f"The `collate_results` parameter must be set to True or False; you specified '{collate_results}', which is neither." )
        if include_external_refs != True and include_external_refs != False:
            raise RuntimeError( f"The `include_external_refs` parameter must be set to True or False; you specified '{include_external_refs}', which is neither." )
    elif called_function == 'summarize' or called_function == 'column_values':
        if collate_results is not None:
            raise RuntimeError( f"Something has gone horribly and unexpectedly wrong with respect to phantom collate_results values in summarize() calls; please notify the CDA devs of this event." )
        if include_external_refs is not None:
            raise RuntimeError( f"Something has gone horribly and unexpectedly wrong with respect to phantom include_external_refs values in summarize() calls; please notify the CDA devs of this event." )
    else:
        raise RuntimeError( f"`called_function` must be one of [ 'column_values', 'get_data', 'summarize' ] -- '{called_function}' is none of those. Please notify the CDA devs of this event." )

    # Process return-type directives `return_data_as` and `output_file`.

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing.

    allowed_return_types = {
        'get_data': {
            'dataframe',
            'tsv'
        },
        'summarize': {
            '',
            'dataframe_list',
            'dict',
            'json'
        }
    }

    if return_data_as is not None:
        
        if not isinstance( return_data_as, str ):
            raise RuntimeError( f"Unrecognized 'return_data_as' value '{return_data_as}' requested. Valid values are [ {', '.join( allowed_return_types[called_function] )} ]." )

        if not isinstance( output_file, str ):
            raise RuntimeError( f"The `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path." )

        if return_data_as not in allowed_return_types[called_function]:
            # Complain if we receive an unexpected `return_data_as` value.
            raise RuntimeError( f"Unrecognized 'return_data_as' value '{return_data_as}' requested. Valid values are [ {', '.join( allowed_return_types[called_function] )} ]." )

        elif called_function == 'get_data':
            
            if return_data_as == 'tsv' and output_file == '':
                # If the user asks for TSV, they also have to give us a path for the output file. If they didn't, complain.
                raise RuntimeError( 'Return type \'tsv\' was requested, but \'output_file\' was not specified. Please specify output_file=\'some/path/string/to/write/your/tsv/to/your_tsv_output_file.tsv\'.' )

            elif return_data_as != 'tsv' and output_file != '':
                
                # If the user put something in the `output_file` parameter but didn't specify `result_data_as`='tsv',
                # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
                # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
                # so we complain and ask them to clarify.

                raise RuntimeError( f"'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'.\n(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.)." )

        elif called_function == 'summarize':
            
            if return_data_as == 'json' and output_file == '':
                # If the user asks for JSON, they also have to give us a path for the output file. If they didn't, complain.
                raise RuntimeError( "Return type 'json' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/json/to'." )

            elif return_data_as != 'json' and output_file != '':
                # If the user put something in the `output_file` parameter but didn't specify `result_data_as='json'`,
                # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
                # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
                # so we complain and ask them to clarify.

                raise RuntimeError( f"'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'json'. You requested return_data_as='{return_data_as}'.\n(Note that if you don't specify any value for 'return_data_as', it defaults to printing tables to the standard output stream and not to an output file.)." )

        else:
            raise RuntimeError( f"Got unpexpectedly non-null 'return_data_as' value '{return_data_as}' from function '{called_function}'. Please report this event to the CDA devs." )

    return

#############################################################################################################################
#
# END validate_parameter_values
# 
#############################################################################################################################


