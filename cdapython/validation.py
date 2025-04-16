import re

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

def validate_and_transform_match_filter_list( cached_column_metadata, match_statement_list ):
    """
    Parse `match_*` filter expressions and transform for syntax alidity with the API.

    Arguments:
        cached_column_metadata ( DataFrame; required ):
            Column metadata from the API, cached by the calling function for downstream reuse without further network disturbance.
            The data structure is a DataFrame with columns [ 'table', 'column', 'data_type', 'nullable', 'description' ].

        match_statement_list ( list of strings; optional ):
            One or more conditions, expressed as filter strings

    Returns:
        List of transformed and cleaned up match statements, or an empty list if no inputs were given

    """
    normalized_match_statement_list = []

    if len( match_statement_list ) == 0:
        return normalized_match_statement_list

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
        'text': { '=', '!=' },
    }

    #############################################################################################################################
    # Enable aliases for various ways to say "True" and "False". (Case will be lowered as soon as each literal is received.)

    boolean_alias = {
        'true': 'true',
        't': 'true',
        'false': 'false',
        'f': 'false'
    }

    for filter_expression in match_statement_list:
        
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
            filter_expression = re.sub( r'^(\S+)\s+IS\s+\s+(\S.*)$', r'\1 = \2', filter_expression, flags=re.IGNORECASE )

        elif re.search( r'^(\S+)\s+LIKE\s+(\S.*)$', filter_expression, flags=re.IGNORECASE ) is not None:
            filter_expression = re.sub( r'^(\S+)\s+LIKE\s+\s+(\S.*)$', r'\1 = \2', filter_expression, flags=re.IGNORECASE )

        #############################################################################################################################
        # Now parse the filter expression and validate COLUMN and OP tokens.

        # Try to extract a column name from this filter expression. Don't be case-sensitive.
        filter_column_name = re.sub( r'^([\S]+)\s.*', r'\1', filter_expression ).lower()

        # Let's see if this thing exists.
        filter_column_metadata = cached_column_metadata[ cached_column_metadata['column'] == filter_column_name ]

        # We should see a one-row DataFrame.
        if filter_column_metadata is None or len( filter_column_metadata ) != 1:
            raise RuntimeError( f"Requested column '{filter_column_name}' is not a searchable CDA column." )

        # Identify the data type in the column being filtered.
        target_data_type = filter_column_metadata['data_type'].iloc[0]

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
        filter_value = re.sub( r'''^['"]*(.*)['"]*$''', r'', filter_value)

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
                    
                    if filter_operator == '!=':
                        filter_operator = 'NOT LIKE'
                    else:
                        filter_operator = 'LIKE'

                if re.search(r'.\*.', filter_value) is not None:
                    raise RuntimeError( f"Wildcards (*) are only allowed at the ends of string values; string '{original_filter_value}' is noncompliant (it has one in the middle). Please fix." )

            else:

                # Just to be safe. Types change.
                raise RuntimeError( f"Unanticipated data_type '{target_data_type}' encountered for filter-target column '{filter_column_name}'; cannot continue. Please report this event to CDA developers." )

        else:
            
            # filter_value.lower() == 'null': normalize operator and value. Value normalizatin is entirely unnecessary here but satisfies the author's over-tuned need for well-formed output.

            if filter_operator == '=':
                filter_operator = 'IS'
            elif filter_operator == '!=':
                filter_operator = 'IS NOT'
            else:
                raise RuntimeError( f"Unexpected operator encountered for NULL: '{filter_operator}' (from '{filter_expression}') -- please use = or != instead." )

            filter_value = 'NULL'

        normalized_filter_expression = filter_column_name + ' ' + filter_operator + ' ' + filter_value

        normalized_match_statement_list.append( normalized_filter_expression )

    return normalized_match_statement_list

