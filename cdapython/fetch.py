import json
import pandas as pd
import os
import re
import sys

from cdapython.explore import columns, tables

from cda_client.api_client import ApiClient
from cda_client.api.query_api import QueryApi

from cda_client.exceptions import ApiException

from cda_client.model.paged_response_data import PagedResponseData
from cda_client.model.query import Query

## MAYBE TO DO: Eliminate this and just use the Configuration base class instead.
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
# fetch_rows( table=`table` ): Get CDA data records ('result rows') from `table` that match user-specified criteria.
# 
#############################################################################################################################

def fetch_rows(
    table=None,
    *,
    match_all = [],
    match_any = [],
    data_source = [],
    add_columns = [],
    link_to_table = '',
    provenance = False,
    count_only = False,
    return_data_as = 'dataframe',
    output_file = '',
    debug = False
):
    """
    Get CDA data records ('result rows') from `table` that match user-specified criteria.

    Arguments:
        table( string; required ):
            The table whose rows are to be filtered and retrieved. (Run the tables()
            function to get a list.)

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

        add_columns ( string or list of strings; optional ):
            One or more columns from a second table to add to result data from `table`.
            If multiple values from an added column are all associated with a single
            `table` row, that row will be repeated once for each distinct value, with
            the added data appended to each row.

        link_to_table ( string; optional ):
            A second table from which to fetch entire rows related to the row results
            from `table` that this function produces. `link_to_table` results
            will be appended to `table` rows to which they're related:
            any `table` row related to more than one `link_to_table` row will
            be repeated in the returned data, with one distinct `link_to_table` row
            appended to each repeated copy of its related `table` row.
            If `link_to_table` is specified, `add_columns` cannot be used.

        provenance ( boolean; optional ):
            If True, fetch_rows() will attach cross-reference information
            to each row result describing the upstream data sources from
            which it was derived. Rows deriving from more than one upstream
            source will be repeated in the output, once per data source, as
            with `link_to_table` and `add_columns` (except with provenance
            metadata attached, instead of information from other CDA tables).
            If `provenance` is set to True, `link_to_table` and `add_columns`
            cannot be used.

        return_data_as ( string; optional: 'dataframe' or 'tsv' ):
            Specify how fetch_rows() should return results: as a pandas DataFrame,
            or as output written to a TSV file named by the user. If this
            argument is omitted, fetch_rows() will default to returning
            results as a DataFrame.

        output_file( string; optional ):
            If return_data_as='tsv' is specified, `output_file` should contain a
            resolvable path to a file into which fetch_rows() will write
            tab-delimited results.

        count_only ( boolean; optional ):
            If set to True, fetch_rows() will return two integers: the number of CDA
            `table` rows matching the specified filters, and the total number of rows
            that this function would return if `count_only` were not True. (These numbers
            will be identical if no data from outside `table` has been joined to result
            rows (for example by using `link_to_table` or `add_columns` or
            `provenance`). If `count_only` is set to False (the default), fetch_rows() will
            return a pandas DataFrame containing all CDA `table` rows that match the
            given filters.

        debug ( boolean; optional ):
            If set to True, internal process details will be printed to the standard error
            stream as fetch_rows() is running. If False (the default), ...they won't.

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
        the filters specified just above in the `match_all` argument, when querying
        the `subject` table, we can write:
            
            fetch_rows( table='subject', match_all=[ 'primary_disease_type = *duct*', 'sex = F*' ] )

        NULL is a special VALUE which can be used to match missing data. For
        example, to get `researchsubject` rows where the `primary_diagnosis_site` field
        is missing data, we can write:
            
            fetch_rows( table='researchsubject', match_all=[ 'primary_diagnosis_site = NULL' ] )

    Returns:
        (Default) A pandas.DataFrame containing CDA `table` rows matching the user-specified
            filter criteria. The DataFrame's named columns will match columns in `table`,
            and each row in the DataFrame will contain one CDA `table` row (possibly
            with related data from a second table appended to it, according to user
            directives).

        OR two integers representing the total number of CDA `table` rows matching the given
            filters and the total number of result rows. These two counts will generally
            differ if extra data from non-`table` sources is joined to result rows using
            `link_to_table` or `add_columns`, because `table` rows will be repeated for any
            one-to-many associations that are returned; otherwise they will be the same.

        OR returns nothing, but writes results to a user-specified TSV file

    """

    #############################################################################################################################
    # Top-level type and sanity checking (i.e. not examining list contents yet): ensure nothing untoward got passed into our parameters.

    # Make sure the requested table exists.

    if table is None or not isinstance( table, str ) or table not in tables():
        
        print( f"fetch_rows(): ERROR: The required parameter 'table' must be a searchable CDA table; you supplied '{table}', which is not. Please run tables() for a list.", file=sys.stderr )

        return

    # `match_all`

    if isinstance( match_all, str ):
        
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        match_all = [ match_all ]

    if not isinstance( match_all, list ):
        
        print( f"fetch_rows(): ERROR: value assigned to 'match_all' parameter must be a filter string or a list of filter strings; you specified '{match_all}', which is neither.", file=sys.stderr )

        return

    # `match_any`

    if isinstance( match_any, str ):
        
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        match_any = [ match_any ]

    if not isinstance( match_any, list ):
        
        print( f"fetch_rows(): ERROR: value assigned to 'match_any' parameter must be a filter string or a list of filter strings; you specified '{match_any}', which is neither.", file=sys.stderr )

        return

    # `data_source`

    if isinstance( data_source, str ):
        
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        data_source = [ data_source ]

    elif not isinstance( data_source, list ):
        
        print( f"fetch_rows(): ERROR: value assigned to the 'data_source' parameter must be a string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.", file=sys.stderr )

        return

    # `add_columns`

    if isinstance( add_columns, str ):
        
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        add_columns = [ add_columns ]

    if not isinstance( add_columns, list ):
        
        print( f"fetch_rows(): ERROR: value assigned to 'add_columns' parameter must be a string (e.g. 'primary_diagnosis_site') or a list of strings (e.g. [ 'specimen_type', 'primary_diagnosis_condition' ]); you specified '{add_columns}', which is neither.", file=sys.stderr )

        return

    # `link_to_table`

    if not isinstance( link_to_table, str ):
        
        print( f"fetch_rows(): ERROR: parameter 'link_to_table' must be a string; you supplied '{link_to_table}', which is not.", file=sys.stderr )

        return

    elif link_to_table != '' and link_to_table not in tables():
        
        print( f"fetch_rows(): ERROR: parameter 'link_to_table' must be the name of a searchable CDA table; you provided '{link_to_table}', which is not. See tables() for a list of valid table names.", file=sys.stderr )

        return

    elif link_to_table != '' and link_to_table == table:
        
        print( f"fetch_rows(): ERROR: parameter 'link_to_table' can't specify the same table as the 'table' parameter. Please try again.", file=sys.stderr )

        return

    # `provenance`

    if provenance != True and provenance != False:
        
        print( f"fetch_rows(): ERROR: The `provenance` parameter must be set to True or False; you specified '{provenance}', which is neither.", file=sys.stderr )

        return

    # `return_data_as`

    if not isinstance( return_data_as, str ):
        
        print( f"fetch_rows(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe' or 'tsv'.", file=sys.stderr )

        return

    # `output_file`

    if not isinstance( output_file, str ):
        
        print( f"fetch_rows(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path.", file=sys.stderr )

        return

    # `count_only`

    if count_only != True and count_only != False:
        
        print( f"fetch_rows(): ERROR: The `count_only` parameter must be set to True or False; you specified '{count_only}', which is neither.", file=sys.stderr )

        return

    # `debug`

    if debug != True and debug != False:
        
        print( f"fetch_rows(): ERROR: The `debug` parameter must be set to True or False; you specified '{debug}', which is neither.", file=sys.stderr )

        return

    #############################################################################################################################
    # Preprocess table metadata, to enable consistent processing (and reporting) throughout.

    # Track the data type present in each `table` column, so we can
    # format results properly downstream.

    result_column_data_types = dict()

    # Store the default column ordering as provided by the columns() function,
    # to enable us to always display the same data in the same way.

    source_table_columns_in_order = list()

    for row_index, column_record in columns( table=table ).iterrows():
        
        result_column_data_types[column_record['column']] = column_record['data_type']

        source_table_columns_in_order.append( column_record['column'] )

    # "`table`_associated_project" and "`table`_identifier", provided by the API
    # as non-atomic objects (a list and a list of dicts, respectively) and
    # previously embedded whole into single cells of the rectangular matrices
    # that we returned to users as as result data, are now to be withheld
    # from default user-facing endpoint results, to allow us to meet expectations
    # about basic uniformity (and rapid usability) of CDA result data.
    # 
    # Reliable retrieval of one-to-many project associations is deferred
    # until the CRDC Common Model is implemented, with its own dedicated
    # `project` entity.
    # 
    # If `provenance` is set to True, we will retain the identifier information
    # and include its contents in restructured results.
    # 
    # These two columns don't appear in columns() output right now,
    # so they never make it into `source_table_columns_in_order`.
    # If we want one, we need to add it back.

    if provenance == True and table != 'somatic_mutation':
        
        source_table_columns_in_order.append( f"{table}_identifier" )

        result_column_data_types[f"{table}_identifier"] = 'array_of_id_dictionaries'

    #############################################################################################################################
    # Process return-type directives `return_data_as` and `output_file`.

    allowed_return_types = {
        '',
        'dataframe',
        'tsv'
    }

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        
        # Complain if we receive an unexpected `return_data_as` value.

        print( f"fetch_rows(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe' or 'tsv'.", file=sys.stderr )

        return

    elif return_data_as == 'tsv' and output_file == '':
        
        # If the user asks for TSV, they also have to give us a path for the output file. If they didn't, complain.

        print( f"fetch_rows(): ERROR: return type 'tsv' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/tsv/to'.", file=sys.stderr )

        return

    elif return_data_as != 'tsv' and output_file != '':
        
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        print( f"fetch_rows(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'.", file=sys.stderr )
        print( f"(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.).", file=sys.stderr )

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
    # rows must all simultaneously satisfy (AND; intersection; 'all of these must apply').

    for item in match_all:

        if not isinstance( item, str ) or len( item ) == 0:
            
            print( f"fetch_rows(): ERROR: value assigned to 'match_all' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_all}', which is neither.", file=sys.stderr )

            return

        # Check overall format.

        if re.search( r'^\S+\s+\S+\s+\S.*$', item ) is None:
            
            print( f"fetch_rows(): ERROR: match_all: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.", file=sys.stderr )

            return

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned rows must satisfy at least one (OR; union; 'at least one of these must apply').

    for item in match_any:
        
        if not isinstance( item, str ) or len( item ) == 0:
            
            print( f"fetch_rows(): ERROR: value assigned to 'match_any' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_any}', which is neither.", file=sys.stderr )

            return

        # Check overall format.

        if re.search( r'^\S+\s+\S+\s+\S.*$', item ) is None:
            
            print( f"fetch_rows(): ERROR: match_any: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.", file=sys.stderr )

            return

    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which enumerates user-specified filters on upstream data
    # sources.

    for item in data_source:
        
        if not isinstance( item, str ) or len( item ) == 0:
            
            print( f"fetch_rows(): ERROR: value assigned to the 'data_source' parameter must be a nonempty string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.", file=sys.stderr )

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
            
            print( f"fetch_rows(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{item}', which is not.", file=sys.stderr )

            return

    #############################################################################################################################
    # Enforce mutual exclusivity across different join directives: `add_columns`, `link_to_table` and `provenance`.

    # If it exists, save the name (and, in the following code block, the data type)
    # of the ID field of the table from which we are to join any extra non-`table`
    # columns, so we can present well-formed output later in a consistent way.

    join_table_id_field = None

    if link_to_table != '' and len( add_columns ) > 0:
        
        print( f"fetch_rows(): ERROR: if 'link_to_table' is specified, 'add_columns' cannot also be used. Please choose one of those.", file=sys.stderr )

        return

    elif provenance == True and ( link_to_table != '' or len( add_columns ) > 0 ):
        
        print( f"fetch_rows(): ERROR: if 'provenance' is set to True, neither 'link_to_table' nor 'add_columns' can be used. Please choose one.", file=sys.stderr )

        return

    elif provenance == False:
        
        #############################################################################################################################
        # Manage basic validation for `add_columns`, which enumerates user-specified non-`table` columns to be
        # joined with the main `table` result rows, and `link_to_table`, which specifies an entire non-`table` table
        # to be joined with the main `table` result rows.

        # First: `link_to_table` is just a macro to fetch all the rows from a particular table.
        # Translate it to `add_columns` and process `add_columns` downstream as normal (we ensure
        # above that `add_columns` is always empty whenever `link_to_table` is nonempty -- see
        # the docstring entry for `link_to_table` for context).

        if link_to_table != '':
            
            add_columns = columns( table=link_to_table )['column'].tolist()

        # Eliminate undesirable characters and convert all values to lowercase.

        add_columns = [ re.sub( r'[^a-z0-9_]', r'', column_to_add ).lower() for column_to_add in add_columns ]

        join_tables = set()

        for column_to_add in add_columns:

            columns_response = columns( column=column_to_add )

            if len( columns_response ) != 1:
                
                # There should be exactly one columns() result for a well-defined column name. If there's not one result, fail.

                print( f"fetch_rows(): ERROR: values assigned to 'add_columns' parameter must all be searchable CDA column names: you included '{column_to_add}', which is not.", file=sys.stderr )

                return

            else:
                
                # Log the table from which this column comes.

                join_tables.add( columns_response['table'][0] )

                # Track the data type present in each column, so we can
                # format things properly downstream.

                result_column_data_types[column_to_add] = columns_response['data_type'][0]

        # Adding columns from more than one foreign table is disallowed: results are
        # misleading if not outright incorrect: transitive relationships that do not
        # exist are implied by the concatenation of data from multople foreign
        # tables.

        if len( join_tables ) > 1:
            
            print( f"fetch_rows(): ERROR: extra information from outside the '{table}' table, requested via the 'add_columns' parameter, must all come from a single table. You have specified columns ({add_columns}) from more than one extra table ({sorted( join_tables )}).", file=sys.stderr )

            return

        if len( join_tables ) > 0:
            
            join_table_name = sorted( join_tables )[0]

            # `somatic_mutation` has no row IDs of its own: it is one-to-many subject-to-individual_mutation_observation data (like subject_associated_project), so has no PK and one foreign key into subject.

            if join_table_name != 'somatic_mutation':
                
                join_table_id_field = f"{join_table_name}_id"

                result_column_data_types[join_table_id_field] = columns( column=join_table_id_field )['data_type'][0]

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
            
            print( f"fetch_rows(): ERROR: match_all: requested column '{filter_column_name}' is not a searchable CDA column.", file=sys.stderr )

            return

        # See what the operator is.

        filter_operator = re.sub( r'^\S+\s+(\S+)\s.*', r'\1', item )

        if filter_operator == '==':
            
            # Be kind to computer scientists.

            filter_operator = '='

        elif filter_operator not in allowed_operators:
            
            print( f"fetch_rows(): ERROR: match_all: operator '{filter_operator}' is not supported.", file=sys.stderr )

            return

        # Identify the data type in the column being filtered.

        target_data_type = filter_column_metadata['data_type'][0]

        # Make sure the operator specified is allowed for the data type of the column being filtered.

        if filter_operator not in operators_by_data_type[target_data_type]:
            
            print( f"fetch_rows(): ERROR: match_all: operator '{filter_operator}' is not usable for values of type '{target_data_type}'.", file=sys.stderr )

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
                    
                    print( f"fetch_rows(): ERROR: match_all: requested column {filter_column_name} has data type 'boolean', requiring a true/false value; you specified '{filter_value}', which is neither.", file=sys.stderr )

                    return

                else:
                    
                    filter_value = boolean_alias[filter_value]

            elif target_data_type in [ 'bigint', 'integer', 'numeric' ]:
                
                # If we're supposed to be in a numeric column, make sure we've got a number.

                if re.search( r'^[-+]?\d+(\.\d+)?$', filter_value ) is None:
                    
                    print( f"fetch_rows(): ERROR: match_all: requested column {filter_column_name} has data type '{target_data_type}', requiring a number value; you specified '{filter_value}', which is not.", file=sys.stderr )

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
                    
                    print( f"fetch_rows(): ERROR: match_all: wildcards (*) are only allowed at the ends of string values; string '{original_filter_value}' is noncompliant (it has one in the middle). Please fix.", file=sys.stderr )

                    return

            else:
                
                # Just to be safe. Types change.

                print( f"fetch_rows(): ERROR: match_all: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.", file=sys.stderr )

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
                
                print( f"fetch_rows(): ERROR: match_all: filter query '{filter_column_name} {filter_operator} {filter_value}' is malformed: please use only = and != when filtering on NULL.", file=sys.stderr )

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
            
            print( f"fetch_rows(): ERROR: match_any: requested column '{filter_column_name}' is not a searchable CDA column.", file=sys.stderr )

            return

        # See what the operator is.

        filter_operator = re.sub( r'^\S+\s+(\S+)\s.*', r'\1', item )

        if filter_operator == '==':
            
            # Be kind to computer scientists.

            filter_operator = '='

        elif filter_operator not in allowed_operators:
            
            print( f"fetch_rows(): ERROR: match_any: operator '{filter_operator}' is not supported.", file=sys.stderr )

            return

        # Identify the data type in the column being filtered.

        target_data_type = filter_column_metadata['data_type'][0]

        # Make sure the operator specified is allowed for the data type of the column being filtered.

        if filter_operator not in operators_by_data_type[target_data_type]:
            
            print( f"fetch_rows(): ERROR: match_any: operator '{filter_operator}' is not usable for values of type '{target_data_type}'.", file=sys.stderr )

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
                    
                    print( f"fetch_rows(): ERROR: match_any: requested column {filter_column_name} has data type 'boolean', requiring a true/false value; you specified '{filter_value}', which is neither.", file=sys.stderr )

                    return

                else:
                    
                    filter_value = boolean_alias[filter_value]

            elif target_data_type in [ 'bigint', 'integer', 'numeric' ]:
                
                # If we're supposed to be in a numeric column, make sure we've got a number.

                if re.search( r'^[-+]?\d+(\.\d+)?$', filter_value ) is None:
                    
                    print( f"fetch_rows(): ERROR: match_any: requested column {filter_column_name} has data type '{target_data_type}', requiring a number value; you specified '{filter_value}', which is not.", file=sys.stderr )

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
                    
                    print( f"fetch_rows(): ERROR: match_any: wildcards (*) are only allowed at the ends of string values; string '{original_filter_value}' is noncompliant (it has one in the middle). Please fix.", file=sys.stderr )

                    return

            else:
                
                # Just to be safe. Types change.

                print( f"fetch_rows(): ERROR: match_any: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.", file=sys.stderr )

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
                
                print( f"fetch_rows(): ERROR: match_any: filter query '{filter_column_name} {filter_operator} {filter_value}' is malformed: please use only = and != when filtering on NULL.", file=sys.stderr )

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
    # Parse `add_columns` list: use the API's SELECT and SELECTVALUES operators
    # to build a Query object encoding the given column selections.

    columns_to_fetch = list()

    # Always begin by including all `table` fields, to which any extra
    # columns requested via `add_columns` will be added in each result row.
    # 
    # If anyone wants, we can upgrade later to let users select specific
    # `table` columns to withhold from returned results.

    columns_to_fetch = source_table_columns_in_order.copy()

    # Tracking variable: will our results just include the default
    # column set from `table`?

    use_only_default_columns = True

    # If we're adding extra columns from some non-`table` table*, always
    # include that table's ID field, whether or not it was requested.
    # 
    # *...unless it's `somatic_mutation`, from which we can add extra columns
    # but which doesn't have its own ID field.

    if join_table_id_field is not None:
        
        columns_to_fetch.append( join_table_id_field )

        use_only_default_columns = False

    for column_to_add in add_columns:
        
        # Ignore requests for columns that are already present by default.

        if column_to_add not in columns_to_fetch:
            
            use_only_default_columns = False

            columns_to_fetch.append( column_to_add )

    query_for_extra_columns = None

    if not use_only_default_columns:
        
        query_for_extra_columns = Query()

        query_for_extra_columns.node_type = 'SELECT'

        query_for_extra_columns.l = Query()

        query_for_extra_columns.l.node_type = 'SELECTVALUES'

        query_for_extra_columns.l.value = ', '.join( columns_to_fetch )

    elif table == 'somatic_mutation' and provenance == True:
        
        # We've been asked to provide provenance information for 
        # `somatic_mutation` rows, and "we got them all from ISB-CGC
        # by way of GDC" is likely less helpful than fetching the
        # provenance information for the CDA `subject` rows
        # associated with the `somatic_mutation` rows and attaching
        # that instead. Also, move column `cda_subject_id` to the
        # end of each row so it's next to its own data_source & ID
        # information.

        associated_subject_provenance_columns = [
            
            'cda_subject_id',
            'subject_identifier_system',
            'subject_identifier_field_name',
            'subject_identifier_value'
        ]

        for column in associated_subject_provenance_columns:
            
            result_column_data_types[column] = 'text'

        columns_to_fetch.remove( 'cda_subject_id' )

        columns_to_fetch = columns_to_fetch + associated_subject_provenance_columns

        query_for_extra_columns = Query()

        query_for_extra_columns.node_type = 'SELECT'

        query_for_extra_columns.l = Query()

        query_for_extra_columns.l.node_type = 'SELECTVALUES'

        query_for_extra_columns.l.value = ', '.join( columns_to_fetch )

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

        if table == 'somatic_mutation':
            
            data_source_query.l.value = f"subject_from_{item}"

        else:
            
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

        # `somatic_mutation` has no ID field (and the API blocks us from specifying, say, "subject_alias IS NULL").

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

    # If the user requested extra row data via `add_columns`,
    # `link_to_table` or `provenance`, set that up.
    # 
    # If count_only is True, also cache a version of our combined query
    # _without_ the extra columns, so we can properly count distinct
    # matching `table` rows without having to do a full fetch.

    final_query_without_extra_columns = final_query

    if query_for_extra_columns is not None:
        
        query_for_extra_columns.r = final_query

        final_query = query_for_extra_columns

    if debug:
        
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

        if debug:
            
            # Report that we're pulling in a hostname from the CDA_API_URL environment variable.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: fetch_rows(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: fetch_rows(): Loaded CDA_API_URL from environment", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    else:
        
        api_configuration = CdaConfiguration( verify=True, verbose=True )

        api_client_instance = ApiClient( configuration=api_configuration )

        if debug:
            
            # Report the default location data for the CDA API, as loaded from the CdaConfiguration class.

            print( '-' * 80, file=sys.stderr )

            print( f"BEGIN DEBUG MESSAGE: fetch_rows(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

            print( api_configuration.get_host_settings(), end='\n\n', file=sys.stderr )

            print( '-' * 80, file=sys.stderr )

            print( f"END  DEBUG MESSAGE: fetch_rows(): Loaded CDA API URL from default config", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

    # Alter our debug reports a bit if we're only counting results, instead of fetching them.

    fetch_message = 'fetching all results'

    if count_only:
        
        fetch_message = 'counting results only: not a comprehensive fetch'

    if debug:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: fetch_rows(): Querying CDA API '{table}' endpoint ({fetch_message})", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    # Make a QueryApi object using the connection information in the ApiClient object.

    query_api_instance = QueryApi( api_client_instance )

    query_selector = {
        
        'subject': query_api_instance.subject_query,
        'diagnosis': query_api_instance.diagnosis_query,
        'researchsubject': query_api_instance.research_subject_query,
        'specimen': query_api_instance.specimen_query,
        'treatment': query_api_instance.treatments_query,
        'file': query_api_instance.files,
        'somatic_mutation': query_api_instance.mutation_query
    }

    # Unless we've been asked just to count the anticipated result set, we return all results
    # to users at once. Paging occurs internally, but is made transparent to the user.
    # By default (unless overridden by a `count_only` directive from the user), the
    # following two variables are coded according to CDA performance needs. They
    # should ultimately be moved to a central system-parameter store for easier
    # access: right now, they're replicated everywhere a fetch is performed, which
    # is error-prone when it comes to long-term maintenance.

    starting_offset = 0

    rows_per_page = 500000

    distinct_row_count = None

    if count_only:
        
        # No need to fetch much: we're only counting results. Just grab token
        # rows from the API endpoint and focus on the count totals that
        # come back: one for distinct matching rows, and optionally one
        # other for the total number of result rows that would be returned
        # given the addition of extra columns by the user via the `add_columns`
        # parameter.

        rows_per_page = 1

        if query_for_extra_columns is not None:
            
            # Use the QueryApi instance object's `{table}_query` endpoint-accessor
            # function to get data from the REST API.

            paged_response_data_object = query_selector[table](
                query=final_query_without_extra_columns,
                dry_run=False,
                offset=starting_offset,
                limit=rows_per_page,
                async_req=True,
                include_count=count_only
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

                    print( f"fetch_rows(): ERROR: error message from API: '{error_message}'", file=sys.stderr )

                    return

                except BaseException as e:
                    
                    if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:
                        
                        print( "fetch_rows(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

                    else:
                        
                        print( f"fetch_rows(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

                    return

            distinct_row_count = paged_response_data_object.total_row_count

    # Use the QueryApi instance object's `{table}_query` endpoint-accessor
    # function to get data from the REST API.

    paged_response_data_object = query_selector[table](
        query=final_query,
        dry_run=False,
        offset=starting_offset,
        limit=rows_per_page,
        async_req=True,
        include_count=count_only
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

            print( f"fetch_rows(): ERROR: error message from API: '{error_message}'", file=sys.stderr )

            return

        except BaseException as e:
            
            if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:
                
                print( "fetch_rows(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

            else:
                
                print( f"fetch_rows(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

            return

    if count_only:
        
        # We can short-circuit the rest of this function, now: all we need
        # to return are the result-set counts from the (one or two) API queries we
        # just performed.

        total_result_row_count = paged_response_data_object.total_row_count

        if distinct_row_count is None:
            
            distinct_row_count = total_result_row_count

        return { f"distinct_{table}_rows": distinct_row_count, 'total_result_rows': total_result_row_count }

    # Report some metadata about the results we got back.
    # 
    # print( f"Total row count in result: {paged_response_data_object.total_row_count}", file=sys.stderr )
    # 
    # print( f"Query SQL: {paged_response_data_object.query_sql}", file=sys.stderr )

    """
    # This is immensely verbose, sometimes.

    if debug:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: fetch_rows(): First page of '{table}' endpoint response", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

        print( json.dumps( paged_response_data_object.result, indent=4 ) )

        print( '-' * 80, file=sys.stderr )

        print( f"END DEBUG MESSAGE: fetch_rows(): First page of '{table}' endpoint response", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )
    """

    # Make a Pandas DataFrame out of the first batch of results.
    # 
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function.

    result_dataframe = pd.json_normalize( paged_response_data_object.result )

    # The data we've fetched so far might be just the first page (if the total number
    # of results is greater than `rows_per_page`).
    # 
    # Get the rest of the result pages, if there are any, and add each page's data
    # onto the end of our results DataFrame.

    incremented_offset = starting_offset + rows_per_page

    while paged_response_data_object['next_url'] is not None:
        
        # Show the `next_url` address returned to us by the API.
        # 
        # print( paged_response_data_object['next_url'], file=sys.stderr )

        paged_response_data_object = query_selector[table](
            query=final_query,
            dry_run=False,
            offset=incremented_offset,
            limit=rows_per_page,
            async_req=True,
            include_count=False
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

                print( f"fetch_rows(): ERROR: error message from API: '{error_message}'", file=sys.stderr )

                return

            except BaseException as e:
                
                if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:
                    
                    print( "fetch_rows(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

                else:
                    
                    print( f"fetch_rows(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

                return

        next_result_batch = pd.json_normalize( paged_response_data_object.result )

        if not result_dataframe.empty and not next_result_batch.empty:
            
            # Silence a future deprecation warning about pd.concat and empty DataFrame columns.
            
            next_result_batch = next_result_batch.astype( result_dataframe.dtypes )

            result_dataframe = pd.concat( [ result_dataframe, next_result_batch ] )

        incremented_offset = incremented_offset + rows_per_page

    #############################################################################################################################
    # Postprocess API result data.

    if debug:
        
        print( 'Organizing result data...', file=sys.stderr )

    # Ensure the contents and ordering of the set of default columns for this endpoint
    # is the same whether or not additional column data (from other tables, or provenance
    # metadata for `table` rows) has been requested.

    # Note that we could just filter `result_dataframe` with the 'specify target
    # columns' assignment that we use a little later to sort the remaining output
    # columns, but I think this way is much easier to understand.

    columns_to_drop = list()

    added_columns = list()

    for column_name in result_dataframe:
        
        if column_name not in columns_to_fetch:
            
            columns_to_drop.append( column_name )

        elif column_name not in source_table_columns_in_order:
            
            added_columns.append( column_name )

    if len( columns_to_drop ) > 0:
        
        if debug:
            
            print( f"   -- filtering API columns: {columns_to_drop}", file=sys.stderr )

        result_dataframe = result_dataframe.drop( columns=columns_to_drop )

    # Resequence the output columns according to the sequence given by the columns() function.

    final_column_order = list()

    # First, all the native fields from this endpoint, in the default (relative) order.

    for column in columns_to_fetch:
        
        if column not in added_columns:
            
            final_column_order.append( column )

    # Then the fields from other tables that the user added.

    for added_column in added_columns:
        
        final_column_order.append( added_column )

    if len( result_dataframe.columns ) > 0:
        
        result_dataframe = result_dataframe[ final_column_order ]

        # Joins that transit through intermediate entity tables can come back from the API with phantom missing data (e.g.
        """
        {
            "node_type": "SELECT",
            "l": {
                "node_type": "SELECTVALUES",
                "value": "subject_id, cause_of_death, days_to_birth, days_to_death, ethnicity, race, sex, species, vital_status, diagnosis_id, method_of_diagnosis"
            },
            "r": {
                "node_type": "LIKE",
            "l": {
                "node_type": "column",
                "value": "subject_id"
            },
                "r": {
                    "node_type": "quoted",
                    "value": "TCGA.TCGA-Z2%"
                }
            }
        }
        """
        # ...will produce a weird table with missing diagnosis rows, apparently because it thought it had to bring _something_ back for each researchsubject it checked.
        # 
        # So we strip out all rows whose requested joined table data is missing ID information (if any such extra data was asked for in the first place):

        if join_table_id_field is not None:
            
            result_dataframe = result_dataframe.loc[ ~( result_dataframe[join_table_id_field].isna() ) ]

        if debug:
            
            print( 'Handling missing values...', file=sys.stderr )

        for column in columns_to_fetch:
            
            # CDA has no float values. Cast all numeric data to integers.

            if result_column_data_types[column] in { 'numeric', 'integer', 'bigint' }:
                
                # Columns of type `float64` can contain NaN (missing) values, which cannot (for some reason)
                # be stored in Pandas Series objects (i.e., DataFrame columns) of type `int` or `int64`.
                # Pandas workaround: use extension type 'Int64' (note initial capital), which supports the
                # storage of missing values. These will print as '<NA>'.

                result_dataframe[column] = pd.to_numeric( result_dataframe[column] ).round().astype( 'Int64' )

            elif result_column_data_types[column] in { 'text', 'boolean' }:
                
                # Replace values that are None (== null) with empty strings. (This has been tested and works
                # for both strings and booleans.)

                result_dataframe[column] = result_dataframe[column].fillna( '<NA>' )

            elif result_column_data_types[column] == 'array_of_id_dictionaries':
                
                # All good here, these shouldn't ever be null -- every `table` row has at least one entry in `table`_identifier.

                pass

            else:
                
                # This isn't anticipated. Yell if we get something unexpected.

                print( f"fetch_rows(): ERROR: Unexpected data type `{result_column_data_types[column]}` received; aborting. Please report this event to the CDA development team.", file=sys.stderr )

                return

        # Consolidate provenance information if present.

        if provenance == True:
            
            if table == 'somatic_mutation':
                
                rename_columns = {
                    
                    'subject_identifier_system' : 'subject_data_source',
                    'subject_identifier_field_name': 'subject_data_source_id'
                }

                result_dataframe = result_dataframe.rename( columns=rename_columns )

                result_dataframe['subject_data_source_id'] = result_dataframe['subject_data_source_id'] + ':' + result_dataframe['subject_identifier_value']

                # axis=0: rows; axis=1: columns.

                result_dataframe = result_dataframe.drop( 'subject_identifier_value', axis=1 )

            else:
                
                # We'll need to build a new result matrix, including one copy of
                # each row for each identifier present. Iteratively build a list of
                # tuples (rows) and convert the list to a new DataFrame when complete.

                new_result_matrix = list()

                new_result_column_names = result_dataframe.columns.tolist()

                new_result_column_names.remove( f"{table}_identifier" )

                # There are likely more efficient ways to do this; target this block
                # for optimization if it ever becomes a bottleneck.

                for result_row_index, result_row in result_dataframe.iterrows():
                    
                    identifier_array = result_row[f"{table}_identifier"]

                    for identifier_record in identifier_array:
                        
                        data_source = identifier_record['system']

                        data_source_id = identifier_record['field_name'] + ':' + identifier_record['value']

                        new_row = list()

                        for column_name in new_result_column_names:
                            
                            new_row.append( result_row[column_name] )

                        new_row = new_row + [ data_source, data_source_id ]

                        new_result_matrix.append( tuple( new_row ) )

                new_result_column_names = new_result_column_names + [ f"{table}_data_source", f"{table}_data_source_id" ]
                
                result_dataframe = pd.DataFrame( new_result_matrix, columns=new_result_column_names )

    if return_data_as == '' or return_data_as == 'dataframe':
        
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        return result_dataframe

    elif return_data_as == 'tsv':
        
        # Write results to a user-specified TSV.

        if debug:
            
            print( '-' * 80, file=sys.stderr )

            print( f"      DEBUG MESSAGE: fetch_rows(): Printing results to TSV file '{output_file}'", file=sys.stderr )

            print( '-' * 80, end='\n\n', file=sys.stderr )

        try:
            
            result_dataframe.to_csv( output_file, sep='\t', index=False )

            return

        except Exception as error:
            
            print( f"fetch_rows(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'.", file=sys.stderr )

            return

    print( f"fetch_rows(): ERROR: Something has gone unexpectedly and disastrously wrong with return-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error.", file=sys.stderr )

    return


#############################################################################################################################
# 
# END fetch_rows()
# 
#############################################################################################################################


