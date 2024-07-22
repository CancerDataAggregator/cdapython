import json
import pandas as pd
import os
import re
import sys

from cdapython.explore import columns, tables

import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint

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
    match_from_file = { 'input_file': '', 'input_column': '', 'cda_column_to_match': '' },
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
        table ( string; required ):
            The table whose rows are to be filtered and retrieved. (Run the tables()
            function to get a list.)

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
            Restrict results to those where the value of the given CDA
            column matches at least one value from the given column
            in the given TSV file.

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

        output_file ( string; optional ):
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

    # `match_from_file`

    if not isinstance( match_from_file, dict ):
        
        print( f"fetch_rows(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not.", file=sys.stderr )

        return

    else:
        
        received_keys = set( match_from_file.keys() )

        expected_keys = { 'input_file', 'input_column', 'cda_column_to_match' }

        if received_keys != expected_keys:
            
            print( f"fetch_rows(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not.", file=sys.stderr )

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
    if provenance == True and table != 'mutation':
        
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
    # Manage basic validation for the `match_from_file` parameter, which refers to a target CDA column and a list of allowed
    # values, and restricts all returned rows only to those that contain an allowed value in the target CDA column. Also
    # load column data here from the given TSV, so we can fail early if something goes wrong with the I/O.

    # Cache metadata about this parameter, if it's used.
    # ( We already checked above that `match_from_file` is a dictionary with exactly three keys possessing the expected names.)

    match_from_file_target_column = match_from_file['cda_column_to_match']

    match_from_file_input_file = match_from_file['input_file']

    match_from_file_source_column_name = match_from_file['input_column']

    match_from_file_target_values = set()

    # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
    # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
    # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
    # anger and confusion.

    match_from_file_nulls_allowed = False

    # Make sure the dictionary values are either all null or all not null.

    if match_from_file_target_column == '':
        
        if match_from_file['input_file'] != '' or match_from_file['input_column'] != '':
            
            print( f"fetch_rows(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.", file=sys.stderr )

            return

    elif match_from_file['input_file'] == '':
        
        if match_from_file_target_column != '' or match_from_file['input_column'] != '':
            
            print( f"fetch_rows(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.", file=sys.stderr )

            return

    elif match_from_file['input_column'] == '':
        
        if match_from_file_target_column != '' or match_from_file['input_file'] != '':
            
            print( f"fetch_rows(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.", file=sys.stderr )

            return

    else:
        
        # See if columns() agrees that the requested column exists.\
        if len( columns( column=match_from_file_target_column, return_data_as='list' ) ) == 0:
            
            print( f"fetch_rows(): ERROR: CDA column '{match_from_file_target_column}' (specified in your 'match_from_file' parameter) does not exist. Please see the output of columns() for a list of those that do.", file=sys.stderr )

            return

        if match_from_file_input_file == output_file:
            
            print( f"fetch_rows(): ERROR: You specified the same file ('{output_file}') as both a source of filter values (via 'match_from_file') and the target output file ( via 'output_file'). Please make sure these two files are different.", file=sys.stderr )

            return

        try:
            
            with open( match_from_file_input_file ) as IN:
                
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )

                if match_from_file_source_column_name not in column_names:
                    
                    print( f"fetch_rows(): ERROR: TSV column '{match_from_file_source_column_name}' (specified in your 'match_from_file' parameter) does not exist. Columns in your specified input file ('{match_from_file_input_file}') are:\n\n    {column_names}\n", file=sys.stderr )

                    return

                else:
                    
                    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                        
                        record = dict( zip( column_names, line.split( '\t' ) ) )

                        target_value = record[match_from_file_source_column_name]

                        if target_value is None or target_value == '' or target_value == '<NA>':
                            
                            # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
                            # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
                            # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
                            # anger and confusion.

                            match_from_file_nulls_allowed = True

                        else:
                            
                            match_from_file_target_values.add( target_value )

        except Exception as error:
            
            print( f"fetch_rows(): ERROR: Couldn't load requested column '{match_from_file_source_column_name}' from requested TSV file '{match_from_file_input_file}': got error of type '{type(error)}', with error message '{error}'.", file=sys.stderr )

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
        print('fcte ' + str(filter_column_name))

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

        

    #############################################################################################################################
    # Parse `match_from_file` filter values: complain if
    # 
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere
    # 
    # ...and save parse results as a combined filter expression in a Query object (to be combined with others later).

    # Identify the data type of the target column.

    target_data_type = columns( column=match_from_file_target_column )['data_type'][0]

    processed_target_values = set()

    for target_value in match_from_file_target_values:
        
        # Validate value types and test for wildcards.

        if target_data_type != 'text':
            
            # Ignore leading and trailing whitespace unless we're dealing with strings.

            target_value = re.sub( r'^\s+', r'', target_value )
            target_value = re.sub( r'\s+$', r'', target_value )

        if target_data_type == 'boolean':
            
            # If we're supposed to be in a boolean column, make sure we've got a true/false value.

            target_value = target_value.lower()

            if target_value not in boolean_alias:
                
                print( f"fetch_rows(): ERROR: match_from_file: requested column {match_from_file_target_column} has data type 'boolean', requiring a true/false value; you specified '{target_value}', which is neither.", file=sys.stderr )

                return

            else:
                
                target_value = boolean_alias[target_value]

        elif target_data_type in [ 'bigint', 'integer', 'numeric' ]:
            
            # If we're supposed to be in a numeric column, make sure we've got a number.

            if re.search( r'^[-+]?\d+(\.\d+)?$', target_value ) is None:
                
                print( f"fetch_rows(): ERROR: match_from_file: requested column {match_from_file_target_column} has data type '{target_data_type}', requiring a number value; you specified '{target_value}', which is not.", file=sys.stderr )

                return

        elif target_data_type == 'text':
            
            # Check for wildcards: if found, vomit.

            if re.search( r'\*', target_value ) is not None:
                
                print( f"fetch_rows(): ERROR: match_from_file: wildcards (*) are disallowed here (only exact matches are supported for this option); string '{target_value}' is noncompliant. Please fix.", file=sys.stderr )

                return

        else:
            
            # Just to be safe. Types change.

            print( f"fetch_rows(): ERROR: match_from_file: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.", file=sys.stderr )

            return

        processed_target_values.add( target_value )



    #############################################################################################################################
    # Fetch data from the API.

    # Make an ApiClient object containing the information necessary to connect to the CDA database.

    # Allow users to override the system-default URL for the CDA API by setting their CDA_API_URL
    # environment variable.

    url_override = os.environ.get( 'CDA_API_URL' )

    configuration = openapi_client.Configuration(
        host = "http://127.0.0.1:8000"
    )
    with openapi_client.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = openapi_client.DataApi(api_client)
        q_node = openapi_client.QNode() # QNode | 

        q_node.match_all = match_all
        q_node.match_some = match_any
        q_node.add_columns = add_columns
        
        limit = 100 # int |  (optional) (default to 100)
        offset = 0 # int |  (optional) (default to 0)

        try:
            # Default Paged Endpoint
            api_response = api_instance.subject_paged_endpoint_data_subject_post(q_node, limit=limit, offset=offset)
            print("The response of DataApi->subject_paged_endpoint_data_subject_post:\n")
            pprint(api_response)
        except ApiException as e:
            print("Exception when calling DataApi->subject_paged_endpoint_data_subject_post: %s\n" % e)

    # Alter our debug reports a bit if we're only counting results, instead of fetching them.

    fetch_message = 'fetching all results'

    if count_only:
        
        fetch_message = 'counting results only: not a comprehensive fetch'

    if debug:
        
        print( '-' * 80, file=sys.stderr )

        print( f"BEGIN DEBUG MESSAGE: fetch_rows(): Querying CDA API '{table}' endpoint ({fetch_message})", file=sys.stderr )

        print( '-' * 80, end='\n\n', file=sys.stderr )

    # Make a QueryApi object using the connection information in the ApiClient object.

    

    return


#############################################################################################################################
# 
# END fetch_rows()
# 
#############################################################################################################################


