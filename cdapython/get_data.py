import json
import pandas as pd
import re

import cda_client

from pandas.api.types import is_numeric_dtype

from cdapython.logging_wrappers import get_logger
from cdapython.discover import columns

from cda_client.models.client_error import ClientError
from cda_client.models.internal_error import InternalError
from cda_client.models.q_node import QNode
from cdapython.application_utilities import get_api_client, cleanup_match_statement, cleanup_inputs, verify_inputs, build_match_from_file_filter


#############################################################################################################################
#############################################################################################################################
# Nomenclature notes:
#
# * try to standardize all potential user-facing synonyms for basic database data structures
#   (field, entity, endpoint, cell, value, term, etc.) to "table", "column", "row" and "value".
#############################################################################################################################
#############################################################################################################################

def get_file_data():
    pass

def get_subject_data():
    pass

#############################################################################################################################
#
# get_data( table=`table` ): Get CDA data records ('result rows') from `table` that match user-specified criteria.
#
#############################################################################################################################

def get_data(
    table=None,
    *,
    match_all=[],
    match_any=[],
    match_from_file={"input_file": "", "input_column": "", "cda_column_to_match": ""},
    data_source=[],
    add_columns=[],
    exclude_columns=[],
    provenance=False,
    return_data_as="dataframe",
    output_file=""
):
    """
    Get CDA data records ('result rows') from `table` that match user-specified criteria.

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
            to each result row describing the upstream data sources from
            which it was derived.

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
            will represent one CDA `table` row (possibly with related data from a second table
            appended to it, according to user directives).

        OR returns nothing, but writes results to a user-specified TSV file.

    """

    #############################################################################################################################

    # cache the columns call and tables info so we don't have to call it more than once during get_data
    log = get_logger()
    column_values = columns()

    # Make sure inputs are clean
    match_all, match_any, add_columns, exclude_columns, data_source = cleanup_inputs(match_all, match_any, add_columns, exclude_columns, data_source)

    # Make sure the inputs are what they should be
    verify_inputs(
        column_values,
        match_all, 
        match_any, 
        add_columns, 
        exclude_columns, 
        data_source,
        table,
        match_from_file,
        provenance,
        return_data_as,
        output_file,
        log
        )

    #############################################################################################################################
    # Preprocess table metadata, to enable consistent processing (and reporting) throughout.

    # Track the data type present in each `table` column, so we can
    # format results properly downstream.

    result_column_data_types = dict()

    # Store the default column ordering as provided by the columns() function,
    # to enable us to always display the same data in the same way.

    source_table_columns_in_order = list()

    table_cols = column_values.query(f'table == "{table}"')

    if table_cols is None:
        # Since we've checked for the existence of table previously, this case should never happen.
        log.critical(f"No such table {table}. Please retry with an existent table.")

        return

    for row_index, column_record in table_cols.iterrows():
        result_column_data_types[column_record["column"]] = column_record["data_type"]

        source_table_columns_in_order.append(column_record["column"])
    

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
    if provenance == True and table != "mutation":
        source_table_columns_in_order.append(f"{table}_identifier")

        result_column_data_types[f"{table}_identifier"] = "array_of_id_dictionaries"

    #############################################################################################################################
    # Process return-type directives `return_data_as` and `output_file`.

    allowed_return_types = {"", "dataframe", "tsv"}

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        # Complain if we receive an unexpected `return_data_as` value.

        log.critical(
            f"get_data(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe' or 'tsv'."
        )

        return

    elif return_data_as == "tsv" and output_file == "":
        # If the user asks for TSV, they also have to give us a path for the output file. If they didn't, complain.

        log.critical(
            "get_data(): ERROR: return type 'tsv' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/tsv/to'."
        )

        return

    elif return_data_as != "tsv" and output_file != "":
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        log.critical(
            f"get_data(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'."
        )
        log.critical("(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.).")

        return

    #############################################################################################################################
    # Enable aliases for various ways to say "True" and "False". (Case will be lowered as soon as each literal is received.)

    boolean_alias = {"true": "true", "t": "true", "false": "false", "f": "false"}

    #############################################################################################################################
    # Manage basic validation for the `match_all` parameter, which enumerates user-specified requirements that returned
    # rows must all simultaneously satisfy (AND; intersection; 'all of these must apply').

    for item in match_all:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(
                f"get_data(): ERROR: value assigned to 'match_all' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_all}', which is neither."
            )

            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(
                f"get_data(): ERROR: match_all: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format."
            )

            return

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned rows must satisfy at least one (OR; union; 'at least one of these must apply').

    for item in match_any:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(
                f"get_data(): ERROR: value assigned to 'match_any' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_any}', which is neither."
            )

            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(
                f"get_data(): ERROR: match_any: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format."
            )

            return

    

    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which enumerates user-specified filters on upstream data
    # sources.

    for item in data_source:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(
                f"get_data(): ERROR: value assigned to the 'data_source' parameter must be a nonempty string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither."
            )

            return

    # Let us not care about case, and remove any whitespace before it can do any damage.

    data_source = [re.sub(r"\s+", r"", item).lower() for item in data_source]

    # TEMPORARY: enumerate valid values and warn the user if they supplied something else.
    # At time of writing this is too expensive to retrieve dynamically from the API,
    # so the valid value list is hard-coded here and in the docstring for this function.
    #
    # This should be replaced ASAP with a fetch from a 'release metadata' table or something
    # similar.

    allowed_data_source_values = {"gdc", "pdc", "idc", "cds", "icdc"}

    for item in data_source:
        if item not in allowed_data_source_values:
            log.critical(
                f"get_data(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{item}', which is not."
            )

            return

    #############################################################################################################################
    # UPDATE? :: Enforce mutual exclusivity across different join directives: `add_columns` and `provenance`.

    # If it exists, save the name (and, in the following code block, the data type)
    # of the ID field of the table from which we are to join any extra non-`table`
    # columns, so we can present well-formed output later in a consistent way.


    
    #############################################################################################################################
    # Manage basic validation for `add_columns`, which enumerates user-specified non-`table` columns to be
    # joined with the main `table` result rows.

    # Eliminate undesirable characters and convert all values to lowercase.

    add_columns = [re.sub(r"[^a-z0-9_]", r"", column_to_add).lower() for column_to_add in add_columns]


    try:
        queries_for_match_all = cleanup_match_statement(column_values, match_all)
    except Exception as e:
        log.critical(e)
        return

    try:
        queries_for_match_any = cleanup_match_statement(column_values, match_any)
    except Exception as e:
        log.critical(e)
        return

    if match_from_file['cda_column_to_match'] != '':
        target_data_type = columns(column=match_from_file["cda_column_to_match"])["data_type"][0]
        match_from_file_filter = build_match_from_file_filter(match_from_file, target_data_type, log)
        #TODO should this be added to match_all always?
        queries_for_match_all.append(match_from_file_filter)

    #############################################################################################################################
    # Parse `data_source` filter expressions: complain if any are nonconformant, and (for now) save parse results for each
    # filter expression as a separate Query object.

    # queries_for_data_source = []

    for ds in data_source:
        queries_for_match_all.append(f"{table}_data_at_{ds} = True")

    #############################################################################################################################
    # Parse `add_columns` list: use the API's SELECT and SELECTVALUES operators
    # to build a Query object encoding the given column selections.

    columns_to_fetch = list()

    for column_to_add in add_columns:
        # Ignore requests for columns that are already present by default.

        if column_to_add not in columns_to_fetch:
            columns_to_fetch.append(column_to_add)
    
    columns_to_remove = []

    for col in exclude_columns:
        # Ignore requests to exclude columns that are already excluded.

        if col in columns_to_fetch:
            columns_to_fetch.remove(col)

            columns_to_remove.append(col)

        else:
            log.debug( f'Ignoring request to remove column "{col}" because it doesn\'t exist or is already excluded.' )

    #############################################################################################################################
    # Fetch data from the API.

    query_api_instance = get_api_client()

    q_node = QNode()

    q_node.match_all = queries_for_match_all

    q_node.match_some = queries_for_match_any

    q_node.add_columns = columns_to_fetch

    q_node.exclude_columns = columns_to_remove


    fetch_message = "fetching all results"

    log.debug( f"Querying CDA API '{table}' endpoint ({fetch_message})" )

    query_selector = {
        "file": cda_client.api.data.file_fetch_rows_endpoint_data_file_post,
        "subject": cda_client.api.data.subject_fetch_rows_endpoint_data_subject_post,
    }
    # We return all results to users at once. Paging occurs internally, but is made transparent to the user.
    # The following two variables are coded according to CDA performance needs. They
    # should ultimately be moved to a central system-parameter store for easier
    # access: right now, they're replicated everywhere a fetch is performed, which
    # is error-prone when it comes to long-term maintenance.

    starting_offset = 0

    rows_per_page = 500000

    # Use the QueryApi instance object's `{table}_query` endpoint-accessor
    # function to get data from the REST API.

    log.debug( f"Sending qnode: {q_node}" )
    
    paged_response_data_object = query_selector[table].sync( client=query_api_instance, body=q_node, limit=rows_per_page, offset=starting_offset )

    # Catch errors returned by the API
    if isinstance(paged_response_data_object, ClientError) or isinstance(paged_response_data_object, InternalError):
        msg = f'{paged_response_data_object.error_type}: {paged_response_data_object.message}'
        log.error(msg)
        return


    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function.

    
    result_dataframe = pd.json_normalize(paged_response_data_object.to_dict()["result"])

    # The data we've fetched so far might be just the first page (if the total number
    # of results is greater than `rows_per_page`).
    #
    # Get the rest of the result pages, if there are any, and add each page's data
    # onto the end of our results DataFrame.

    incremented_offset = starting_offset + rows_per_page

    while paged_response_data_object.next_url is not None and len(paged_response_data_object.next_url) > 0:
        # Show the `next_url` address returned to us by the API.
        #
        # print( paged_response_data_object['next_url'], file=sys.stderr )
        log.debug(f'Pulling next paged result from api: {paged_response_data_object.to_dict()['next_url']}')

        paged_response_data_object = query_selector[table].sync(
            client=query_api_instance, body=q_node, offset=incremented_offset, limit=rows_per_page
        )
        #TODO catch api exceptions

        next_result_batch = pd.json_normalize(paged_response_data_object.to_dict()["result"])

        if not result_dataframe.empty and not next_result_batch.empty:
            # Silence a future deprecation warning about pd.concat and empty DataFrame columns.

            #TODO: double check this
            for col in next_result_batch.columns:
                if is_numeric_dtype(next_result_batch[col]):
                    next_result_batch[col] = next_result_batch[col].fillna(0)

            next_result_batch = next_result_batch.astype(result_dataframe.dtypes)

            result_dataframe = pd.concat([result_dataframe, next_result_batch])

        incremented_offset = incremented_offset + rows_per_page

    #############################################################################################################################
    # Postprocess API result data.

    log.debug("Organizing result data...")

    # Ensure the contents and ordering of the set of default columns for this endpoint
    # is the same whether or not additional column data (from other tables, or provenance
    # metadata for `table` rows) has been requested.

    # Note that we could just filter `result_dataframe` with the 'specify target
    # columns' assignment that we use a little later to sort the remaining output
    # columns, but I think this way is much easier to understand.

    # columns_to_drop = list()

    # added_columns = list()

    # for column_name in result_dataframe:
    #     if column_name not in columns_to_fetch:
    #         columns_to_drop.append(column_name)

    #     elif column_name not in source_table_columns_in_order:
    #         added_columns.append(column_name)

    # if len(columns_to_drop) > 0:
    #     log.debug(f"   -- filtering API columns: {columns_to_drop}")

    #     result_dataframe = result_dataframe.drop(columns=columns_to_drop)

    # # Resequence the output columns according to the sequence given by the columns() function.

    # final_column_order = list()

    # # First, all the native fields from this endpoint, in the default (relative) order.

    # for column in columns_to_fetch:
    #     if column not in added_columns:
    #         final_column_order.append(column)

    # # Then the fields from other tables that the user added.

    # for added_column in added_columns:
    #     final_column_order.append(added_column)

    # if len(result_dataframe.columns) > 0:
    #     # result_dataframe = result_dataframe[ final_column_order ]

    #     # Joins that transit through intermediate entity tables can come back from the API with phantom missing data (e.g.
    #     """
    #     {
    #         "node_type": "SELECT",
    #         "l": {
    #             "node_type": "SELECTVALUES",
    #             "value": "subject_id, cause_of_death, days_to_birth, days_to_death, ethnicity, race, sex, species, vital_status, diagnosis_id, method_of_diagnosis"
    #         },
    #         "r": {
    #             "node_type": "LIKE",
    #         "l": {
    #             "node_type": "column",
    #             "value": "subject_id"
    #         },
    #             "r": {
    #                 "node_type": "quoted",
    #                 "value": "TCGA.TCGA-Z2%"
    #             }
    #         }
    #     }
    #     """
    #     # ...will produce a weird table with missing diagnosis rows, apparently because it thought it had to bring _something_ back for each researchsubject it checked.
    #     #
    #     # So we strip out all rows whose requested joined table data is missing ID information (if any such extra data was asked for in the first place):

    #     if join_table_id_field is not None:
    #         result_dataframe = result_dataframe.loc[~(result_dataframe[join_table_id_field].isna())]

    #     log.debug("Handling missing values...")

        # for column in columns_to_fetch:

        #     # CDA has no float values. Cast all numeric data to integers.

        #     print('name: ' + column + ' ' + str(type(result_dataframe[column])) + ' datatypes=' + str(result_column_data_types[column]))

        #     if result_column_data_types[column] in { 'numeric', 'integer', 'bigint' }:

        #         # Columns of type `float64` can contain NaN (missing) values, which cannot (for some reason)
        #         # be stored in Pandas Series objects (i.e., DataFrame columns) of type `int` or `int64`.
        #         # Pandas workaround: use extension type 'Int64' (note initial capital), which supports the
        #         # storage of missing values. These will print as '<NA>'.

        #         result_dataframe[column] = pd.to_numeric( result_dataframe[column] ).round().astype( 'Int64' )

        #     elif result_column_data_types[column] in { 'text', 'boolean' }:

        #         # Replace values that are None (== null) with empty strings. (This has been tested and works
        #         # for both strings and booleans.)

        #         result_dataframe[column] = result_dataframe[column].fillna( '<NA>' )

        #     elif result_column_data_types[column] == 'array_of_id_dictionaries':

        #         # All good here, these shouldn't ever be null -- every `table` row has at least one entry in `table`_identifier.

        #         pass

        #     else:

        #         # This isn't anticipated. Yell if we get something unexpected.

        #         log.critical( f"get_data(): ERROR: Unexpected data type `{result_column_data_types[column]}` received; aborting. Please report this event to the CDA development team." )

        #         return

        # Consolidate provenance information if present.

        # if provenance == True:
        #     if table == "mutation":
        #         rename_columns = {
        #             "subject_identifier_system": "subject_data_source",
        #             "subject_identifier_field_name": "subject_data_source_id",
        #         }

        #         result_dataframe = result_dataframe.rename(columns=rename_columns)

        #         result_dataframe["subject_data_source_id"] = (
        #             result_dataframe["subject_data_source_id"] + ":" + result_dataframe["subject_identifier_value"]
        #         )

        #         # axis=0: rows; axis=1: columns.

        #         result_dataframe = result_dataframe.drop("subject_identifier_value", axis=1)

        #     else:
        #         # We'll need to build a new result matrix, including one copy of
        #         # each row for each identifier present. Iteratively build a list of
        #         # tuples (rows) and convert the list to a new DataFrame when complete.

        #         new_result_matrix = list()

        #         new_result_column_names = result_dataframe.columns.tolist()

        #         new_result_column_names.remove(f"{table}_identifier")

        #         # There are likely more efficient ways to do this; target this block
        #         # for optimization if it ever becomes a bottleneck.

        #         for result_row_index, result_row in result_dataframe.iterrows():
        #             identifier_array = result_row[f"{table}_identifier"]

        #             for identifier_record in identifier_array:
        #                 data_source = identifier_record["upstream_identifiers_data_source"]

        #                 data_source_id = identifier_record["data_source_id_field_name"] + ":" + identifier_record["data_source_id_value"]

        #                 new_row = list()

        #                 for column_name in new_result_column_names:
        #                     new_row.append(result_row[column_name])

        #                 new_row = new_row + [data_source, data_source_id]

        #                 new_result_matrix.append(tuple(new_row))

        #         new_result_column_names = new_result_column_names + [f"{table}_data_source", f"{table}_data_source_id"]

        #         result_dataframe = pd.DataFrame(new_result_matrix, columns=new_result_column_names)

    if return_data_as == "" or return_data_as == "dataframe":
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        return result_dataframe

    elif return_data_as == "tsv":
        # Write results to a user-specified TSV.

        log.debug( f"Printing results to TSV file '{output_file}'" )

        try:
            result_dataframe.to_csv(output_file, sep="\t", index=False)

            return

        except Exception as error:
            log.critical(
                f"get_data(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'."
            )

            return

    log.critical(
        "get_data(): ERROR: Something has gone unexpectedly and disastrously wrong with return-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error."
    )

    return


#############################################################################################################################
#
# END get_data
#
#############################################################################################################################


