import json
import re

import pandas as pd
from pandas.api.types import is_numeric_dtype

import cda_client

# from cda_client.rest import ApiException
from cda_client.models.client_error import ClientError
from cda_client.models.internal_error import InternalError
from cda_client.models.q_node import QNode
from cdapython.application_utilities import get_api_client, set_log_level, get_logger, cleanup_match_statement, cleanup_inputs, verify_inputs, build_match_from_file_filter
from cdapython.explore import columns

SEP = "-" * 80

# Nomenclature notes:
#
# * try to standardize all potential user-facing synonyms for basic database data structures
#   (field, entity, endpoint, cell, value, term, etc.) to "table", "column", "row" and "value".


class CdaApiQueryEncoder(json.JSONEncoder):
    def default(self, o):
        if type(o) == "mappingproxy":
            return None

        tmp_dict = vars(o)

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
    match_all=[],
    match_any=[],
    match_from_file={"input_file": "", "input_column": "", "cda_column_to_match": ""},
    data_source=[],
    add_columns=[],
    exclude_columns=[],
    link_to=[],
    provenance=False,
    count_only=False,
    return_data_as="dataframe",
    output_file="",
    debug=False
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

        exclude_columns ( string or list of strings; optional ):
            One or more columns from a second table to remove from result data from `table`.
            If multiple values from an added column are all associated with a single
            `table` row, that row will be repeated once for each distinct value, with
            the added data appended to each row.


        link_to ( string or list of strings; optional ):
            Other tables from which to fetch entire rows related to the row results
            from `table` that this function produces. `link_to` results
            will be appended to `table` rows to which they're related:
            any `table` row related to more than one `link_to` row will
            be repeated in the returned data, with one distinct `link_to` row
            appended to each repeated copy of its related `table` row.
            If `link_to` is specified, `add_columns` cannot be used.

        provenance ( boolean; optional ):
            If True, fetch_rows() will attach cross-reference information
            to each row result describing the upstream data sources from
            which it was derived. Rows deriving from more than one upstream
            source will be repeated in the output, once per data source, as
            with `link_to` and `add_columns` (except with provenance
            metadata attached, instead of information from other CDA tables).
            If `provenance` is set to True, `link_to` and `add_columns`
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
            rows (for example by using `link_to` or `add_columns` or
            `provenance`). If `count_only` is set to False (the default), fetch_rows() will
            return a pandas DataFrame containing all CDA `table` rows that match the
            given filters.

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
            `link_to` or `add_columns`, because `table` rows will be repeated for any
            one-to-many associations that are returned; otherwise they will be the same.

        OR returns nothing, but writes results to a user-specified TSV file

    """

    #############################################################################################################################

    # cache the columns call and tables info so we don't have to call it more than once during fetch_rows
    log = get_logger()
    column_values = columns()
    set_log_level(log, debug=debug)

    # Make sure inputs are clean
    match_all, match_any, add_columns, exclude_columns, data_source, link_to = cleanup_inputs(match_all, match_any, add_columns, exclude_columns, data_source, link_to)

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
        link_to,
        provenance,
        return_data_as,
        output_file,
        count_only,
        log
        )

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
            f"fetch_rows(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe' or 'tsv'."
        )

        return

    elif return_data_as == "tsv" and output_file == "":
        # If the user asks for TSV, they also have to give us a path for the output file. If they didn't, complain.

        log.critical(
            "fetch_rows(): ERROR: return type 'tsv' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/tsv/to'."
        )

        return

    elif return_data_as != "tsv" and output_file != "":
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        log.critical(
            f"fetch_rows(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'."
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
                f"fetch_rows(): ERROR: value assigned to 'match_all' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_all}', which is neither."
            )

            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(
                f"fetch_rows(): ERROR: match_all: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format."
            )

            return

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned rows must satisfy at least one (OR; union; 'at least one of these must apply').

    for item in match_any:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(
                f"fetch_rows(): ERROR: value assigned to 'match_any' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_any}', which is neither."
            )

            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(
                f"fetch_rows(): ERROR: match_any: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format."
            )

            return

    

    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which enumerates user-specified filters on upstream data
    # sources.

    for item in data_source:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(
                f"fetch_rows(): ERROR: value assigned to the 'data_source' parameter must be a nonempty string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither."
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
                f"fetch_rows(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{item}', which is not."
            )

            return

    #############################################################################################################################
    # Enforce mutual exclusivity across different join directives: `add_columns`, `link_to_table` and `provenance`.

    # If it exists, save the name (and, in the following code block, the data type)
    # of the ID field of the table from which we are to join any extra non-`table`
    # columns, so we can present well-formed output later in a consistent way.


    
    #############################################################################################################################
    # Manage basic validation for `add_columns`, which enumerates user-specified non-`table` columns to be
    # joined with the main `table` result rows, and `link_to_table`, which specifies an entire non-`table` table
    # to be joined with the main `table` result rows.

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
    
    if link_to != []:
        columns_to_fetch.extend(link_to)

    columns_to_remove = []

    for col in exclude_columns:
        # Ignore requests to exclude columns that are already excluded.

        if col in columns_to_fetch:
            columns_to_fetch.remove(col)

            columns_to_remove.append(col)

        else:
            log.debug(f'Ignoring request to remove column "{col}" because it doesn\'t exist or is already excluded.')

    #############################################################################################################################
    # Fetch data from the API.

    query_api_instance = get_api_client()

    q_node = QNode()

    q_node.match_all = queries_for_match_all

    q_node.match_some = queries_for_match_any

    q_node.add_columns = columns_to_fetch

    q_node.exclude_columns = columns_to_remove


    fetch_message = "fetching all results"

    if count_only:
        fetch_message = "counting results only: not a comprehensive fetch"

    log.debug(f"BEGIN DEBUG MESSAGE: fetch_rows(): Querying CDA API '{table}' endpoint ({fetch_message})")

    query_selector = {
        "file": cda_client.api.data.file_fetch_rows_endpoint_data_file_post,
        "subject": cda_client.api.data.subject_fetch_rows_endpoint_data_subject_post,
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

    # Use the QueryApi instance object's `{table}_query` endpoint-accessor
    # function to get data from the REST API.

    log.debug(f"Sending qnode: {q_node}")
    
    paged_response_data_object = query_selector[table].sync(
        client=query_api_instance, body=q_node, limit=rows_per_page, offset=starting_offset
    )

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

    log.debug('Completed fetching rows from API')


    if return_data_as == "" or return_data_as == "dataframe":
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        return result_dataframe

    elif return_data_as == "tsv":
        # Write results to a user-specified TSV.
        log.debug(f"{SEP}\n      DEBUG MESSAGE: fetch_rows(): Printing results to TSV file '{output_file}'\n{SEP}")


        try:
            result_dataframe.to_csv(output_file, sep="\t", index=False)

            return

        except Exception as error:
            log.critical(
                f"fetch_rows(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'."
            )

            return

    log.critical(
        "fetch_rows(): ERROR: Something has gone unexpectedly and disastrously wrong with return-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error."
    )

    return


#############################################################################################################################
#
# END fetch_rows
#
#############################################################################################################################
