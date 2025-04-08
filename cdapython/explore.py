import json
import re
import os

import pandas as pd
from multiprocessing.pool import ApplyResult

import tabulate
import cda_client

# from cda_client.api import ApiException
from .application_utilities import get_api_client, set_log_level, get_logger, cleanup_match_statement
from cda_client.models.q_node import QNode
from cda_client.errors import UnexpectedStatus
from cda_client.api.summary import file_summary_endpoint_summary_file_post as summary_file_endpoint
from cda_client.api.summary import subject_summary_endpoint_summary_subject_post as summary_subject_endpoint




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
    log = get_logger()

    columns_result_df = columns(return_data_as="dataframe")

    if columns_result_df is None:
        log.error("tables(): ERROR: Something went fatally wrong with columns(); can't complete tables(), aborting.")
        return

    else:
        return sorted(columns_result_df["table"].unique())


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


def columns(*, return_data_as="", output_file="", sort_by="", debug = False, **filter_arguments):
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
    set_log_level(log, debug=debug)

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

    banned_columns = ["file_associated_project", "subject_associated_project"]

    #############################################################################################################################
    # Process return-type directives `return_data_as` and `output_file`.

    allowed_return_types = {"", "dataframe", "tsv", "list"}

    if not isinstance(return_data_as, str):
        log.critical(
            f"columns(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'."
        )

        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance(output_file, str):
        log.critical(
            f"columns(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path."
        )

        return

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        # Complain if we receive an unexpected `return_data_as` value.

        log.critical(
            f"columns(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'."
        )

        return

    elif return_data_as == "tsv" and output_file == "":
        # If the user asks for a TSV, they also have to give us a path for that TSV. If they didn't, complain.

        log.critical(
            "columns(): ERROR: return type 'tsv' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/tsv/to'."
        )

        return

    elif return_data_as != "tsv" and output_file != "":
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        msg = f"columns(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'.\n"
        msg += "(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.)."
        log.critical(msg)

        return

    #############################################################################################################################
    # Process `sort_by` directives.

    if isinstance(sort_by, str):
        # Make `sort_by` a list, if it's not, so we don't have to split the way we
        # process this information into parallel distinct branches.

        if sort_by == "":
            sort_by = []

        else:
            sort_by = [sort_by]

    elif not isinstance(sort_by, list):
        # Also detect any disallowed incoming data types and complain if we find any.

        log.error(
            f"columns(): ERROR: 'sort_by' must be a string or a list of strings; you used '{sort_by}', which is neither."
        )

        return

    # Enumerate all allowed values that a user can specify using the `sort_by` parameter. ( 'X:asc' will be aliased immediately to just 'X'. )

    allowed_sort_by_arguments = [
        "table",
        "table:desc",
        "column",
        "column:desc",
        "data_type",
        "data_type:desc",
        "nullable",
        "nullable:desc",
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

            log.critical(
                f"columns(): ERROR: 'sort_by' must be a string or a list of strings; you used '{sort_by}', which is neither."
            )
            return

        # Let's not care about case.

        field_code = field_code.lower()

        # ':asc' is redundant. Remove it (politely).

        field_code = re.sub(r":asc$", r"", field_code)

        if field_code not in allowed_sort_by_arguments:
            # Complain if we receive any unexpected sort_by directives.

            log.critical(
                f"columns(): ERROR: '{field_code}' is not a valid directive for the 'sort_by' parameter. Please use one of [ '"
                + "', '".join(allowed_sort_by_arguments)
                + "' ] instead."
            )

            return

        code_basename = field_code

        if re.search(r":desc$", field_code) is not None:
            code_basename = re.sub(r":desc$", "", field_code)

        if code_basename not in seen_so_far:
            seen_so_far[code_basename] = field_code

        else:
            # Complain if we receive multiple sort_by directives for the same output column.

            log.critical(
                f"columns(): ERROR: Multiple sort_by directives received for the same output column, including '{seen_so_far[code_basename]}' and '{field_code}': please specify only one directive per output column."
            )

            return

        if re.search(r":desc$", field_code) is not None:
            by_list.append(code_basename)

            ascending_list.append(False)

        else:
            by_list.append(field_code)

            ascending_list.append(True)

    # Report details of the final parsed sort logic.

    log.debug("-" * 80)

    log.debug("BEGIN DEBUG MESSAGE: columns(): Processed sort directives")

    log.debug("-" * 80)

    sort_dataframe = pd.DataFrame({"sort_by": by_list, "ascending?": ascending_list})

    log.debug(sort_dataframe)

    log.debug("-" * 80)

    log.debug("END   DEBUG MESSAGE: columns(): Processed sort directives")

    log.debug("-" * 80)

    #############################################################################################################################
    # Process user-supplied filter directives.

    # Enumerate all allowed filters that a user can specify with named parameters.

    allowed_filter_arguments = ["table", "column", "data_type", "nullable", "description", "exclude_table"]

    for filter_argument_name in filter_arguments:
        if filter_argument_name not in allowed_filter_arguments:
            # Complain if we receive any unexpected filter arguments.

            log.critical(f"columns(): ERROR: Received unexpected argument {filter_argument_name}; aborting.")

            return

        elif filter_argument_name == "nullable":
            if not isinstance(filter_arguments[filter_argument_name], bool):
                # Complain if we got a parameter value of the wrong data type.

                log.critical(
                    f"columns(): ERROR: 'nullable' must be a Boolean value (True or False); you used '{filter_arguments[filter_argument_name]}', which is not."
                )

                return

        elif not (
            isinstance(filter_arguments[filter_argument_name], str)
            or isinstance(filter_arguments[filter_argument_name], list)
        ):
            # Complain if we got a parameter value of the wrong data type.

            log.critical(
                f"columns(): ERROR: '{filter_argument_name}' must be a string or a list of strings; you used '{filter_arguments[filter_argument_name]}', which is neither."
            )

            return

        elif isinstance(filter_arguments[filter_argument_name], list):
            for pattern in filter_arguments[filter_argument_name]:
                if not isinstance(pattern, str):
                    # Complain if we receive any unexpected data types inside a filter list (i.e. anything but strings).

                    log.critical(
                        f"columns(): ERROR: '{filter_argument_name}' must be a string or a list of strings; you used '{filter_arguments[filter_argument_name]}', which is neither."
                    )

                    return

        # Validate

    # Report details of fully processed user directives prior to querying.

    status_report = f"return_data_as='{return_data_as}', output_file='{output_file}'"

    status_report = status_report + f", sort_by={sort_by}"

    for filter_argument_name in filter_arguments:
        if isinstance(filter_arguments[filter_argument_name], str):
            status_report = status_report + f", {filter_argument_name}='{filter_arguments[filter_argument_name]}'"

        else:
            status_report = status_report + f", {filter_argument_name}={filter_arguments[filter_argument_name]}"

    log.debug("-" * 80)

    log.debug("BEGIN DEBUG MESSAGE: columns(): Processed filter directives; summary")

    log.debug("-" * 80)

    log.debug("running explore.py columns( " + status_report + " )")

    log.debug("-" * 80)

    log.debug("END   DEBUG MESSAGE: columns(): Processed filter directives; summary")

    log.debug("-" * 80)

    #############################################################################################################################
    # Fetch data from the API.

    query_api_instance = get_api_client()

    # try:
    # Columns Endpoint
    with query_api_instance as client:
        columns_response_data_object = cda_client.api.columns.columns_endpoint_columns_get.sync(client=client)

    # except openapi_client.ApiException as e:
    #    print("Exception when calling ColumnsApi->columns_endpoint_columns_post: %s\n" % e)

    # query_api_instance = QueryApi( api_client_instance )

    # Use the QueryApi instance object's `columns` endpoint-accessor
    # function to get data from the REST API.

    # columns_response_data_object = query_api_instance.columns( async_req=True )

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

    # result_dataframe = pd.DataFrame.from_records(data = [{'table': 'subject', 'column':'sex', 'data_type':'text', 'nullable': False, 'description':'boringdesc'}])
    result_dataframe = pd.DataFrame.from_records(columns_response_data_object.to_dict()["result"])

    # Filter banned columns.

    for banned_column in banned_columns:
        result_dataframe = result_dataframe.loc[result_dataframe["column"] != banned_column]

    log.debug("-" * 80)

    log.debug("      DEBUG MESSAGE: columns(): Created result DataFrame")

    log.debug("-" * 80)

    #############################################################################################################################
    # Execute sorting directives, if we got any; otherwise perform the default sort on the result DataFrame.

    if len(sort_by) == 0:
        # By default, we sort column records by column name, gathered into groups by table,
        # to facilitate predictable output patterns. For easy access, we'd also like each
        # ID column to show up first in its table's group.
        #
        # Temporarily prepend a '.' to all *_id column names, so they float to the top of each
        # table's list of columns when we sort.

        result_dataframe = result_dataframe.replace(to_replace=r"(.*_id)$", value=r".\1", regex=True)

        # Sort all column records, first on table and then on column name.

        result_dataframe = result_dataframe.sort_values(by=["table", "column"], ascending=[True, True])

        # Remove the '.' characters we temporarily prepended to *_id column names
        # to force the sorting algorithm to place all such columns first within each
        # table's group of column records.

        result_dataframe = result_dataframe.replace(to_replace=r"^\.(.*_id)$", value=r"\1", regex=True)

    else:
        # Sort all column records according to the user-specified directives we've processed.

        result_dataframe = result_dataframe.sort_values(by=by_list, ascending=ascending_list)

    log.debug("-" * 80)

    log.debug("      DEBUG MESSAGE: columns(): Applied sort_by directives")

    log.debug("-" * 80)

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

        if filter_name == "nullable":
            return_if_nullable = filter_arguments[filter_name]

            if not isinstance(return_if_nullable, bool):
                log.critical(
                    f"columns(): ERROR: Please specify either nullable=True or nullable=False, not (what you sent) nullable='{return_if_nullable}'."
                )

                return

            result_dataframe = result_dataframe.loc[result_dataframe["nullable"] == return_if_nullable]

        else:
            filters = filter_arguments[filter_name]

            filter_patterns = list()

            # If the filter list wasn't a list at all but a (nonempty) string, we just have
            # one filter. Listify it (so we don't have to care downstream about how many there are).

            if isinstance(filters, str) and filters != "":
                filter_patterns = [filters]

            # Otherwise, just start with the list they sent us.

            elif isinstance(filters, list):
                filter_patterns = filters

            # (If neither of the above conditions was met, `filter_patterns` will remain an
            # empty list, and the rest of this filter-processing section will (by design) have no effect.

            target_field = filter_name

            if filter_name == "description":
                updated_pattern_list = list()

                for original_filter_pattern in filter_patterns:
                    updated_filter_pattern = f"*{original_filter_pattern}*"

                    updated_pattern_list.append(updated_filter_pattern)

                filter_patterns = updated_pattern_list

                target_field = "description"

            elif filter_name == "exclude_table":
                target_field = "table"

            match_pattern_string = ""

            for filter_pattern in filter_patterns:
                # Process wildcard characters.

                if re.search(r"^\*", filter_pattern) is not None:
                    # Any prefix will do, now.
                    #
                    # Strip leading '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                    filter_pattern = re.sub(r"^\*+", r"", filter_pattern)

                else:
                    # No wildcard at the beginning of `filter_pattern` --> require all successful matches to _begin_ with `filter_pattern` by prepending a ^ character to `filter_pattern`:
                    #
                    # ...I know this looks weird, but it's just tacking a '^' character onto the beginning of `filter_pattern`.

                    filter_pattern = re.sub(r"^", r"^", filter_pattern)

                if re.search(r"\*$", filter_pattern) is not None:
                    # Any suffix will do, now.
                    #
                    # Strip trailing '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                    filter_pattern = re.sub(r"\*+$", r"", filter_pattern)

                else:
                    # No wildcard at the end of `filter_pattern` --> require all successful matches to _end_ with `filter_pattern` by appending a '$' character to `filter_pattern`:
                    #
                    # ...I know this looks weird, but it's just tacking a '$' character onto the end of `filter_pattern`.

                    filter_pattern = re.sub(r"$", r"$", filter_pattern)

                # Build the overall match pattern as we go, one (processed) `filter_pattern` at a time.

                match_pattern_string = match_pattern_string + filter_pattern + "|"

            # Strip trailing |.

            match_pattern_string = re.sub(r"\|$", r"", match_pattern_string)

            if filter_name == "exclude_table":
                # Retain all rows where the value of `target_field` (in this case, the value of `table`) does _not_ match any of the given filter patterns.

                result_dataframe = result_dataframe.loc[
                    ~(result_dataframe[target_field].str.contains(match_pattern_string, case=False))
                ]

            else:
                # Retain all rows where the value of `target_field` matches any of the given filter patterns.
                result_dataframe = result_dataframe.loc[
                    result_dataframe[target_field].str.contains(match_pattern_string, case=False)
                ]

    log.debug("-" * 80)

    log.debug("      DEBUG MESSAGE: columns(): Applied value-filtration directives")

    log.debug("-" * 80)

    #############################################################################################################################
    # Send the results back to the user.

    # Reindex DataFrame rows to match their final sort order.

    result_dataframe = result_dataframe.reset_index(drop=True)

    if return_data_as == "":
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        # The following, for the dubiously useful record, is a somewhat worse alternative default thing to do.
        #
        # print( result_dataframe.to_string( index=False, justify='right', max_rows=25, max_colwidth=50 ), file=sys.stdout )

        log.debug("-" * 80)

        log.debug("      DEBUG MESSAGE: columns(): Returning results in default form (pandas.DataFrame)")

        log.debug("-" * 80)

        return result_dataframe

    elif return_data_as == "dataframe":
        # Give the user back the results DataFrame.

        log.debug("-" * 80)

        log.debug("      DEBUG MESSAGE: columns(): Returning results as pandas.DataFrame")

        log.debug("-" * 80)

        return result_dataframe

    elif return_data_as == "list":
        # Give the user back a list of column names.

        log.debug("-" * 80)

        log.debug("      DEBUG MESSAGE: columns(): Returning results as list of column names")

        log.debug("-" * 80)

        return result_dataframe["column"].to_list()

    else:
        # Write the results DataFrame to a user-specified TSV file.

        log.debug("-" * 80)

        log.debug(f"      DEBUG MESSAGE: columns(): Printing results to TSV file '{output_file}'")

        log.debug("-" * 80)

        try:
            result_dataframe.to_csv(output_file, sep="\t", index=False)

            return

        except Exception as error:
            log.critical(
                f"columns(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'."
            )

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
    column="", *, return_data_as="", output_file="", sort_by="", filters=None, data_source="", force=False, debug = False
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


    Returns:
        pandas.DataFrame OR list OR returns nothing, but writes retrieved
        data to a user-specified TSV file
    """
    log = get_logger()
    set_log_level(log, debug=debug)


    #############################################################################################################################
    # Check for our one required parameter.

    if (not isinstance(column, str)) or column == "":
        log.critical(
            "column_values(): ERROR: parameter 'column' cannot be omitted. Please specify a column from which to fetch a list of distinct values."
        )

        return

    # If there's whitespace in our column name, remove it before it does any damage.

    column = re.sub(r"\s+", r"", column)

    # Let's not care about case.

    column = column.lower()

    # # See if columns() agrees that the requested column exists.
    # if len(columns(column=column, return_data_as="list")) == 0:
    #     log.critical(
    #         f"column_values(): ERROR: parameter 'column' must be a searchable CDA column name. You supplied '{column}', which is not."
    #     )

    #     return
    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which describes user-specified filtration on upstream data
    # sources.

    if not isinstance(data_source, str):
        log.critical(
            f"column_values(): ERROR: value assigned to 'data_source' parameter must be a string (e.g. 'GDC'); you specified '{data_source}', which is not."
        )

        return

    # TEMPORARY: enumerate valid `data_source` values and warn the user if they supplied something else.
    # At time of writing this is too expensive to retrieve dynamically from the API,
    # so the valid value list is hard-coded here and in the docstring for this function.
    #
    # This should be replaced ASAP with a fetch from a 'release metadata' table or something
    # similar.

    allowed_data_source_values = {"GDC", "PDC", "IDC", "CDS", "ICDC"}

    if data_source != "":
        # Let us not care about case, and remove any whitespace before it can do any damage.

        data_source = re.sub(r"\s+", r"", data_source).upper()

        if data_source not in allowed_data_source_values:
            log.critical(
                f"column_values(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{data_source}', which is not."
            )

            return

    #############################################################################################################################
    # Check in advance for columns flagged as high-overhead.

    expensive_columns = {"file_id", "byte_size", "checksum", "drs_uri", "file_integer_id_alias", "label"}

    if not force and column in expensive_columns:
        log.critical(
            f"column_values(): WARNING: '{column}' has a very large number of values; retrieval is blocked by default. To perform this query, use column_values( ..., 'force=True' )."
        )

        return

    #############################################################################################################################
    # Listify `filters`, if it's a string, so we can process it in a uniform way later on.

    if filters is None:
        filters = list()

    elif isinstance(filters, str):
        filters = [filters]

    #############################################################################################################################
    # Process return-type directives.

    allowed_return_types = {"", "dataframe", "tsv", "list"}

    if not isinstance(return_data_as, str):
        log.critical(
            f"column_values(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'."
        )

        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame' or return_data_as='TSV'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance(output_file, str):
        log.critical(
            f"column_values(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path."
        )

        return

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        log.critical(
            f"column_values(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe', 'list' or 'tsv'."
        )

        return

    elif return_data_as == "tsv" and output_file == "":
        log.critical(
            "column_values(): ERROR: return type 'tsv' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/tsv/to'."
        )

        return

    elif return_data_as != "tsv" and output_file != "":
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='tsv'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.

        log.critical(
            f"column_values(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'tsv'. You requested return_data_as='{return_data_as}'."
        )
        log.critical("(Note that if you don't specify any value for 'return_data_as', it defaults to 'dataframe'.).")

        return

    #############################################################################################################################
    # Process sorting directives.

    # Enumerate all allowed values that a user can specify using the `sort_by` parameter. ( 'X:asc' will be aliased immediately to just 'X'. )

    allowed_sort_by_options = {
        "list": {"", "value", "value:desc"},
        "dataframe_or_tsv": {"", "count", "count:desc", "value", "value:desc"},
    }

    if not isinstance(sort_by, str):
        # Complain if we receive any unexpected data types instead of string directives.

        log.critical(f"column_values(): ERROR: 'sort_by' must be a string; you used '{sort_by}', which is not.")

        return

    # Let's not care about case.

    sort_by = sort_by.lower()

    # ':asc' is redundant. Remove it (politely).

    sort_by = re.sub(r":asc$", r"", sort_by)

    if return_data_as == "list":
        # Restrict sorting options for lists.

        if sort_by == "":
            sort_by = "value"

        elif sort_by not in allowed_sort_by_options["list"]:
            log.critical(
                f"column_values(): ERROR: return_data_as='list' can only be processed with sort_by='value' or sort_by='value:desc' (or omitting sort_by altogether). Please modify unsupported sort_by directive '{sort_by}' and try again."
            )

            return

    else:
        # For TSV output files and DataFrames, we support more user-configurable options (defaulting to sort_by='count:desc'):

        if sort_by == "":
            sort_by = "count:desc"

        elif sort_by not in allowed_sort_by_options["dataframe_or_tsv"]:
            log.critical(
                f"column_values(): ERROR: unrecognized sort_by '{sort_by}'. Please use one of 'count', 'value', 'count:desc', 'value:desc', 'count:asc' or 'value:asc' (or omit the sort_by parameter altogether)."
            )

            return

    # Report details of the final parsed sort logic.

    log.debug("-" * 80)

    log.debug(
        "BEGIN DEBUG MESSAGE: column_values(): Processed all parameter directives. Calling API to fetch data for:"
    )

    log.debug("-" * 80)

    parameter_dict = {
        "column": column,
        "return_data_as": return_data_as,
        "output_file": output_file,
        "sort_by": sort_by,
        "filters": filters,
        "data_source": data_source,
        "force": force,
    }

    log.debug(parameter_dict)

    log.debug("-" * 80)

    log.debug("END   DEBUG MESSAGE: column_values(): Processed sort directives")

    log.debug("-" * 80)

    #############################################################################################################################
    # Fetch data from the API.

    query_api_instance = get_api_client()
    columnname = column  # str |
    system = data_source  # str |  (optional) (default to '')
    count = True  # bool |  (optional) (default to False)
    total_count = True  # bool |  (optional) (default to False)
    records_per_page = 500000  # int |  (optional)
    starting_offset = 0  # int |  (optional)

    # try:
    # Unique Values Endpoint
    with query_api_instance as client:
        paged_response_data_object = (
            cda_client.api.unique_values.unique_values_endpoint_unique_values_columnname_post.sync(
                client=client,
                columnname=columnname,
                system=system,
                count=count,
                total_count=total_count,
                limit=records_per_page,
                offset=starting_offset,
            )
        )

    # except Exception as e:
    #    print("Exception when calling UniqueValuesApi->unique_values_endpoint_unique_values_columnname_post: %s\n" % e)

    log.debug("-" * 80)

    log.debug("BEGIN DEBUG MESSAGE: column_values(): Querying CDA API 'unique_values' endpoint")

    log.debug("-" * 80)

    # Report some metadata about the results we got back.

    log.debug(f"Number of result rows: {paged_response_data_object.total_row_count}")

    log.debug(f"Query SQL: '{paged_response_data_object.query_sql}'")

    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function.

    result_dataframe = pd.json_normalize(paged_response_data_object.to_dict()["result"])

    # The data we've fetched so far might be just the first page (if the total number
    # of results is greater than `records_per_page`).
    #
    # Get the rest of the result pages, if there are any, and add each page's data
    # onto the end of our results DataFrame.

    incremented_offset = starting_offset + records_per_page

    more_than_one_result_page = False
    if paged_response_data_object.next_url is not None:
        log.debug("Fetching remaining results in pages...")

        more_than_one_result_page = True

    while paged_response_data_object.next_url is not None and len(paged_response_data_object.next_url) > 0:
        log.debug(f"   ...fetching {paged_response_data_object.next_url}...")

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

        # paged_response_data_object = query_api_instance.unique_values(
        #     body=column,
        #     system=data_source,
        #     count=True,
        #     async_req=True,
        #     offset=incremented_offset,
        #     limit=records_per_page,
        #     include_count=True
        # )

        paged_response_data_object = query_api_instance.unique_values_endpoint_unique_values_columnname_post(
            columnname,
            system=system,
            count=count,
            total_count=total_count,
            limit=records_per_page,
            offset=incremented_offset,
        )

        # if isinstance( paged_response_data_object, UniqueValueResponseObj ):

        #     while paged_response_data_object.ready() is False:

        #         paged_response_data_object.wait( 5 )

        #     try:

        #         paged_response_data_object = paged_response_data_object.get()

        #     except ApiException as e:

        #         try:

        #             # Ordinarily, this exception represents a structured complaint
        #             # from the API service that something went wrong. In this case,
        #             # the `body` property of the ApiException object will contain
        #             # a JSON-encoded message generated by the API describing the
        #             # unfortunate circumstance.

        #             error_message = json.loads( e.body )['message']

        #         except:

        #             # Unfortunately, if something goes wrong at the level of the
        #             # HTTP service on which the API relies -- that is, when we
        #             # can't actually communicate with the API as such because
        #             # something's gone wrong with our ability to talk to the web
        #             # server -- the ApiException class is overloaded to encode
        #             # that HTTP protocol error (and not throw any further exceptions),
        #             # instead of handling such events somewhere more appropriate
        #             # (like via a different exception class altogether).

        #             error_message = str( e )

        #         print( f"column_values(): ERROR: error message from API: '{error_message}'.", file=sys.stderr )

        #         return

        #     except BaseException as e:

        #         if re.search( 'urllib3.exceptions.MaxRetryError', str( type(e) ) ) is not None:

        #             print( "column_values(): ERROR: Can't connect to the CDA API service.", file=sys.stderr )

        #         else:

        #             print( f"column_values(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).", file=sys.stderr )

        #         return

        next_result_batch = pd.json_normalize(paged_response_data_object.to_dict()["result"])

        if not result_dataframe.empty and not next_result_batch.empty:
            # Silence a future deprecation warning about pd.concat and empty DataFrame columns.

            next_result_batch = next_result_batch.astype(result_dataframe.dtypes)

            result_dataframe = pd.concat([result_dataframe, next_result_batch])

        incremented_offset = incremented_offset + records_per_page

    if more_than_one_result_page:
        log.debug("...done.")

    log.debug("-" * 80)

    log.debug(
        "END   DEBUG MESSAGE: column_values(): Queried CDA API 'unique_values' endpoint and created result DataFrame"
    )

    log.debug("-" * 80)

    #############################################################################################################################
    # Postprocess API result data, if there is any.

    if len(result_dataframe) == 0:
        return result_dataframe

    log.debug("-" * 80)

    log.debug("BEGIN DEBUG MESSAGE: column_values(): Postprocessing results")

    log.debug("-" * 80)

    log.debug("Casting counts to integers and fixing symmetry for returned column labels...")

    # Term-count values come in as floats. Make them not that.

    if "value_count" not in result_dataframe.columns:
        log.critical("column_values: No column called value_count in api response.")
        return

    result_dataframe["value_count"] = result_dataframe["value_count"].astype(int)

    # `X_id` columns come back labeled just as `id`. Fix.

    if re.search(r"_id$", column) is not None:
        result_dataframe = result_dataframe.rename(columns={"id": column})

    # `X_integer_id_alias` columns come back labeled just as `integer_id_alias`. Fix.

    elif re.search(r"_integer_id_alias$", column) is not None:
        result_dataframe = result_dataframe.rename(columns={"integer_id_alias": column})

    # `X_associated_project` columns come back labeled just as `associated_project`. Fix.

    elif re.search(r"_associated_project$", column) is not None:
        result_dataframe = result_dataframe.rename(columns={"associated_project": column})

    # `X_identifier_Y` columns come back labeled just as `Y`. Fix.

    elif re.search(r"^(.*_identifier_)(.+)$", column) is not None:
        suffix = re.sub(r"^.*_identifier_(.+)$", r"\1", column)

        # Adjust the header the API sent us for the values column.

        result_dataframe = result_dataframe.rename(columns={suffix: column})

    log.debug("Handling missing values...")

    # CDA has no float values. If the API gives us some, cast them to integers.
    
    if result_dataframe[column].dtype == "float64":
        # Columns of type `float64` can contain NaN (missing) values, which cannot (for some reason)
        # be stored in Pandas Series objects (i.e., DataFrame columns) of type `int` or `int64`.
        # Pandas workaround: use extension type 'Int64' (note initial capital), which supports the
        # storage of missing values. These will print as '<NA>'.

        result_dataframe[column] = result_dataframe[column].round().astype("Int64")

    elif result_dataframe[column].dtype == "object":
        # String data comes through as a column with dtype 'object', based on something involving
        # the variability inherent in string lengths.
        #
        # See https://stackoverflow.com/questions/33957720/how-to-convert-column-with-dtype-as-object-to-string-in-pandas-dataframe

        # Replace term values that are None (== null) with empty strings.

        result_dataframe = result_dataframe.fillna("")

    elif result_dataframe[column].dtype == "bool":
        result_dataframe = result_dataframe.fillna("")

    else:
        # This isn't anticipated. Yell if we get something unexpected.

        log.critical(
            f"column_values(): ERROR: Unexpected data type `{result_dataframe[column].dtype}` received; aborting. Please report this event to the CDA development team."
        )

        return

    #############################################################################################################################
    # Filter returned values according to user specifications.

    # Default behavior: all result values must be exact matches to at least one
    # filter (ignoring case). To match end-to-end, we use a ^ to represent
    # the beginning of each value and a $ to indicate the end. If the user
    # specifies wildcards on one or both ends of a filter, we'll remove one or both
    # restrictions as instructed for that filter.

    match_pattern_string = ""

    # If the user includes an empty string in the filters list, make sure we return
    # a count for empty (null) values in addition to any values matching other filters.

    include_null_count = False

    for filter_pattern in filters:
        if filter_pattern == "":
            include_null_count = True

        else:
            # Process wildcard characters.

            if re.search(r"^\*", filter_pattern) is not None:
                # Any prefix will do, now.
                #
                # Strip leading '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                filter_pattern = re.sub(r"^\*+", r"", filter_pattern)

            else:
                # No wildcard at the beginning of `filter_pattern` --> require all successful matches to _begin_ with `filter_pattern` by prepending a ^ character to `filter_pattern`:
                #
                # ...I know this looks weird, but it's just tacking a '^' character onto the beginning of `filter_pattern`.

                filter_pattern = re.sub(r"^", r"^", filter_pattern)

            if re.search(r"\*$", filter_pattern) is not None:
                # Any suffix will do, now.
                #
                # Strip trailing '*' characters off of `filter_pattern` so we don't confuse the downstream matching function.

                filter_pattern = re.sub(r"\*+$", r"", filter_pattern)

            else:
                # No wildcard at the end of `filter_pattern` --> require all successful matches to _end_ with `filter_pattern` by appending a '$' character to `filter_pattern`:
                #
                # ...I know this looks weird, but it's just tacking a '$' character onto the end of `filter_pattern`.

                filter_pattern = re.sub(r"$", r"$", filter_pattern)

            # Build the overall match pattern as we go, one (processed) `filter_pattern` at a time.

            match_pattern_string = match_pattern_string + filter_pattern + "|"

    # Strip the trailing '|' character from the end of the last `filter_pattern`.

    match_pattern_string = re.sub(r"\|$", r"", match_pattern_string)

    print_regex = match_pattern_string

    if include_null_count:
        if print_regex == "":
            print_regex = "(missing values)"

        else:
            print_regex = print_regex + "|(missing values)"

    if print_regex == "":
        print_regex = "(none)"

    else:
        print_regex = f"/{print_regex}/"

    log.debug(f"Applying pattern filters: {print_regex}")

    # Filter results to match the full aggregated regular expression in `match_pattern_string`.

    if include_null_count and match_pattern_string != "":
        result_dataframe = result_dataframe.loc[
            result_dataframe[column].astype(str).str.contains(match_pattern_string, case=False)
            | result_dataframe[column].astype(str).str.contains(r"^$")
            | result_dataframe[column].isna()
        ]

    elif include_null_count:
        result_dataframe = result_dataframe.loc[
            result_dataframe[column].astype(str).str.contains(r"^$") | result_dataframe[column].isna()
        ]

    else:
        # This will return unfiltered results if `match_pattern_string` is empty (i.e. if the user asked for no filters to be applied),
        # and will filter results according to `match_pattern_string` if not.

        result_dataframe = result_dataframe.loc[
            result_dataframe[column].astype(str).str.contains(match_pattern_string, case=False)
        ]

    # Sort results. Default (note that the final value of `sort_by` is determined earlier in this function) is to sort by term count, descending.

    log.debug(f"Applying sort directive '{sort_by}'...")

    if sort_by == "count":
        # Sort by count; break ties among groups of values with identical counts by sub-sorting each such group alphabetically by value.

        result_dataframe = result_dataframe.sort_values(by=["value_count", column], ascending=[True, True])

    elif sort_by == "count:desc":
        # Sort by count, descending; break ties among groups of values with identical counts by sub-sorting each such group alphabetically by value.

        result_dataframe = result_dataframe.sort_values(by=["value_count", column], ascending=[False, True])

    elif sort_by == "value":
        # No need for a sub-sort, here, since values aren't repeated.

        result_dataframe = result_dataframe.sort_values(by=column, ascending=True)

    elif sort_by == "value:desc":
        # No need for a sub-sort, here, since values aren't repeated.

        result_dataframe = result_dataframe.sort_values(by=column, ascending=False)

    else:
        log.error("column_values(): ERROR: something has gone horribly wrong; we should never get here.")

        return

    log.debug("-" * 80)

    log.debug("END   DEBUG MESSAGE: column_values(): Postprocessed results")

    log.debug("-" * 80)

    #############################################################################################################################
    # Send the results back to the user.

    # Reindex DataFrame rows to match their final sort order.

    result_dataframe = result_dataframe.reset_index(drop=True)

    # Pretty-print missing values.

    if result_dataframe[column].dtype == "object":
        # String data comes through as a column with dtype 'object', based on something involving
        # the variability inherent in string lengths.
        #
        # See https://stackoverflow.com/questions/33957720/how-to-convert-column-with-dtype-as-object-to-string-in-pandas-dataframe

        # Replace term values that are None (== null) with empty strings.

        result_dataframe = result_dataframe.replace(r"^$", r"<NA>", regex=True)

    elif result_dataframe[column].dtype == "bool":
        result_dataframe = result_dataframe.replace(r"^$", r"<NA>", regex=True)

    if return_data_as == "":
        # Right now, the default is the same as if the user had
        # specified return_data_as='dataframe'.

        # The following, for the dubiously useful record, is a somewhat worse alternative default thing to do.
        #
        # print( result_dataframe.to_string( index=False, justify='right', max_rows=25, max_colwidth=50 ), file=sys.stdout )

        log.debug("-" * 80)

        log.debug("      DEBUG MESSAGE: column_values(): Returning results in default form (pandas.DataFrame)")

        log.debug("-" * 80)

        return result_dataframe

    elif return_data_as == "dataframe":
        # Give the user back the results DataFrame.

        log.debug("-" * 80)

        log.debug("      DEBUG MESSAGE: column_values(): Returning results as pandas.DataFrame")

        log.debug("-" * 80)

        return result_dataframe

    elif return_data_as == "list":
        # Strip the term-values column out of the results DataFrame and give them to the user as a Python list.

        log.debug("-" * 80)

        log.debug("      DEBUG MESSAGE: column_values(): Returning results as list of column values")

        log.debug("-" * 80)

        return result_dataframe[column].to_list()

    else:
        # Write the results DataFrame to a user-specified TSV file.

        log.debug("-" * 80)

        log.debug(f"      DEBUG MESSAGE: column_values(): Printing results to TSV file '{output_file}'")

        log.debug("-" * 80)

        try:
            result_dataframe.to_csv(output_file, sep="\t", index=False)

            return

        except Exception as error:
            log.critical(
                f"column_values(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'."
            )

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
    table="",
    *,
    return_data_as="",
    output_file="",
    match_all=[],
    match_any=[],
    match_from_file={"input_file": "", "input_column": "", "cda_column_to_match": ""},
    data_source=[],
    add_columns=[],
    exclude_columns=[],
    link_to_table="",
    debug=False,
):
    """
    For a set of rows in a user-specified table that all match a user-specified set of
    filters -- "result rows" -- get a report showing counts of values present in that
    set of rows, profiled across a small set of pre-selected columns.

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
            If multiple values from an added column are all associated with a single
            `table` row, that row will be repeated once for each distinct value, with
            the added data appended to each row.

        exclude_columns ( string or list of strings; optional ):
            One or more columns from a second table to remove from result data from `table`.
            If multiple values from an added column are all associated with a single
            `table` row, that row will be repeated once for each distinct value, with
            the added data appended to each row.

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

    col_values = columns(debug=debug)

    log = get_logger()
    set_log_level(log, debug=debug)


    table_results = pd.DataFrame()

    # Top-level type and sanity checking (i.e. not examining list contents yet): ensure nothing untoward got passed into our parameters.

    if col_values is None:
        log.critical(
            "fetch_rows(): ERROR: Something went fatally wrong with columns(); can't complete tables(), aborting."
        )
        return

    else:
        # So - yes we have a function "def tables()" that does this already, but since we already have the columns data
        # we use this one-liner to extract the tables.
        table_results = sorted(col_values["table"].unique())


    if debug != True and debug != False:
        log.critical(f"summary_counts(): ERROR: The `debug` parameter must be set to True or False; you specified '{debug}', which is neither.")
        return

    #############################################################################################################################
    # Ensure our one required argument exists and is a valid table name.

    if not isinstance(table, str) or table == "":
        log.critical(f"summary_counts(): ERROR: parameter 'table' is required and must be a nonempty string; you supplied '{table}', which is not.")
        return

    valid_tables = tables()

    if table not in valid_tables:
        log.critical(f"summary_counts(): ERROR: parameter 'table' must be a searchable CDA table; you supplied '{table}', which is not.")
        return

    #############################################################################################################################
    # Process return-type directives `return_data_as` and `output_file`.

    allowed_return_types = {"", "dataframe_list", "dict", "json"}

    if not isinstance(return_data_as, str):
        log.critical(f"summary_counts(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe_list', 'dict' or 'json' (or omit the 'return_data_as' parameter altogether).")
        return

    # Let's not be picky if someone wants to give us return_data_as='DataFrame_LIsT' or return_data_as='JSON'

    return_data_as = return_data_as.lower()

    # We can't do much validation on filenames. If `output_file` isn't
    # a locally writeable path, it'll fail when we try to open it for
    # writing. Strip trailing whitespace from both ends and wrap the
    # file-access operation (later, below) in a try{} block.

    if not isinstance(output_file, str):
        log.critical(f"summary_counts(): ERROR: the `output_file` parameter, if not omitted, should be a string containing a path to the desired output file. You supplied '{output_file}', which is not a string, let alone a valid path.")
        return

    output_file = output_file.strip()

    if return_data_as not in allowed_return_types:
        # Complain if we receive an unexpected `return_data_as` value.
        log.critical(f"summary_counts(): ERROR: unrecognized return type '{return_data_as}' requested. Please use one of 'dataframe_list', 'dict' or 'json' (or omit the 'return_data_as' parameter altogether).")
        return

    elif return_data_as == "json" and output_file == "":
        # If the user asks for JSON, they also have to give us a path for the output file. If they didn't, complain.
        print("summary_counts(): ERROR: return type 'json' requested, but 'output_file' not specified. Please specify output_file='some/path/string/to/write/your/json/to'.",)
        return

    elif return_data_as != "json" and output_file != "":
        # If the user put something in the `output_file` parameter but didn't specify `result_data_as='json'`,
        # they most likely want their data saved to a file (so ignoring the parameter misconfiguration
        # isn't safe), but ultimately we can't be sure what they meant (so taking an action isn't safe),
        # so we complain and ask them to clarify.
        log.critical(f"summary_counts(): ERROR: 'output_file' was specified, but this is only meaningful if 'return_data_as' is set to 'json'. You requested return_data_as='{return_data_as}'.")
        log.critical("(Note that if you don't specify any value for 'return_data_as', it defaults to printing tables to the standard output stream and not to an output file.).")
        return

    #############################################################################################################################
    # Define the list of supported filter-string operators.

    allowed_operators = {">", ">=", "<", "<=", "=", "!="}

    #############################################################################################################################
    # Enumerate restrictions on operator use to appropriate data types.

    operators_by_data_type = {
        "bigint": allowed_operators,
        "boolean": {"=", "!="},
        "integer": allowed_operators,
        "numeric": allowed_operators,
        "text": {"=", "!="},
    }

    #############################################################################################################################
    # Enable aliases for various ways to say "True" and "False". (Case will be lowered as soon as each literal is received.)

    boolean_alias = {"true": "true", "t": "true", "false": "false", "f": "false"}

    #############################################################################################################################
    # Manage basic validation for the `match_all` parameter, which enumerates user-specified requirements that returned
    # records must all be simultaneously satisfied (AND; intersection; 'all of these must apply').

    if isinstance(match_all, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        match_all = [match_all]

    if not isinstance(match_all, list):
        log.critical(f"summary_counts(): ERROR: value assigned to 'match_all' parameter must be a filter string or a list of filter strings; you specified '{match_all}', which is neither.")
        return

    for item in match_all:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(f"summary_counts(): ERROR: value assigned to 'match_all' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_all}', which is neither.")
            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(f"summary_counts(): ERROR: match_all: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.")
            return

    #############################################################################################################################
    # Manage basic validation for the `match_any` parameter, which enumerates user-specified requirements for which
    # returned records must satisfy at least one (OR; union; 'at least one of these must apply').

    if isinstance(match_any, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.
        match_any = [match_any]

    if not isinstance(match_any, list):
        log.critical(f"summary_counts(): ERROR: value assigned to 'match_any' parameter must be a filter string or a list of filter strings; you specified '{match_any}', which is neither.")

        return

    for item in match_any:
        if not isinstance(item, str) or len(item) == 0:
            log.critical( f"summary_counts(): ERROR: value assigned to 'match_any' parameter must be a nonempty filter string or a list of nonempty filter strings; you specified '{match_any}', which is neither.")

            return

        # Check overall format.

        if re.search(r"^\S+\s+\S+\s+\S.*$", item) is None:
            log.critical(f"summary_counts(): ERROR: match_any: filter string '{item}' does not conform to 'COLUMN_NAME OP VALUE' format.")
            return

    #############################################################################################################################
    # Manage basic validation for the `match_from_file` parameter, which refers to a target CDA column and a list of allowed
    # values, and constrains summary_counts() to describe only data from rows that contain an allowed value in the target
    # CDA column. Also load column data here from the given TSV, so we can fail early if something goes wrong with the I/O.

    # Top-level type and sanity checking (i.e. not examining dictionary values yet): ensure nothing untoward got passed into `match_from_file`.

    if not isinstance(match_from_file, dict):
        log.critical(f"summary_counts(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not.")
        return

    else:
        received_keys = set(match_from_file.keys())

        expected_keys = {"input_file", "input_column", "cda_column_to_match"}

        if received_keys != expected_keys:
            log.critical(f"summary_counts(): ERROR: value assigned to 'match_from_file' parameter must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match']; you specified '{match_from_file}', which is not.")
            return

    # Cache metadata about this parameter, if it's been used (i.e. if values aren't blank).

    match_from_file_target_column = match_from_file["cda_column_to_match"]

    match_from_file_input_file = match_from_file["input_file"]

    match_from_file_source_column_name = match_from_file["input_column"]

    match_from_file_target_values = set()

    # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
    # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
    # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
    # anger and confusion when results don't match the input set along the given column.

    match_from_file_nulls_allowed = False

    # Make sure the dictionary values are either all zero-length strings or all nonzero-length strings.

    # I know the following isn't efficiently written -- sorry. Was in an unusually urgent hurry and the logic reflects first-round thoughts only.

    if match_from_file_target_column == "":
        if match_from_file["input_file"] != "" or match_from_file["input_column"] != "":
            log.critical(f"summary_counts(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.")
            return

    elif match_from_file["input_file"] == "":
        if match_from_file_target_column != "" or match_from_file["input_column"] != "":
            log.critical(f"summary_counts(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.")
            return

    elif match_from_file["input_column"] == "":
        if match_from_file_target_column != "" or match_from_file["input_file"] != "":
            log.critical(f"summary_counts(): ERROR: if the 'match_from_file' parameter is used, it must be a 3-element dictionary with keys ['input_file', 'input_column', 'cda_column_to_match'] pointing to non-empty values. You specified '{match_from_file}', which is not that.")
            return

    else:
        # See if columns() agrees that the requested column exists.

        if len(columns(column=match_from_file_target_column, return_data_as="list")) == 0:
            log.critical(f"summary_counts(): ERROR: CDA column '{match_from_file_target_column}' (specified in your 'match_from_file' parameter) does not exist. Please see the output of columns() for a list of those that do.")
            return

        if match_from_file_input_file == output_file:
            log.critical(f"summary_counts(): ERROR: You specified the same file ('{output_file}') as both a source of filter values (via 'match_from_file') and the target output file ( via 'output_file'). Please make sure these two files are different.")
            return

        try:
            with open(match_from_file_input_file) as IN:
                column_names = next(IN).rstrip("\n").split("\t")

                if match_from_file_source_column_name not in column_names:
                    log.critical(f"summary_counts(): ERROR: TSV column '{match_from_file_source_column_name}' (specified in your 'match_from_file' parameter) does not exist. Columns in your specified input file ('{match_from_file_input_file}') are:\n\n    {column_names}\n")
                    return

                else:
                    for line in [next_line.rstrip("\n") for next_line in IN]:
                        record = dict(zip(column_names, line.split("\t")))

                        target_value = record[match_from_file_source_column_name]

                        if target_value is None or target_value == "" or target_value == "<NA>":
                            # Interpret missing data as 'empty values allowed' -- if we don't do this, we're setting our users up to (a) create a TSV
                            # from fetched results and then (b) filter downstream queries based on those results subject to a hidden condition that
                            # any results fetched in (a) that have missing values will be ignored when filtering, which seems to me like a recipe for
                            # anger and confusion.

                            match_from_file_nulls_allowed = True

                        else:
                            match_from_file_target_values.add(target_value)

        except Exception as error:
            log.critical(f"summary_counts(): ERROR: Couldn't load requested column '{match_from_file_source_column_name}' from requested TSV file '{match_from_file_input_file}': got error of type '{type(error)}', with error message '{error}'.")
            return

    #############################################################################################################################
    # Manage basic validation for the `data_source` parameter, which enumerates user-specified filters on upstream data
    # sources.

    if isinstance(data_source, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        data_source = [data_source]

    elif not isinstance(data_source, list):
        log.critical(f"summary_counts(): ERROR: value assigned to the 'data_source' parameter must be a string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.")
        return

    for item in data_source:
        if not isinstance(item, str) or len(item) == 0:
            log.critical(f"summary_counts(): ERROR: value assigned to the 'data_source' parameter must be a nonempty string (e.g. 'GDC') or a list of strings (e.g. [ 'GDC', 'CDS' ]); you specified '{data_source}', which is neither.")
            return
        
    # `add_columns`

    if isinstance(add_columns, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        add_columns = [add_columns]

    if not isinstance(add_columns, list):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to 'add_columns' parameter must be a string (e.g. 'primary_diagnosis_site') or a list of strings (e.g. [ 'specimen_type', 'primary_diagnosis_condition' ]); you specified '{add_columns}', which is neither."
        )

        return

    # `exclude_columns`

    if isinstance(exclude_columns, str):
        # Listify, so we don't have to care later about whether this was a string or a list of strings.

        exclude_columns = [exclude_columns]

    if not isinstance(exclude_columns, list):
        log.critical(
            f"fetch_rows(): ERROR: value assigned to 'exclude_columns' parameter must be a string (e.g. 'primary_diagnosis_site') or a list of strings (e.g. [ 'specimen_type', 'primary_diagnosis_condition' ]); you specified '{exclude_columns}', which is neither."
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
            log.critical(f"summary_counts(): ERROR: values assigned to the 'data_source' parameter must be one of { 'GDC', 'PDC', 'IDC', 'CDS', 'ICDC' }. You supplied '{item}', which is not.")
            return

    result_column_data_types = dict()

    # Store the default column ordering as provided by the columns() function,
    # to enable us to always display the same data in the same way.

    source_table_columns_in_order = list()

    table_cols = col_values.query(f'table == "{table}"')

    if table_cols is None:
        # Since we've checked for the existence of table previously, this case should never happen.
        log.critical(f"No such table {table}. Please retry with an existent table.")

        return

    for row_index, column_record in table_cols.iterrows():
        result_column_data_types[column_record["column"]] = column_record["data_type"]

        source_table_columns_in_order.append(column_record["column"])

    #############################################################################################################################
    # Parse `match_all` filter expressions: complain if
    #
    #     * requested columns don't exist
    #     * illegal or type-inappropriate operators are used
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere but at the ends of a filter string
    #
    # ...and save parse results for each filter expression as a separate Query object (to be combined later).

    try:
        queries_for_match_all = cleanup_match_statement(col_values, match_all)
    except Exception as e:
        log.critical(e)
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

    try:
        queries_for_match_any = cleanup_match_statement(col_values, match_any)
    except Exception as e:
        log.critical(e)
        return

    #############################################################################################################################
    # Parse `match_from_file` filter values: complain if
    #
    #     * filter values don't match the data types of the columns they're paired with
    #     * wildcards appear anywhere
    #
    # ...and save parse results as a combined filter expression in a Query object (to be combined with others later).

    # Identify the data type of the target column.

    target_data_type = columns(column=match_from_file_target_column)["data_type"][0]

    processed_target_values = set()

    for target_value in match_from_file_target_values:
        # Validate value types and test for wildcards.

        if target_data_type != "text":
            # Ignore leading and trailing whitespace unless we're dealing with strings.

            target_value = re.sub(r"^\s+", r"", target_value)
            target_value = re.sub(r"\s+$", r"", target_value)

        if target_data_type == "boolean":
            # If we're supposed to be in a boolean column, make sure we've got a true/false value.

            target_value = target_value.lower()

            if target_value not in boolean_alias:
                log.critical(f"summary_counts(): ERROR: match_from_file: requested column {match_from_file_target_column} has data type 'boolean', requiring a true/false value; you specified '{target_value}', which is neither.")
                return

            else:
                target_value = boolean_alias[target_value]

        elif target_data_type in ["bigint", "integer", "numeric"]:
            # If we're supposed to be in a numeric column, make sure we've got a number.

            if re.search(r"^[-+]?\d+(\.\d+)?$", target_value) is None:
                log.critical(f"summary_counts(): ERROR: match_from_file: requested column {match_from_file_target_column} has data type '{target_data_type}', requiring a number value; you specified '{target_value}', which is not.")
                return

        elif target_data_type == "text":
            # Check for wildcards: if found, vomit.

            if re.search(r"\*", target_value) is not None:
                log.critical(f"summary_counts(): ERROR: match_from_file: wildcards (*) are disallowed here (only exact matches are supported for this option); string '{target_value}' is noncompliant. Please fix.")
                return

        else:
            # Just to be safe. Types change.

            log.critical(f"summary_counts(): ERROR: match_from_file: unanticipated `target_data_type` '{target_data_type}', cannot continue. Please report this event to CDA developers.")
            return

        processed_target_values.add(target_value)

    # Build a Query object for the column data loaded according to `match_from_file`.

    query_for_match_from_file = None

    # if match_from_file_nulls_allowed == True:
    #     query_for_match_from_file = Query()

    #     query_for_match_from_file.node_type = "OR"

    #     match_from_file_null_match_subquery = Query()

    #     match_from_file_null_match_subquery.node_type = "IS"

    #     match_from_file_null_match_subquery.l = Query()

    #     match_from_file_null_match_subquery.l.node_type = "column"

    #     match_from_file_null_match_subquery.l.value = match_from_file_target_column

    #     match_from_file_null_match_subquery.r = Query()

    #     match_from_file_null_match_subquery.r.node_type = "unquoted"

    #     match_from_file_null_match_subquery.r.value = "NULL"

    #     query_for_match_from_file.l = match_from_file_null_match_subquery

    #     match_from_file_allowed_values_subquery = Query()

    #     match_from_file_allowed_values_subquery.node_type = "IN"

    #     match_from_file_allowed_values_subquery.l = Query()

    #     match_from_file_allowed_values_subquery.l.node_type = "column"

    #     match_from_file_allowed_values_subquery.l.value = match_from_file_target_column

    #     match_from_file_allowed_values_subquery.r = Query()

    #     match_from_file_allowed_values_subquery.r.node_type = "unquoted"

    #     if target_data_type == "text":
    #         match_from_file_allowed_values_subquery.r.value = (
    #             r'("' + r'","'.join(sorted(processed_target_values)) + r'")'
    #         )

    #     else:
    #         match_from_file_allowed_values_subquery.r.value = r"(" + r",".join(sorted(processed_target_values)) + r")"

    #     query_for_match_from_file.r = match_from_file_allowed_values_subquery

    # elif len(processed_target_values) > 0:
    #     query_for_match_from_file = Query()

    #     query_for_match_from_file.node_type = "IN"

    #     query_for_match_from_file.l = Query()

    #     query_for_match_from_file.l.node_type = "column"

    #     query_for_match_from_file.l.value = match_from_file_target_column

    #     query_for_match_from_file.r = Query()

    #     query_for_match_from_file.r.node_type = "unquoted"

    #     if target_data_type == "text":
    #         query_for_match_from_file.r.value = r'("' + r'","'.join(sorted(processed_target_values)) + r'")'

    #     else:
    #         query_for_match_from_file.r.value = r"(" + r",".join(sorted(processed_target_values)) + r")"

    #############################################################################################################################
    # Parse `data_source` filter expressions: complain if any are nonconformant, and (for now) save parse results for each
    # filter expression as a separate Query object.

    queries_for_data_source = []

    for ds in data_source:
        queries_for_match_all.append(f"{table}_data_at_{ds} = True")


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
    column_values = columns()
    if link_to_table != "":
        add_columns.extend(column_values.query(f'table == "{link_to_table}"')["column"].tolist())

    for column_to_add in add_columns:
        # Ignore requests for columns that are already present by default.

        if column_to_add not in columns_to_fetch:
            use_only_default_columns = False

            columns_to_fetch.append(column_to_add)

    columns_to_remove = []

    for col in exclude_columns:
        # Ignore requests to exclude columns that are already excluded.

        if col in columns_to_fetch:
            columns_to_fetch.remove(col)

            columns_to_remove.append(col)

        else:
            log.debug(f'Ignoring request to remove column "{col}" because it doesn\'t exist or is already excluded.')

    # for item in data_source:
    #     # Build a Query object for this data source and add it to the data_source list.

    #     data_source_query = Query()

    #     data_source_query.node_type = "="

    #     data_source_query.l = Query()

    #     data_source_query.l.node_type = "column"

    #     data_source_query.l.value = f"{table}_from_{item}"

    #     data_source_query.r = Query()

    #     data_source_query.r.node_type = "unquoted"

    #     data_source_query.r.value = "true"

    #     queries_for_data_source.append(data_source_query)

    #############################################################################################################################
    # Combine all match_all filter queries into one big AND-linked query.

    # combined_match_all_query = Query()

    # if len(queries_for_match_all) == 0:
    #     # No filters in this group: nothing to do here.

    #     combined_match_all_query = None

    # elif len(queries_for_match_all) == 1:
    #     # No fancy combination needed. Just pass the API the one filter we've got.

    #     combined_match_all_query = queries_for_match_all[0]

    # else:
    #     # Hook up the first two queries in `queries_for_match_all` with an `AND`.

    #     combined_match_all_query.node_type = "AND"

    #     combined_match_all_query.l = queries_for_match_all[0]

    #     combined_match_all_query.r = queries_for_match_all[1]

    #     if len(queries_for_match_all) > 2:
    #         # Add any remaining queries in `queries_for_match_all`, one at a time.

    #         for i in range(2, len(queries_for_match_all)):
    #             bigger_query = Query()

    #             bigger_query.node_type = "AND"

    #             bigger_query.l = combined_match_all_query

    #             bigger_query.r = queries_for_match_all[i]

    #             combined_match_all_query = bigger_query

    # #############################################################################################################################
    # # Combine all `match_any` filter queries into one big OR-linked query.

    # combined_match_any_query = Query()

    # if len(queries_for_match_any) == 0:
    #     # No filters in this group: nothing to do here.

    #     combined_match_any_query = None

    # elif len(queries_for_match_any) == 1:
    #     # No fancy combination needed. Just pass the API the one filter we've got.

    #     combined_match_any_query = queries_for_match_any[0]

    # else:
    #     # Hook up the first two queries in `queries_for_match_any` with an `OR`.

    #     combined_match_any_query.node_type = "OR"

    #     combined_match_any_query.l = queries_for_match_any[0]

    #     combined_match_any_query.r = queries_for_match_any[1]

    #     if len(queries_for_match_any) > 2:
    #         # Add any remaining queries in `queries_for_match_any`, one at a time.

    #         for i in range(2, len(queries_for_match_any)):
    #             bigger_query = Query()

    #             bigger_query.node_type = "OR"

    #             bigger_query.l = combined_match_any_query

    #             bigger_query.r = queries_for_match_any[i]

    #             combined_match_any_query = bigger_query

    # #############################################################################################################################
    # # Combine all data_source queries into one big AND-linked query.

    # combined_data_source_query = Query()

    # if len(queries_for_data_source) == 0:
    #     # No filters in this group: nothing to do here.

    #     combined_data_source_query = None

    # elif len(queries_for_data_source) == 1:
    #     # No fancy combination needed. Just pass the API the one filter we've got.

    #     combined_data_source_query = queries_for_data_source[0]

    # else:
    #     # Hook up the first two queries in `queries_for_data_source` with an `AND`.

    #     combined_data_source_query.node_type = "AND"

    #     combined_data_source_query.l = queries_for_data_source[0]

    #     combined_data_source_query.r = queries_for_data_source[1]

    #     if len(queries_for_data_source) > 2:
    #         # Add any remaining queries in `queries_for_data_source`, one at a time.

    #         for i in range(2, len(queries_for_data_source)):
    #             bigger_query = Query()

    #             bigger_query.node_type = "AND"

    #             bigger_query.l = combined_data_source_query

    #             bigger_query.r = queries_for_data_source[i]

    #             combined_data_source_query = bigger_query

    #############################################################################################################################
    # Combine all the filters we've processed.

    # final_query = Query()

    # if (
    #     combined_match_all_query is None
    #     and combined_match_any_query is None
    #     and combined_data_source_query is None
    #     and query_for_match_from_file is None
    # ):
    #     # We got no filters. Retrieve everything: make the query we pass to the API
    #     # equivalent to 'id IS NOT NULL', which should match everything.

    #     final_query.node_type = "IS NOT"

    #     final_query.l = Query()

    #     final_query.l.node_type = "column"

    #     final_query.l.value = f"{table}_id"

    #     final_query.r = Query()

    #     final_query.r.node_type = "unquoted"

    #     final_query.r.value = "NULL"

    #     # `somatic_mutation` has no ID field.

    #     if table == "somatic_mutation":
    #         final_query.node_type = "OR"

    #         final_query.l = Query()
    #         final_query.l.node_type = "IS"

    #         final_query.l.l = Query()
    #         final_query.l.l.node_type = "column"
    #         final_query.l.l.value = "hotspot"

    #         final_query.l.r = Query()
    #         final_query.l.r.node_type = "unquoted"
    #         final_query.l.r.value = "NULL"

    #         final_query.r = Query()
    #         final_query.r.node_type = "IS NOT"

    #         final_query.r.l = Query()
    #         final_query.r.l.node_type = "column"
    #         final_query.r.l.value = "hotspot"

    #         final_query.r.r = Query()
    #         final_query.r.r.node_type = "unquoted"
    #         final_query.r.r.value = "NULL"

    # elif combined_data_source_query is None:
    #     if query_for_match_from_file is None:
    #         if combined_match_all_query is not None and combined_match_any_query is None:
    #             final_query = combined_match_all_query

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             final_query = combined_match_any_query

    #         else:
    #             # Join both non-null filter groups with an `AND`.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = combined_match_any_query

    #     else:
    #         if combined_match_all_query is None and combined_match_any_query is None:
    #             final_query = query_for_match_from_file

    #         elif combined_match_all_query is not None and combined_match_any_query is None:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = query_for_match_from_file

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_any_query

    #             final_query.r = query_for_match_from_file

    #         else:
    #             final_query.node_type = "AND"

    #             final_query.l = Query()

    #             final_query.l.node_type = "AND"

    #             final_query.l.l = combined_match_all_query

    #             final_query.l.r = combined_match_any_query

    #             final_query.r = query_for_match_from_file

    # else:
    #     if query_for_match_from_file is None:
    #         if combined_match_all_query is None and combined_match_any_query is None:
    #             final_query = combined_data_source_query

    #         elif combined_match_all_query is not None and combined_match_any_query is None:
    #             # Join both non-null filter groups with an `AND`.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_data_source_query

    #             final_query.r = combined_match_all_query

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             # Join both non-null filter groups with an `AND`.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_data_source_query

    #             final_query.r = combined_match_any_query

    #         else:
    #             # Join all three filter groups via 'AND'.

    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = combined_match_any_query

    #             actually_final_query = Query()

    #             actually_final_query.node_type = "AND"

    #             actually_final_query.l = final_query

    #             actually_final_query.r = combined_data_source_query

    #             final_query = actually_final_query

    #     else:
    #         if combined_match_all_query is None and combined_match_any_query is None:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_data_source_query

    #             final_query.r = query_for_match_from_file

    #         elif combined_match_all_query is not None and combined_match_any_query is None:
    #             final_query.node_type = "AND"

    #             final_query.l = Query()

    #             final_query.l.node_type = "AND"

    #             final_query.l.l = combined_data_source_query

    #             final_query.l.r = combined_match_all_query

    #             final_query.r = query_for_match_from_file

    #         elif combined_match_all_query is None and combined_match_any_query is not None:
    #             final_query.node_type = "AND"

    #             final_query.l = Query()

    #             final_query.l.node_type = "AND"

    #             final_query.l.l = combined_data_source_query

    #             final_query.l.r = combined_match_any_query

    #             final_query.r = query_for_match_from_file

    #         else:
    #             final_query.node_type = "AND"

    #             final_query.l = combined_match_all_query

    #             final_query.r = combined_match_any_query

    #             actually_final_query = Query()

    #             actually_final_query.node_type = "AND"

    #             actually_final_query.l = final_query

    #             actually_final_query.r = Query()

    #             actually_final_query.r.node_type = "AND"

    #             actually_final_query.r.l = combined_data_source_query

    #             actually_final_query.r.r = query_for_match_from_file

    #             final_query = actually_final_query

    query_api_instance = get_api_client()

    q_node = QNode()

    q_node.match_all = queries_for_match_all

    q_node.match_some = queries_for_match_any

    q_node.add_columns = columns_to_fetch

    q_node.exclude_columns = columns_to_remove

    # Dump JSON describing the full combined query structure.
    log.debug(json.dumps(q_node.to_dict(), indent=4, cls=CdaApiQueryEncoder))

    #############################################################################################################################
    # Fetch data from the API.

    # Make an ApiClient object containing the information necessary to connect to the CDA database.

    # Allow users to override the system-default URL for the CDA API by setting their CDA_API_URL
    # environment variable.

    url_override = os.environ.get("CDA_API_URL")
    # TODO: What is this trying to accomplish?
    # if url_override is not None and len(url_override) > 0:
    #     api_configuration = CdaConfiguration(host=url_override, verify=True, verbose=True)

    #     api_client_instance = ApiClient(configuration=api_configuration)

    #     # Report that we're pulling in a hostname from the CDA_API_URL environment variable.

    #     log.debug("-" * 80)
    #     log.debug("BEGIN DEBUG MESSAGE: summary_counts(): Loaded CDA_API_URL from environment")
    #     log.debug("-" * 80, end="\n\n")
    #     log.debug(api_configuration.get_host_settings(), end="\n\n")
    #     log.debug("-" * 80)
    #     log.debug("END  DEBUG MESSAGE: summary_counts(): Loaded CDA_API_URL from environment")
    #     log.debug("-" * 80, end="\n\n")

    # else:
    #     api_configuration = CdaConfiguration(verify=True, verbose=True)

    #     api_client_instance = ApiClient(configuration=api_configuration)

    #     # Report the default location data for the CDA API, as loaded from the CdaConfiguration class.
    #     log.debug("-" * 80)
    #     log.debug("BEGIN DEBUG MESSAGE: summary_counts(): Loaded CDA API URL from default config")
    #     log.debug("-" * 80, "\n\n")
    #     log.debug(api_configuration.get_host_settings(), "\n\n")
    #     log.debug("-" * 80)
    #     log.debug("END  DEBUG MESSAGE: summary_counts(): Loaded CDA API URL from default config")
    #     log.debug("-" * 80, "\n\n")

    # log.debug("-" * 80)
    # log.debug(f"BEGIN DEBUG MESSAGE: summary_counts(): Querying CDA API '{table}/counts' endpoint")
    # log.debug("-" * 80, "\n\n")

    # Make a QueryApi object using the connection information in the ApiClient object.

    query_api_instance = get_api_client()

    # Use the QueryApi instance object's `{table}_counts_query` endpoint-accessor
    # function to get data from the REST API.

    query_selector = {
        "file": summary_file_endpoint,
        "subject": summary_subject_endpoint,
    }

    paged_response_data_object = query_selector[table].sync(client=query_api_instance, body=q_node)

    ### TEMPORARY TRAP: Remove this when mutations/counts/ is fixed
    ###                 and everything should just work; all the
    ###                 downstream processing is already in place.

    if table == "mutation":
        log.critical("summary_counts(): ERROR_WITH_APOLOGIES: summary counts for somatic_mutation are not available at present. Please select any of our other fine tables.")
        return

    # Gracefully fetch asynchronously-generated results once they're ready.

    if isinstance(paged_response_data_object, ApplyResult):
        while paged_response_data_object.ready() is False:
            paged_response_data_object.wait(5)

        try:
            paged_response_data_object = paged_response_data_object.get()

        except UnexpectedStatus as e:
            try:
                # Ordinarily, this exception represents a structured complaint
                # from the API service that something went wrong. In this case,
                # the `body` property of the ApiException object will contain
                # a JSON-encoded message generated by the API describing the
                # unfortunate circumstance.

                error_message = json.loads(e.body)["message"]

            except:
                # Unfortunately, if something goes wrong at the level of the
                # HTTP service on which the API relies -- that is, when we
                # can't actually communicate with the API as such because
                # something's gone wrong with our ability to talk to the web
                # server -- the ApiException class is overloaded to encode
                # that HTTP protocol error (and not throw any further exceptions),
                # instead of handling such events somewhere more appropriate
                # (like via a different exception class altogether).

                error_message = str(e)

            log.critical(f"summary_counts(): ERROR: error message from API: '{error_message}'")

            return

        except BaseException as e:
            if re.search("urllib3.exceptions.MaxRetryError", str(type(e))) is not None:
                log.critical("summary_counts(): ERROR: Can't connect to the CDA API service.")

            else:
                log.critical(f"summary_counts(): ERROR: Something ({type(e)}) went wrong when trying to connect to the API. Please check settings (rerunning the last call with debug=True will give more information).")
            return

    # Report some metadata about the results we got back.
    #
    # print( f"Total row count in result: {paged_response_data_object.total_row_count}", file=sys.stderr )
    #
    # print( f"Query SQL: {paged_response_data_object.query_sql}", file=sys.stderr )

    # This is immensely verbose, sometimes.

    log.debug("-" * 80)
    log.debug(f"BEGIN DEBUG MESSAGE: summary_counts(): First page of '{table}/counts' endpoint response")
    log.debug("-" * 80, "\n\n")
    log.debug(json.dumps(paged_response_data_object.to_dict()["result"], indent=4))
    log.debug("-" * 80)
    log.debug(f"END DEBUG MESSAGE: summary_counts(): First page of '{table}/counts' endpoint response")
    log.debug("-" * 80, "\n\n")

    # Make a Pandas DataFrame out of the first batch of results.
    #
    # The API returns responses in JSON format: convert that JSON into a DataFrame
    # using pandas' json_normalize() function.

    return paged_response_data_object.to_dict()["result"]
    # TODO Fix what is supposed to happen after:
    result_dataframe = pd.json_normalize(paged_response_data_object.to_dict()["result"])

    #############################################################################################################################
    # Postprocess API result data.

    log.debug("Organizing result data...")

    #############################################################################################################################
    # Postprocess API result data.

    # This column duplicates `total_count` when querying somatic_mutation. Remove it.

    if "mutation_id" in result_dataframe:
        result_dataframe = result_dataframe.drop(columns=["mutation_id"])

    # For some reason, the highest-level summary counts come through as floats. Fix that
    # (and rename them while we're at it).

    toplevel_columns_to_fix = {"total_count": f"total_{table}_matches", "file_id": "total_related_files"}

    for result_column in toplevel_columns_to_fix:
        if result_column in result_dataframe:
            result_dataframe[result_column] = result_dataframe[result_column].round().astype(int)

            result_dataframe = result_dataframe.rename(columns={result_column: toplevel_columns_to_fix[result_column]})

    if return_data_as == "" or return_data_as == "dataframe_list":
        # Right now, the default is to print one table to standard output
        # for each DataFrame that would be returned had they requested
        # `return_data_as='dataframe_list'`.

        result_list = list()

        for toplevel_column in [f"total_{table}_matches", "total_related_files"]:
            if toplevel_column in result_dataframe:
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                result_list.append(pd.DataFrame(result_dataframe[toplevel_column], columns=[toplevel_column]))

        for result_column in result_dataframe.columns:
            if result_column not in [f"total_{table}_matches", "total_related_files"]:
                # Copy the column into a new DataFrame, then append the new DataFrame to the result list.

                if result_dataframe[result_column].dtype == "int64":
                    result_dataframe[result_column] = int(result_dataframe[result_column][0])

                elif result_dataframe[result_column].dtype == "object":
                    source_pair_keyword = result_column
                    dest_pair_keyword = result_column

                    if re.search(r"_identifier_system$", result_column) is not None:
                        source_pair_keyword = "system"
                        dest_pair_keyword = f"{table}_data_source"

                    result_column_dict = {dest_pair_keyword: list(), "count": list()}

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
                            print_value = "<NA>"

                            if dict_pair[source_pair_keyword] is not None and dict_pair[source_pair_keyword] != "":
                                print_value = dict_pair[source_pair_keyword]

                            result_column_dict[dest_pair_keyword].append(print_value)

                            result_column_dict["count"].append(dict_pair["count"])

                    result_list.append(
                        pd.DataFrame.from_dict(result_column_dict)
                        .sort_values(by=["count"], ascending=[False])
                        .reset_index(drop=True)
                    )

                else:
                    log.critical(f"summary_counts(): ERROR: unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event.")
                    return

        if return_data_as == "":
            log.debug("-" * 80)
            log.debug("      DEBUG MESSAGE: summary_counts(): Returning results in default form (printing list of tables to standard output)")
            log.debug("-" * 80, "\n\n")

            with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.max_colwidth", 65):
                for dataframe in result_list:
                    # Put the count values first in the display.

                    max_col_width = 80

                    maxcolwidths_list = [None, max_col_width]

                    colalign_list = ["right", "left"]

                    if len(dataframe.columns) == 1:
                        maxcolwidths_list = [None]

                        colalign_list = ["left"]

                    else:
                        # Truncate displayed text values manually and add ellipses. The `tabulate` library doesn't do this on its own (as Pandas does).

                        dataframe[dataframe.columns[0]] = dataframe[dataframe.columns[0]].apply(
                            lambda x: re.sub(f"^(.{{{max_col_width-3}}}).*", r"\1...", x)
                            if (x is not None and len(x) > max_col_width)
                            else x
                        )

                    new_column_ordering = list(reversed(dataframe.columns.tolist()))

                    dataframe = dataframe[new_column_ordering]

                    # Suppress output of confusing row-index column when displaying DataFrame contents and get some control over cell alignment.

                    print(
                        tabulate.tabulate(
                            dataframe,
                            showindex=False,
                            headers=dataframe.columns,
                            tablefmt="double_outline",
                            colalign=colalign_list,
                            maxcolwidths=maxcolwidths_list,
                            disable_numparse=True,
                        )
                    )

            return

        elif return_data_as == "dataframe_list":
            log.debug("-" * 80)
            log.debug("      DEBUG MESSAGE: summary_counts(): Returning results as a list of pandas.DataFrame objects")
            log.debug("-" * 80, "\n\n")
            return result_list

    elif return_data_as == "dict" or return_data_as == "json":
        # Build a Python dictionary to shape returned results.

        result_dict = dict()

        for result_column in result_dataframe.columns:
            if result_dataframe[result_column].dtype == "int64":
                result_dict[result_column] = int(result_dataframe[result_column][0])

            elif result_dataframe[result_column].dtype == "object":
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

                    if re.search(r"_identifier_system$", result_column) is not None:
                        pair_keyword = "system"

                    for dict_pair in result_dataframe[result_column][0]:
                        result_dict[result_column][dict_pair[pair_keyword]] = dict_pair["count"]

            else:
                log.critical(f"summary_counts(): ERROR: unexpected return type '{result_dataframe[result_column].dtype}' observed in result column '{result_column}'; please inform the CDA devs of this event.")
                return

        if return_data_as == "dict":
            log.debug("-" * 80)
            log.debug("      DEBUG MESSAGE: summary_counts(): Returning results as a Python dictionary")
            log.debug("-" * 80, "\n\n")

            return result_dict

        elif return_data_as == "json":
            # Write the results to a user-specified JSON file.
            log.debug("-" * 80)
            log.debug(f"      DEBUG MESSAGE: summary_counts(): Printing results to JSON file '{output_file}'")
            log.debug("-" * 80, "\n\n")

            try:
                with open(output_file, "w") as OUT:
                    json.dump(result_dict, OUT, indent=4, ensure_ascii=True)

                return

            except Exception as error:
                log.critical(f"summary_counts(): ERROR: Couldn't write to requested output file '{output_file}': got error of type '{type(error)}', with error message '{error}'.")
                return

    log.critical("summary_counts(): ERROR: Something has gone unexpectedly and disastrously wrong with return-data postprocessing. Please alert the CDA devs to this event and include details of how to reproduce this error.")

    return


#############################################################################################################################
#
# END summary_counts()
#
#############################################################################################################################


