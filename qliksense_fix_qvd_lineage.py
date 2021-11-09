"""
Author:  dwrigley
Purpose: if the qliksense scanner for EDC is not creating lineage between qvd tables
         this script will :-
         - find any tables in the resource (-rn command-line parameter)
           - for each table
             - look at the expression attribute, if it has a (qvd) reference
               - find the referenced qvd table
                 - if found - will generate linegae at table and column level
                   using direct id's (sinde we are looking them up here)

         - optionally, create a custom lineage resource and execute the import
"""
import urllib3
import argparse
import time
from edcSessionHelper import EDCSession
import re

urllib3.disable_warnings()

# set edc helper session + variables (easy/re-useable connection to edc api)
# edcSession = EDCSession()


class mem:
    # memory objects - easier than global vars
    edcSession: EDCSession = EDCSession()
    qvd_table_names = []
    tables_to_find = []
    qvd_table_sources = {}  # key = table name, val=list of qvd refs
    qvd_table_sources_short = {}  # key = table name, val=list of table names


def setup_cmd_parser():
    parser = argparse.ArgumentParser(parents=[mem.edcSession.argparser])
    # define script command-line parameters (in global scope for gooey/wooey)

    # add args specific to this utility (left/right resource, schema, classtype...)
    parser.add_argument(
        "-f",
        "--csvFileName",
        default="qliksense_qvd_lineage.csv",
        required=False,
        help=(
            "csv file to create/write (no folder) " "default=qliksense_qvd_lineage.csv "
        ),
    )
    parser.add_argument(
        "-o",
        "--outDir",
        default="out",
        required=False,
        help=(
            "output folder to write results - default = ./out "
            " - will create folder if it does not exist"
        ),
    )

    parser.add_argument(
        "-i",
        "--edcimport",
        default=False,
        # type=bool,
        action="store_true",
        help=(
            "use the rest api to create the custom lineage resource "
            "and start the import process"
        ),
    )

    parser.add_argument(
        "-rn",
        "--qliksense_resource",
        default="qliksense",
        required=True,
        help=(
            "custom lineage resource name to create/update - default value=qliksense"
        ),
    )
    return parser


def find_qliksense_tables(resource_name: str):
    print(f"finding tables in resource {resource_name}")
    parameters = {
        "offset": 0,
        "pageSize": 500,
        "q": "core.classType:com.infa.ldm.bi.qlikSense.Table",
        "fq": f"core.resourceName:{resource_name}",
    }
    print(f"\t\tsearching using parms: {parameters}")

    # execute catalog rest call, for a page of results
    resp = mem.edcSession.session.get(
        mem.edcSession.baseUrl + "/access/2/catalog/data/objects",
        params=parameters,
    )
    status = resp.status_code
    if status != 200:
        # some error - e.g. catalog not running, or bad credentials
        print("error! " + str(status) + str(resp.json()))
        return None

    resultJson = resp.json()
    total = resultJson["metadata"]["totalCount"]
    print(f"objects found: {total}")

    for item in resultJson["items"]:
        process_qliksense_table(item)


def process_qliksense_table(object: dict):
    app_name = get_parent_obj_name(object)
    table_name = getFactValue(object, "core.name")
    table_expr = getFactValue(object, "com.infa.ldm.bi.qlikSense.Expression")
    has_qvd_ref = "(qvd)" in table_expr
    print(f"processing table:{table_name} qvd_ref:{has_qvd_ref} app={app_name}")
    if not has_qvd_ref:
        print("\ttable has no qvd ref, skipping")
        return
    mem.qvd_table_names.append(table_name)

    # extract the referenced qvd object(s) - there might be >1
    extracted = extract_qvd_names(table_expr)
    print(extracted)
    mem.tables_to_find.extend(extracted.keys())
    mem.qvd_table_sources[table_name] = list(extracted.values())
    mem.qvd_table_sources_short[table_name] = list(extracted.keys())


def extract_qvd_names(expr: str):
    qvds = {}
    print("extracting qvd names from expr...")
    regex = r"\[([^]]+.qvd)\]\(qvd\)"
    for match in re.findall(regex, expr):
        print(f"\tmatch...{match}")
        # get the table name - the last entry
        table_ref = match.rsplit("\\")[-1].split(".qvd")[0]
        qvds[table_ref] = match
    return qvds


def get_parent_obj_name(object: dict):
    """
    given a qliksense object- look at the com.infa.ldm.bi.qlikSense.ApplicationTable
    association and get the name
    """
    for assoc in object["srcLinks"]:
        if assoc["association"] == "com.infa.ldm.bi.qlikSense.ApplicationTable":
            return assoc["name"]
    # not found
    return "<<unknown>>"


def getFactValue(item, attrName):
    """
    returns the value of a fact (attribute) from an item

    iterates over the "facts" list - looking for a matching attributeId
    to the paramater attrName
    returns the "value" property or ""
    """
    # get the value of a specific fact from an item
    value = ""
    for facts in item["facts"]:
        if facts.get("attributeId") == attrName:
            value = facts.get("value")
            break
    return value


def main():
    # read command-line parms, init edc connection and start the process
    print("Qliksense EDC Scanner - QVD lineage fixer")
    start_time = time.time()
    cmd_parser = setup_cmd_parser()
    args, unknown = cmd_parser.parse_known_args()
    # setup edc session and catalog url - with auth in the session header,
    # by using system vars or command-line args
    mem.edcSession.initUrlAndSessionFromEDCSettings()
    print(f"command-line args parsed = {args} ")

    # since -rn is mandatoy, we only get here if a resource is specified
    find_qliksense_tables(args.qliksense_resource)

    print(f"\nfound {len(mem.qvd_table_names)} tables to process")
    print(
        f"\t{len(mem.tables_to_find)} tables to find in edc, {len(set(mem.tables_to_find))} unique"
    )

    end_time = time.time()
    # end of main()
    print(f"Finished - run time = {end_time - start_time:.3f} seconds ---")


if __name__ == "__main__":
    main()
