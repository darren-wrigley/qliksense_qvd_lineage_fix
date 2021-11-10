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
import os
import csv
import edcutils

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

    resource_name = ""

    tab_cache = {}

    lineageWriter = csv.writer
    lineage_cache = []

    tables_not_found = []

    links_written = 0


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
        "q": 'core.classType:com.infa.ldm.bi.qlikSense.Table',
        "fq": f"core.resourceName:{resource_name}",
    }
    #  -core.name:"Meta"
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

    # write the expression to file
    if not os.path.exists("tmp"):
        print("creating folder ./tmp")
        os.makedirs("tmp")

    with open(f"./tmp/{table_name}", "w") as f:
        f.write(table_expr.replace("\r", ""))

    # extract the referenced qvd object(s) - there might be >1
    extracted = extract_qvd_names(table_expr, table_name, object)
    print(extracted)
    mem.tables_to_find.extend(extracted.keys())
    mem.qvd_table_sources[table_name] = list(extracted.values())
    mem.qvd_table_sources_short[table_name] = list(extracted.keys())


def extract_qvd_names(expr: str, tab_name: str, target_obj: dict):
    qvds = {}
    print("extracting qvd names from expr...")
    statements = expr.split(";")
    print(f"\texpression has {len(statements)} statements")
    regex = r"\[([^]]+.qvd)\]"
    col_regex = r",\s*(?![^()]*\))"

    st_count = 0

    for statement in statements:
        print("\t\tstatement")
        st_count += 1
        for match in re.findall(regex, statement):
            print(f"\t\t\tmatch...{match}")
            with open(f"./tmp/{tab_name}_{st_count}", "w") as f:
                f.write(statement.replace("\r", ""))

            print("Statement with qvd>>>")
            print(statement)
            print("Statement with qvd<<<")
            st_refs = {}
            # get the table name - the last entry
            table_ref = match.rsplit("\\")[-1].split(".qvd")[0]
            qvds[table_ref] = match

            # column extraction
            load_pos = statement.upper().find("LOAD")
            from_pos = statement.upper().find("FROM")
            col_ref_stmnt = statement[load_pos + 4 : from_pos]
            # get rid of any distinct
            col_ref_stmnt = re.sub(r"distinct", "", col_ref_stmnt, flags=re.I)
            col_stmnts = re.split(col_regex, col_ref_stmnt)
            print(f"columns found... {len(col_stmnts)}")
            for qvd_col in col_stmnts:
                # col_stmnt = qvd_col.replace("")
                print(f"\tcol:{qvd_col.strip()}")
                to_col, fields = split_column_ref(qvd_col.strip())
                st_refs[to_col] = fields

            print(f"pos'-- {load_pos},{from_pos}")
            print(st_refs)

            # find the table
            ref_table_dict = find_ref_table(table_ref)
            if "id" in ref_table_dict:
                print(f"ready to link id {ref_table_dict['id']} to {target_obj['id']}")
                write_lineage(
                    ref_table_dict["id"], target_obj["id"], "core.DataSetDataFlow"
                )

                for ref_col in st_refs:
                    print(f"\tfind col: {ref_col} in target_obj")
                    to_col_id = get_col_id(target_obj, ref_col)
                    for from_name in st_refs[ref_col]:
                        from_col_id = get_col_id(ref_table_dict, from_name)
                        if from_col_id is None or to_col_id is None:
                            print("nones....")
                            continue
                        print(f"\t\tread to link fields... {from_col_id}>>{to_col_id}")
                        write_lineage(
                            from_col_id,
                            to_col_id,
                            "core.DirectionalDataFlow",
                        )

                # find  the columns

    return qvds


def get_col_id(in_obj, name_to_find):
    for dst_obj in in_obj["dstLinks"]:
        if (
            dst_obj["association"] == "com.infa.ldm.bi.qlikSense.TableColumn"
            and dst_obj["name"] == name_to_find
        ):
            return dst_obj["id"]


def write_lineage(from_id, to_id, link_type):
    key = from_id + ">" + to_id
    if key not in mem.lineage_cache:
        mem.lineageWriter.writerow([link_type, "", "", from_id, to_id])
        mem.lineage_cache.append(key)
        mem.links_written += 1


def find_ref_table(table_name):
    print(f"finding table {table_name} in cache={table_name in mem.tab_cache}")

    if table_name in mem.tab_cache:
        print(f"using cache for {table_name}")
        return mem.tab_cache[table_name]

    parameters = {
        "offset": 0,
        "pageSize": 10,
        "q": "core.classType:com.infa.ldm.bi.qlikSense.Table",
        "fq": [f"core.resourceName:{mem.resource_name}", f'core.name:"{table_name}"'],
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

    if total == 1:
        mem.tab_cache[table_name] = resultJson["items"][0]
        return resultJson["items"][0]
    elif total == 0:
        print(f"no object found for or {table_name}")
    else:
        print("0 or >1 items found...")

    mem.tables_not_found.append(table_name)

    # return an empty dict if not found
    return {}


def split_column_ref(in_ref: str):
    print(f"splitting col... {in_ref}")
    ret = {}
    to_col = in_ref
    fm_expr = in_ref
    split_ref = re.split(r"\s+AS\s+", in_ref, flags=re.I)
    print(split_ref)
    if len(split_ref) == 1:
        pass
    else:
        to_col = split_ref[1]
        fm_expr = split_ref[0]

    if "[" in to_col:
        to_col = to_col.replace("[", "").replace("]", "")
    if '"' in to_col:
        to_col = to_col.replace('"', "")

    print(f"to_col={to_col} expr={fm_expr}")
    ref_fields = get_field_possibles(fm_expr)
    ret[to_col] = ref_fields

    print(f"returning:{ret}")
    return to_col, ref_fields


def get_field_possibles(expr: str):
    # check for quoted strings, [] strings and words or words_words
    refs = []
    quoted_strings = re.findall(r'"([^"]+)"', expr)
    square_strings = re.findall(r"\[([^]]+)\]", expr)
    refs.extend(quoted_strings)
    refs.extend(square_strings)
    if len(refs) == 0:
        # find words
        # if there is no "(" - just return the words
        if "(" not in refs:
            refs.append(expr)
        else:
            print("not sure what to do here")

    print(f"\trefs={refs}")
    return refs


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
    mem.resource_name = args.qliksense_resource
    init_lineage(args.outDir)
    find_qliksense_tables(mem.resource_name)

    print(f"\nfound {len(mem.qvd_table_names)} tables to process")
    print(
        f"\t{len(mem.tables_to_find)} tables to find in edc, {len(set(mem.tables_to_find))} unique"
    )
    print("qvd references...")
    print("qvd_file,qvd_table,used_by_table")
    for k, v in mem.qvd_table_sources.items():
        # print(f"\t{k}")
        for qvd in v:
            tab_name = qvd.rsplit("\\")[-1].split(".qvd")[0]
            print(f"{qvd},{tab_name},{k}")

    mem.fLineage.close()
    end_time = time.time()

    # starting custom linege import
    print("calling lineage impport")
    edcutils.createOrUpdateAndExecuteResourceUsingSession(
        mem.edcSession.baseUrl,
        mem.edcSession.session,
        mem.resource_name + "_lineage",
        "template/custom_lineage_template_no_auto.json",
        mem.resource_name + "_lineage.csv",
        args.outDir + "/" + mem.resource_name + "_lineage.csv",
        False,
        "LineageScanner",
    )
    # end of main()

    print(f"tables found: {len(mem.tab_cache)}")
    print(f"lineage links written: {len(mem.links_written)}")
    print(f"tables not found: {len(mem.tables_not_found)}")
    if len(mem.tables_not_found) >0 :
        print(f"\t{mem.tables_not_found}")
    print(f"Finished - run time = {end_time - start_time:.3f} seconds ---")


def init_lineage(out_folder):
    if not os.path.exists(out_folder):
        print(f"creating folder ./{out_folder}")
        os.makedirs(out_folder)

    mem.fLineage = open(
        os.path.join(out_folder, mem.resource_name + "_lineage.csv"), "w"
    )
    mem.lineageWriter = csv.writer(mem.fLineage, lineterminator="\n")
    mem.lineageWriter.writerow(
        [
            "Association",
            "From Connection",
            "To Connection",
            "From Object",
            "To Object",
        ]
    )


if __name__ == "__main__":
    main()
