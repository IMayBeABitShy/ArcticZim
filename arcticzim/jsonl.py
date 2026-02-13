"""
Utilities for working with jsonl files.
"""
import argparse
import os
import json
import pprint
import math
import datetime

from tqdm.auto import tqdm


def process_jsonl(path, desc="Reading file"):
    """
    Process a jsonl file, yielding each element.

    @param path: path to the file to read
    @type path: L{str}
    @param desc: description for the tqdm progressbar
    @type desc: L{str}
    @yields: each element in the jsonl file
    @ytype: a json element, usually a L{dict}
    """
    with open(path, "r") as fin:
        total_size = fin.seek(0, os.SEEK_END)
        fin.seek(0, os.SEEK_SET)
        entry = 0

        with tqdm(desc=desc, total=total_size, unit="B", unit_scale=True, unit_divisor=1024) as t:

            for line in fin:
                entry += 1
                t.set_postfix(entry=entry, refresh=False)
                t.update(len(line))
                sline = line.strip()
                if sline:
                    yield json.loads(sline)


def analyze_jsonl(path):
    """
    Analyze the contents of a jsonl file, returning info about the keys and values.

    @param path: path to the file to analyze
    @type path: L{str}
    @return: details about the data
    @rtype: L{dict}
    """
    fields = {}
    first = True
    for element in process_jsonl(path=path, desc="Analyzing file"):
        keys = element.keys()
        for key in keys:
            value = element[key]
            if key not in fields:
                fields[key] = {
                    "always_present": first,
                    "types": set(),
                    "nullable": False,
                    "example": value,
                    "count": 0,
                }
            fields[key]["types"].add(type(value))
            fields[key]["count"] += 1
            if value and not fields[key]["example"]:
                fields[key]["example"] = value
            if value is None:
                fields[key]["nullable"] = True
            if isinstance(value, (str, list, tuple)):
                length = len(value)
                if ("max_length" not in fields[key]) or (fields[key]["max_length"] < length):
                    fields[key]["max_length"] = length
            if isinstance(value, (int, float)):
                if ("min_value" not in fields[key]) or (fields[key]["min_value"] > value):
                    fields[key]["min_value"] = value
                if ("max_value" not in fields[key]) or (fields[key]["max_value"] < value):
                    fields[key]["max_value"] = value

            for existing_key in fields:
                if existing_key not in keys:
                    fields[existing_key]["always_present"] = False
        first = False

    return fields


def generate_columns(fields):
    """
    Generate columns for a jsonl.

    @param fields: field analysis as returned by L{analyze_jsonl}
    @type fields: L{str}
    @return: a list of field definitions
    @rtype: L{list} of L{str}
    """
    defs = []
    for fieldname in sorted(list(fields.keys())):
        field = fields[fieldname]
        # get field type
        fieldtypes = field["types"]
        fieldtypedef = None
        if str in fieldtypes:
            fieldtypedef = "Unicode({})".format(
                2 ** math.ceil(math.log(field["max_length"], 2)) if field["max_length"] > 0 else 128,
            )
            used_fieldtype = str
        elif bool in fieldtypes:
            # fieldtypedef = "Boolean"
            used_fieldtype = bool
        elif int in fieldtypes:
            # fieldtypedef = "Integer"
            used_fieldtype = int
        elif datetime.datetime in fieldtypes:
            # fieldtypedef = "DateTime"
            used_fieldtype = datetime.datetime
        else:
            used_fieldtype = None
        # nullable check
        nullable = field["nullable"] or not field["always_present"]
        if used_fieldtype is not None:
            fielddef = "{}: Mapped[{}]{}".format(
                fieldname,
                (used_fieldtype.__name__ if not nullable else "Optional[{}]".format(used_fieldtype.__name__)),
                (" = mapped_column({})".format(fieldtypedef) if fieldtypedef is not None else ""),
            )
        else:
            fielddef = "# {}: {}".format(fieldname, fields[fieldname])
        defs.append(fielddef)
    return defs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Examine a jsonl file.")
    parser.add_argument("path", help="path to file to read")
    parser.add_argument(
        "--generate-column-definitions",
        action="store_true",
        dest="generate_columns",
        help="If set, output sqlalchemy column definitions instead",
    )
    ns = parser.parse_args()

    result = analyze_jsonl(ns.path)
    if ns.generate_columns:
        for fielddef in generate_columns(result):
            print(fielddef)
    else:
        pprint.pprint(result)
