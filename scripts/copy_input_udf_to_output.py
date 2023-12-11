#!/usr/bin/env python

DESC = """EPP script to copy input UDFs into output UDFs by matching names

Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
"""
import sys
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims


def main(lims, pid, udfs):
    process = Process(lims, id=pid)

    inputs = process.all_inputs()
    input_udf_dict = dict()
    for input in inputs:
        if input.name not in input_udf_dict.keys():
            input_udf_dict[input.name] = dict(input.udf.items())
        else:
            sys.stderr.write(f"ERROR: Duplicated artifact name {input.name}!")
            sys.exit(2)

    outputs = [i for i in process.all_outputs() if i.type == "Analyte"]
    for output in outputs:
        if output.name in input_udf_dict.keys():
            for udf in udfs:
                if udf in input_udf_dict[output.name].keys():
                    output.udf[udf] = input_udf_dict[output.name][udf]
                    output.put()
                else:
                    sys.stderr.write(f"ERROR: Specified UDF {udf} not found!")
                    sys.exit(2)
        else:
            sys.stderr.write(f"ERROR: Output name {output.name} not found!")
            sys.exit(2)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", required=True, help="Lims id for current Process")
    parser.add_argument(
        "--udfs",
        nargs="+",
        required=True,
        help="List of output UDF to be copied from input; e.g. --udf A B C D",
    )
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args.pid, args.udfs)
