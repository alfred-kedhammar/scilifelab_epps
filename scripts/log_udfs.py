#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from datetime import datetime as dt
import pandas as pd
from tabulate import tabulate

DESC = """ Use this script to create / update an append-only log where the step UDF info is stored """


def main(lims, args):

    currentStep = Process(lims, id=args.pid)

    timestamp = dt.now().strftime("%y%m%d_%H%M%S")

    # Collect last log file, if there is one
    file_art = [op for op in currentStep.all_outputs() if op.name == "Field log"][0]
    if file_art.files:
        file_bytes = lims.get_file_contents(uri = file_art.files[0].uri).read()
        file_str = file_bytes.decode("utf-8")
    else:
        file_str = None
                
    # Parse outputs and their UDFs
    outputs = [output for output in currentStep.all_outputs() if output.type == "Analyte"]

    # Collect all populated UDFs
    output_udfs = []
    for output in outputs:
        for udf_tuple in output.udf.items():
            if udf_tuple[0] not in output_udfs:
                output_udfs.append(udf_tuple[0])

    # Start building log dataframe
    # TODO assert input format
    rows = []
    for output in outputs:
        row = {}
        row["Sample"] = output.name
        for udf in output_udfs:
            if output.udf[udf]:
                row[udf] = output.udf[udf]
        rows.append(row)

    df = pd.DataFrame(rows).set_index("Sample")

    new_log_name = f"UDF_log_{currentStep.id}_{timestamp}.txt"
    with open(new_log_name, "w") as f:
        
        # Write previous file contents
        if file_str:
            f.write(file_str)

        # Write new file contents
        f.write(timestamp+"\n")
        f.write(tabulate(df, headers="keys"))
        f.write("\n\n")

    for out in currentStep.all_outputs():
        if out.name == "Field log":
            for f in out.files:
                lims.request_session.delete(f.uri)
            lims.upload_new_file(out, new_log_name)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)