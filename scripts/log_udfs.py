#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from datetime import datetime as dt
from tabulate import tabulate
from ont_send_fc_to_db import parse_fc
import pandas as pd
import sys


DESC = """
Script for the EPP "Log fields" and file slot "Field log".
Use this script to create / update an append-only log where the step UDF info is stored.
Step-specific input assertions may be added where indicated.
"""


def main(lims, args):

    try:

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

        # Step-specific assertions
        if 'ONT Sequencing and Reloading' in currentStep.type.name:
            for art_tuple in [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["uri"].type == "Analyte"]:
                parse_fc(art_tuple)

        # Start building log dataframe
        rows = []
        for output in outputs:
            row = {}
            row["Sample"] = output.name
            for udf in output_udfs:
                try:
                    row[udf] = output.udf[udf]
                except KeyError:
                    row[udf] = None

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

    except AssertionError as e:
        sys.stderr.write(str(e))
        sys.exit(2)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)