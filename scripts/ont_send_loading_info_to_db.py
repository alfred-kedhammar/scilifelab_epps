#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from ont_send_fc_to_db import get_ONT_db
from ont_generate_samplesheet import get_minknow_sample_id
import sys
from datetime import datetime as dt
import pandas as pd
from io import StringIO

DESC = """ Script for EPP "ont_check_run_has_started".

Ensure all samples in the step correspond to an ONT run that was started with the correct samplesheet.
"""


def parse_fc(art_tuple):
    fc = {}
    fc["samplesheet_id"] = art_tuple[1]["uri"].parent_process.id
    fc["fc_id"] = art_tuple[1]["uri"].udf.get("ONT flow cell ID")
    fc["minknow_sample_id"] = get_minknow_sample_id(art_tuple[1]["uri"])
    fc["qc"] = art_tuple[1]["uri"].udf.get("ONT Flow Cell QC Pore Count")
    fc["load_fmol"] = art_tuple[1]["uri"].udf.get("Amount (fmol)")

    return fc


def get_file(lims, currentStep, file_name):
    file_art = [op for op in currentStep.all_outputs() if op.name == file_name][0]
    if file_art.files:
        readable = StringIO(lims.get_file_contents(uri=file_art.files[0].uri))
    return readable


def main(lims, args):
    try:
        currentStep = Process(lims, id=args.pid)

        timestamp = dt.now().strftime("%y%m%d_%H%M%S")

        # Read samplesheet and extract run info (trim out sample specific info)
        samplesheet = pd.read_csv(get_file(lims, currentStep, "ONT sample sheet"))
        df = samplesheet[
            ["experiment_id", "sample_id", "flow_cell_id", "position_id"]
        ].drop_duplicates()

        db = get_ONT_db()
        view = db.view("info/all_stats")

        runtime_log = []
        errors = False
        for i, row in df.iterrows():
            matching_doc = [
                doc
                for doc in view.rows
                if f"{row.experiment_id}" in doc.value["TACA_run_path"]
                and f"{row.sample_id}" in doc.value["TACA_run_path"]
                and f"{row.flow_cell_id}" in doc.value["TACA_run_path"]
                and f"{row.position_id}" in doc.value["TACA_run_path"]
            ]

            try:
                assert (
                    len(matching_doc) > 0
                ), f"The database contains no document with experiment ID {fc['samplesheet_id']} and flow cell ID {fc['fc_id']}. If the run was recently started, wait until it appears in GenStat."
                assert (
                    len(matching_doc) == 1
                ), f"The database contains multiple documents with flow cell ID {fc['fc_id']} and experiment ID {fc['samplesheet_id']}. Contact a database administrator."

                doc_id = matching_doc[0].id
                doc = db[doc_id]

                dict_to_add = {
                    "step_name": currentStep.type.name,
                    "pid": currentStep.id,
                    "timestamp": timestamp,
                    "qc": fc["qc"],
                    "load_fmol": fc["load_fmol"],
                }

                try:
                    # Try to find pre-existing nest and loading list to append to
                    lims_nest = doc["lims"]
                    try:
                        loading_list = lims_nest["loading"]
                    except KeyError:
                        loading_list = []
                except KeyError:
                    # Create new nest and loading list
                    loading_list = []
                    lims_nest = {"loading": loading_list}

                loading_list.append(dict_to_add)

                doc.update({"lims": lims_nest})
                db[doc.id] = doc

                runtime_log.append(f"Flowcell {fc['fc_id']} was updated successfully.")

            except AssertionError as e:
                errors = True
                runtime_log.append(str(e))
                continue

        if errors:
            raise AssertionError("\n".join(runtime_log))

    except AssertionError as e:
        sys.stderr.write(str(e))
        sys.exit(2)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
