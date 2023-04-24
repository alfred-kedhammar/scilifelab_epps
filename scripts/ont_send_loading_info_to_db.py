#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from ont_send_reloading_info_to_db import get_ONT_db
import sys
import pandas as pd
from io import StringIO
from datetime import datetime as dt
import re
from utils import udf_tools
from ont_generate_samplesheet import make_samplesheet
import os

DESC = """ Script for EPP "ont_check_run_has_started".

Ensure all samples in the step correspond to an ONT run that was started with the correct samplesheet.
"""


def get_file(lims, currentStep, file_name):
    file_art = [op for op in currentStep.all_outputs() if op.name == file_name][0]
    if file_art.files:
        readable = StringIO(lims.get_file_contents(uri=file_art.files[0].uri))
        return readable
    return None


def main(lims, args):
    try:
        currentStep = Process(lims, id=args.pid)
        timestamp = dt.now().strftime("%y%m%d_%H%M%S")

        # Read samplesheet and extract run info (trim out sample specific info)
        try:
            samplesheet = get_file(lims, currentStep, "ONT sample sheet")
        except:
            raise AssertionError("No sample sheet provided.")

        # Check that current UDFs correspond to the current samplesheet, by generating it anew and comparing
        new_ss_path = make_samplesheet(currentStep)
        new_ss_contents = open(new_ss_path, "r").read()
        os.remove(new_ss_path)
        assert (
            samplesheet.read() == new_ss_contents
        ), "The current sample sheet doesn't correspond to the current UDFs."

        df = pd.read_csv(get_file(lims, currentStep, "ONT sample sheet"))
        if "position_id" in df.columns:
            df = df[
                ["experiment_id", "sample_id", "flow_cell_id", "position_id"]
            ].drop_duplicates()
        else:
            df = df[["experiment_id", "sample_id", "flow_cell_id"]].drop_duplicates()

        db = get_ONT_db()
        view = db.view("info/all_stats")

        arts = [art for art in currentStep.all_outputs() if art.type == "Analyte"]

        # Match sample sheet contents to artifacts
        qcs = []
        amts = []
        for i, row in df.iterrows():
            matching_arts = [
                art for art in arts if art.udf["ONT flow cell ID"] == row.flow_cell_id
            ]
            assert (
                len(matching_arts) == 1
            ), "Sample sheet contents doesn't match current step."
            qcs.append(
                udf_tools.fetch(
                    matching_arts[0], "ONT Flow Cell QC Pore Count", on_fail="None"
                )
            )
            amts.append(
                udf_tools.fetch(matching_arts[0], "Amount (fmol)", on_fail="None")
            )

        df["qc_pore_count"] = qcs
        df["initial_loading_fmol"] = amts

        # Match sample sheet contents and artifacts to db
        runtime_log = []
        errors = False
        for i, row in df.iterrows():
            try:
                pattern = f"{row.experiment_id}/{row.sample_id}/[^/]*_{row.position_id}_{row.flow_cell_id}_[^/]*"
            except AttributeError:
                pattern = f"{row.experiment_id}/{row.sample_id}/[^/]*_{row.flow_cell_id}_[^/]*"

            runtime_log_lines = [f"Checking StatusDB for run path: \n{pattern}"]

            matching_docs = []
            for doc in view.rows:
                query = doc.value["TACA_run_path"]
                if re.match(pattern, query):
                    matching_docs.append(doc)

            try:
                if len(matching_docs) == 0:
                    partially_matching_paths = [
                        doc.value["TACA_run_path"]
                        for doc in view.rows
                        if f"{row.experiment_id}" in doc.value["TACA_run_path"]
                        or f"{row.sample_id}" in doc.value["TACA_run_path"]
                        or f"{row.flow_cell_id}" in doc.value["TACA_run_path"]
                    ]

                    runtime_log_lines += [
                        f"The database contains no runs matching the sample sheet",
                        "If the run was recently started, wait until it appears in GenStat. If the samplesheet is incorrect, upload the correct one.",
                    ]

                    if partially_matching_paths:
                        runtime_log_lines += [
                            "Partial matches:",
                            "\n".join(partially_matching_paths),
                        ]

                    raise AssertionError("\n".join(runtime_log_lines))

                elif len(matching_docs) > 1:
                    runtime_log_lines.append(
                        "The database contains multiple matching documents. Contact a database administrator.",
                    )
                    raise AssertionError("\n".join(runtime_log_lines))

                elif len(matching_docs) == 1:
                    doc_id = matching_docs[0].id
                    doc = db[doc_id]

                    dict_to_add = {
                        "step_name": currentStep.type.name,
                        "pid": currentStep.id,
                        "timestamp": timestamp,
                        "qc": row.qc_pore_count,
                        "load_fmol": row.initial_loading_fmol,
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

                runtime_log.append(
                    f"Flowcell {row.flow_cell_id} was updated successfully."
                )

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
