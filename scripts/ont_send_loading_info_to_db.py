#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from ont_send_fc_to_db import get_ONT_db
from ont_generate_samplesheet import get_minknow_sample_id
import sys
import pandas as pd
from io import StringIO
from datetime import datetime as dt

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
    return None


def main(lims, args):
    try:
        currentStep = Process(lims, id=args.pid)
        timestamp = dt.now().strftime("%y%m%d_%H%M%S")

        # Read samplesheet and extract run info (trim out sample specific info)
        try:
            samplesheet = pd.read_csv(get_file(lims, currentStep, "ONT sample sheet"))
        except:
            raise AssertionError("No sample sheet provided.")
        df = samplesheet[
            ["experiment_id", "sample_id", "flow_cell_id", "position_id"]
        ].drop_duplicates()

        db = get_ONT_db()
        view = db.view("info/all_stats")

        arts = [art for art in currentStep.all_outputs() if art.type == "Analyte"]

        # Match sample sheet contents to artifacts
        assert len(df) == len(
            arts
        ), "Sample sheet contents doesn't match current step info."

        qcs = []
        amts = []
        for i, row in df.iterrows():
            matching_arts = [art for art in arts if art.udf["ONT flow cell ID"]]
            assert (
                len(matching_arts) == 1
            ), "Sample sheet contents doesn't match current step."
            qcs.append(matching_arts[0].udf["ONT Flow Cell QC Pore Count"])
            amts.append(matching_arts[0].udf["Amount (fmol)"])

        df["qc_pore_count"] = qcs
        df["initial_loading_fmol"] = amts

        # Match sample sheet contents and artifacts to db
        runtime_log = []
        errors = False
        for i, row in df.iterrows():
            matching_docs = [
                doc
                for doc in view.rows
                if f"{row.experiment_id}" in doc.value["TACA_run_path"]
                and f"{row.sample_id}" in doc.value["TACA_run_path"]
                and f"{row.flow_cell_id}" in doc.value["TACA_run_path"]
                and f"{row.position_id}" in doc.value["TACA_run_path"]
            ]

            try:
                if len(matching_docs) == 0:
                    partially_matching_paths = [
                        "\t".join(doc.value["TACA_run_path"].split("/"))
                        for doc in view.rows
                        if f"{row.experiment_id}" in doc.value["TACA_run_path"]
                        or f"{row.sample_id}" in doc.value["TACA_run_path"]
                        or f"{row.flow_cell_id}" in doc.value["TACA_run_path"]
                    ]

                    msg_lines = [
                        f"The database contains no runs matching the query on flow cell {row.flow_cell_id}",
                        "\nIf the run was recently started, wait until it appears in GenStat. If the samplesheet is incorrect, modify it accordingly.",
                        "\n"
                        + "\t".join(["Experiment ID", "Sample ID", "Flow Cell ID"]),
                        "=============================================",
                        "\nQuery:",
                        "\t".join([row.experiment_id, row.sample_id, row.flow_cell_id]),
                        "\nPartial matches:",
                        "\t".join([row.experiment_id, row.sample_id, row.flow_cell_id]),
                        "\t".join(partially_matching_paths),
                    ]

                    raise AssertionError("\n".join(msg_lines))

                if len(matching_docs) > 1:
                    msg_lines = [
                        "Checking StatusDB for: "
                        f"MinKNOW Experiment ID {row.experiment_id}, MinKNOW Sample ID {row.sample_id}, Flow cell ID {row.flow_cell_id}, Position {row.position_id}.",
                        "The database contains multiple matching documents contact a database administrator.",
                    ]
                    raise AssertionError("\n".join(msg_lines))

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
