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

        arts = [art for art in currentStep.all_outputs() if art.type == "Analyte"]

        # Read samplesheet and extract run info (trim out sample specific info)
        try:
            samplesheet = get_file(lims, currentStep, "ONT sample sheet")
        except:
            samplesheet = None

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

        # Match sample sheet contents to artifacts
        qcs = []
        amts = []
        for i, row in df.iterrows():
            matching_arts = [
                art
                for art in arts
                if str(art.udf["ONT flow cell ID"]) == str(row.flow_cell_id)
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
                udf_tools.fetch(
                    matching_arts[0], "ONT flow cell load amount (fmol)", on_fail="None"
                )
            )

        df["qc_pore_count"] = qcs
        df["initial_loading_fmol"] = amts

        # Match sample sheet contents and artifacts to db
        runtime_log = [
            "Check that all runs have synced to the database (i.e. they are visible in GenStat) and that the samplesheet info is correct."
        ]
        errors = False
        fc2run = {}
        for i, row in df.iterrows():
            try:
                pattern = f"{row.experiment_id}/{row.sample_id}/[^/]*_{row.position_id}_{row.flow_cell_id}_[^/]*"
            except AttributeError:
                pattern = f"{row.experiment_id}/{row.sample_id}/[^/]*_{row.flow_cell_id}_[^/]*"

            matching_docs = []
            for doc in view.rows:
                query = doc.value["TACA_run_path"]
                if re.match(pattern, query):
                    matching_docs.append(doc)

            try:
                if len(matching_docs) == 0:
                    runtime_log.append(
                        f"Path {pattern.replace('[^/]','')} was not found in the database."
                    )

                    raise AssertionError()

                elif len(matching_docs) > 1:
                    runtime_log.append(
                        f"Path {pattern.replace('[^/]','')} was found in multiple instances in the database. Contact a database administrator."
                    )
                    raise AssertionError()

                # Make dict for mapping to run names
                fc = [
                    art.udf["ONT flow cell ID"]
                    for art in arts
                    if art.udf["ONT flow cell ID"] == row.flow_cell_id
                ][0]
                fc2run[fc] = matching_docs[0].value["TACA_run_path"].split("/")[-1]

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
                    try:
                        doc["lims"]["loading"].append(dict_to_add)
                    except KeyError:
                        doc["lims"]["loading"] = []
                        doc["lims"]["loading"].append(dict_to_add)
                except:
                    doc["lims"] = {}
                    doc["lims"]["loading"] = []
                    doc["lims"]["loading"].append(dict_to_add)

                db[doc.id] = doc

                runtime_log.append(
                    f"Path {pattern.replace('[^/]','')} was found and updated successfully."
                )

            except AssertionError as e:
                errors = True
                continue

        if errors:
            raise AssertionError("\n".join(runtime_log))

        for art in arts:
            udf_tools.put(art, "ONT run ID", fc2run[art.udf["ONT flow cell ID"]])

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
