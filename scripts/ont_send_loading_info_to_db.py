#!/usr/bin/env python

import logging
import os
import re
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime as dt
from io import StringIO

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims
from ont_generate_samplesheet import minknow_samplesheet_default
from ont_send_reloading_info_to_db import get_ONT_db

from epp_utils import udf_tools

DESC = """Script for EPP "ont_send_loading_info_to_db".

- Ensure UDFs, samplesheet and run name do not contain any contradictions
- Upload LIMS-specific information to the run entry in the database
"""

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")


def assert_samplesheet_vs_udfs(currentStep, samplesheet_contents):
    """Check that the current samplesheet is up to date, by re-generating one from the current UDFs and comparing it to the existing one."""

    # Generate new samplesheet from step, then read it and remove the file
    new_samplesheet_path = minknow_samplesheet_default(currentStep)
    new_samplesheet_contents = open(new_samplesheet_path).read()
    os.remove(new_samplesheet_path)

    # Check step samplesheet is up-to-date with step UDFs
    if samplesheet_contents != new_samplesheet_contents:
        logging.error(
            f"The current sample sheet doesn't correspond to the current UDFs.\nCurrent sample sheet:\n{samplesheet_contents}\nNew sample sheet:\n{new_samplesheet_contents}"
        )
        raise AssertionError(
            "The current sample sheet doesn't correspond to the current UDFs."
        )


def assert_udfs_to_run_name(lims, args):
    """Assert step UDFs do not contradict the run name."""
    currentStep = Process(lims, id=args.pid)
    arts = [art for art in currentStep.all_outputs() if art.type == "Analyte"]

    for art in arts:
        yyyymmdd, hhmm, pos, fc_id, hash = art.udf["ONT run name"].split("_")
        assert (
            art.udf["ONT flow cell ID"] == fc_id
        ), f"Mismatch between flowcell ID '{art.udf['ONT flow cell ID']}' and run name '{art.udf['ONT run name']}'"
        assert (
            art.udf["ONT flow cell position"] == pos
        ), f"Mismatch between flowcell position '{art.udf['ONT flow cell position']}' and run name '{art.udf['ONT run name']}'"

    pass


def send_runs_to_db(lims: Lims, args: Namespace):
    currentStep = Process(lims, id=args.pid)

    arts = [art for art in currentStep.all_outputs() if art.type == "Analyte"]

    db = get_ONT_db()
    view = db.view("info/all_stats")

    errors = False
    for art in arts:
        logging.info(f"Checking {art.name}...")

        try:
            run_id = art.udf["ONT run name"]
        except KeyError:
            logging.info(f"No run name supplied for {art.name}")
            errors = True

        matching_docs = []
        for doc in view.rows:
            if run_id == doc.key:
                matching_docs.append(doc)

        try:
            if len(matching_docs) == 0:
                logging.info(f"{run_id} was not found in the database.")
                raise AssertionError()

            elif len(matching_docs) > 1:
                logging.info(
                    f"{run_id} was found in multiple instances in the database. Contact a database administrator."
                )
                raise AssertionError()

            doc_id = matching_docs[0].id
            doc = db[doc_id]

            dict_to_add = {
                "step_name": currentStep.type.name,
                "step_id": currentStep.id,
                "timestamp": TIMESTAMP,
                "operator": currentStep.technician.name,
                "sample_name": art.name,
                "sample_id": art.id,
                "load_fmol": art.udf["ONT flow cell loading amount (fmol)"],
                "load_vol": art.udf["Volume to take (uL)"],
            }

            if "lims" not in doc:
                doc["lims"] = {}
            if "loading" not in doc["lims"]:
                doc["lims"]["loading"] = []
            doc["lims"]["loading"].append(dict_to_add)

            db[doc.id] = doc

            logging.info(f"{run_id} was found and updated successfully.")

        except AssertionError:
            errors = True
            continue

    if errors:
        raise AssertionError()


def main():
    try:
        # Parse args
        parser = ArgumentParser(description=DESC)
        parser.add_argument("--pid", help="Lims id for current Process")
        args: Namespace = parser.parse_args()

        # Set up LIMS
        lims = Lims(BASEURI, USERNAME, PASSWORD)
        lims.check_version()
        currentStep = Process(lims, id=args.pid)

        # Set up logging
        log_filename: str = (
            "_".join(
                [
                    "ont-db",
                    currentStep.id,
                    TIMESTAMP,
                    currentStep.technician.name.replace(" ", ""),
                ]
            )
            + ".log"
        )

        logging.basicConfig(
            filename=log_filename,
            filemode="w",
            format="%(levelname)s: %(message)s",
            level=logging.INFO,
        )

        # Assert we are in the right step
        assert (
            "ONT Start Sequencing" in currentStep.type.name
        ), f"Unrecognized LIMS step: {currentStep.type.name}."

        # Get ONT samplesheet file artifact
        file_art = [
            op for op in currentStep.all_outputs() if op.name == "ONT sample sheet"
        ][0]

        # If samplesheet file is loaded
        if file_art.files:
            logging.info("Detected samplesheet.")
            samplesheet_contents = lims.get_file_contents(uri=file_art.files[0].uri)

            logging.info("Checking that the loaded samplesheet is up to date...")
            assert_samplesheet_vs_udfs(currentStep, samplesheet_contents)

            # Parse samplesheet to run-level dataframe
            df_ss = pd.read_csv(StringIO(samplesheet_contents))
            columns_to_keep = ["experiment_id", "sample_id", "flow_cell_id"]
            if "position_id" in df_ss.columns:
                columns_to_keep.append("position_id")
            df_ss_runs = (
                df_ss[columns_to_keep].drop_duplicates()
            )  # Duplicates can occur because rows are on sample level, not on run level

        send_runs_to_db(lims, args, df_ss_runs)

    # Post error to LIMS GUI
    except AssertionError as e:
        sys.stderr.write(str(e))
        sys.exit(2)


if __name__ == "__main__":
    main()


def match_to_db_using_samplesheet(lims: Lims, args: Namespace):
    currentStep = Process(lims, id=args.pid)

    # Match df to db
    db = get_ONT_db()
    view = db.view("info/all_stats")

    runtime_log = [
        "Check that all runs have synced to the database (i.e. they are visible in GenStat) and that the samplesheet info is correct."
    ]
    errors = False
    fc2run = {}
    for i, row in df.iterrows():
        try:
            pattern = f"{row.experiment_id}/{row.sample_id}/[^/]*_{row.position_id}_{row.flow_cell_id}_[^/]*"
        except AttributeError:
            pattern = (
                f"{row.experiment_id}/{row.sample_id}/[^/]*_{row.flow_cell_id}_[^/]*"
            )

        matching_docs = []
        for doc in view.rows:
            query = doc.value["TACA_run_path"]
            if re.match(pattern, query):
                matching_docs.append(doc)

        try:
            if len(matching_docs) == 0:
                logging.info(
                    f"Path {pattern.replace('[^/]','')} was not found in the database."
                )

                raise AssertionError()

            elif len(matching_docs) > 1:
                logging.info(
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
                "timestamp": TIMESTAMP,
                "qc": row.qc_pore_count,
                "load_fmol": row.initial_loading_fmol,
                "operator": currentStep.technician.name,
            }

            if "lims" not in doc:
                doc["lims"] = {}
            if "loading" not in doc["lims"]:
                doc["lims"]["loading"] = []
            doc["lims"]["loading"].append(dict_to_add)

            db[doc.id] = doc

            logging.info(
                f"Path {pattern.replace('[^/]','')} was found and updated successfully."
            )

        except AssertionError:
            errors = True
            continue

    if errors:
        raise AssertionError("\n".join(runtime_log))

    for art in arts:
        udf_tools.put(art, "ONT run name", fc2run[art.udf["ONT flow cell ID"]])
