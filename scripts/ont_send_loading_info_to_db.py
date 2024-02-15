#!/usr/bin/env python

import logging
import os
import re
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime as dt

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Artifact, Process
from genologics.lims import Lims
from ont_generate_samplesheet import minknow_samplesheet_default
from ont_send_reloading_info_to_db import get_ONT_db

from epp_utils import udf_tools

DESC = """Script for EPP "ont_send_loading_info_to_db".

- Ensure UDFs, samplesheet and run name do not contain any contradictions
- Upload LIMS-specific information to the run entry in the database
"""

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")


def assert_samplesheet_vs_udfs(process: Process, samplesheet_contents: str) -> None:
    """Check that the current samplesheet is up to date, by re-generating one from the current UDFs and comparing it to the existing one."""

    # Generate new samplesheet from step, then read it and remove the file
    new_samplesheet_path = minknow_samplesheet_default(process)
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


def udfs_matches_run_name(art: Artifact) -> bool:
    """Check that artifact run name is not contradicted by other UDFs."""

    matches = True
    _yyyymmdd, _hhmm, pos, fc_id, _hash = art.udf["ONT run name"].split("_")

    if art.udf["ONT flow cell ID"] != fc_id:
        matches = False
        msg = f"Mismatch between UDFs 'ONT flow cell ID': '{art.udf['ONT flow cell ID']}' and 'ONT run name': '{art.udf['ONT run name']}'"
        logging.error(msg)
    if art.udf["ONT flow cell position"] != pos:
        matches = False
        msg = f"Mismatch between UDFs 'ONT flow cell position': '{art.udf['ONT flow cell position']}' and 'ONT run name': '{art.udf['ONT run name']}'"
        logging.error(msg)

    if matches:
        return True
    else:
        return False


def get_matching_docs(art: Artifact, process: Process, view, run_name: str) -> list:
    matching_docs = []

    # If the run name is supplied, query the database directly
    if run_name:
        logging.info(
            f"Full run name supplied. Quering the database for run {run_name}."
        )
        for doc in view.rows:
            if run_name == doc.key:
                matching_docs.append(doc)

    # If run name is not supplied, try to find it in the database, assuming it follows the samplesheet naming convention
    else:
        # Define query pattern
        if art.udf["ONT flow cell position"]:
            pattern = rf"{process.id}/{art.name}/[^/]*_{art.udf['ONT flow cell position']}_{art.udf['ONT flow cell ID']}_[^/]*"
        else:
            pattern = (
                rf"{process.id}/{art.name}/[^/]*_{art.udf['ONT flow cell ID']}_[^/]*"
            )
        logging.info(
            f"No run name supplied. Quering the database for run with path pattern {pattern}."
        )

        for doc in view.rows:
            query = doc.value["TACA_run_path"]
            if re.match(pattern, query):
                matching_docs.append(doc)

    return matching_docs


def update_doc(doc, db, process: Process, art: Artifact) -> None:
    dict_to_add = {
        "step_name": process.type.name,
        "step_id": process.id,
        "timestamp": TIMESTAMP,
        "operator": process.technician.name,
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


def send_runs_to_db(process: Process) -> None:
    arts: list[Artifact] = [
        art for art in process.all_outputs() if art.type == "Analyte"
    ]

    db = get_ONT_db()
    view = db.view("info/all_stats")

    for art in arts:
        logging.info(f"Checking {art.name}...")

        run_name: str = udf_tools.fetch(art, "ONT run name", on_fail=None)
        if run_name:
            # Assert run name is not contradicted by other UDFs
            if not udfs_matches_run_name(art):
                logging.warning("Run name contradicted by other UDFs. Skipping.")
                continue

        # Get matching run docs
        matching_docs: list = get_matching_docs(art, process, view, run_name)

        if len(matching_docs) == 0:
            logging.warning("Run was not found in the database. Skipping.")
            continue

        elif len(matching_docs) > 1:
            logging.warning(
                f"{run_name} was found in multiple instances in the database. Contact a database administrator. Skipping."
            )
            continue

        else:
            doc_id: str = matching_docs[0].id
            doc = db[doc_id]

            logging.info("Found matching run in the database.")

            update_doc(doc, db, process, art)
            logging.info(f"{run_name} was found and updated successfully.")


def ont_send_loading_info_to_db() -> None:
    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args: Namespace = parser.parse_args()

    # Set up LIMS
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    process = Process(lims, id=args.pid)

    # Set up logging
    log_filename: str = (
        "_".join(
            [
                "ont-db",
                process.id,
                TIMESTAMP,
                process.technician.name.replace(" ", ""),
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
        "ONT Start Sequencing" in process.type.name
    ), f"Unrecognized LIMS step: {process.type.name}."

    # Get ONT samplesheet file artifact
    file_art: Artifact = [
        op for op in process.all_outputs() if op.name == "ONT sample sheet"
    ][0]

    # If samplesheet file is loaded
    if file_art.files:
        logging.info("Detected samplesheet.")
        samplesheet_contents: str = lims.get_file_contents(uri=file_art.files[0].uri)

        logging.info("Checking that the loaded samplesheet is up to date...")
        assert_samplesheet_vs_udfs(process, samplesheet_contents)

    send_runs_to_db(process)


def main() -> None:
    try:
        ont_send_loading_info_to_db()
    except Exception as e:
        # Post error to LIMS GUI
        logging.error(e)
        logging.shutdown()
        sys.exit(2)
    else:
        logging.info("Script completed successfully.")
        logging.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    main()
