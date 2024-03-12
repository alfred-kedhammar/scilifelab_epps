#!/usr/bin/env python

import logging
import os
import re
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime as dt

from couchdb.client import Database, Document, Row, ViewResults
from generate_minknow_samplesheet import (
    generate_MinKNOW_samplesheet,
    strip_characters,
    upload_file,
)
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Artifact, Process
from genologics.lims import Lims
from ont_send_reloading_info_to_db import get_ONT_db

from epp_utils import udf_tools

DESC = """Script for finishing the step to start ONT sequencing in LIMS.

Makes sure there are no discprepancies in the provided information and
appends LIMS-specific information to the ONT run in StatusDB.
"""

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")
SCRIPT_NAME: str = os.path.basename(__file__).split(".")[0]


def assert_samplesheet_vs_udfs(
    process: Process, args: Namespace, samplesheet_contents: str
):
    """Check that the current samplesheet is up to date, by re-generating one from the current UDFs and comparing it to the existing one."""

    # Generate new samplesheet from step, then read it and remove the file
    logger = logging.getLogger("generate_minknow_samplesheet")
    logger.setLevel(logging.ERROR)
    new_samplesheet_path = generate_MinKNOW_samplesheet(process=process, qc=args.qc)
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
    else:
        logging.info("Samplesheet is up to date.")


def udfs_matches_run_name(art: Artifact) -> bool:
    """Check that artifact run name is not contradicted by other UDFs."""

    matches: bool = True
    _yyyymmdd, _hhmm, pos, fc_id, _hash = art.udf["ONT run name"].split("_")

    if art.udf["ONT flow cell ID"] != fc_id:
        matches = False
        msg = f"Mismatch between UDFs 'ONT flow cell ID': '{art.udf['ONT flow cell ID']}' and 'ONT run name': '{art.udf['ONT run name']}'"
        logging.error(msg)
    if art.udf["ONT flow cell position"] != pos:
        matches = False
        msg = f"Mismatch between UDFs 'ONT flow cell position': '{art.udf['ONT flow cell position']}' and 'ONT run name': '{art.udf['ONT run name']}'"
        logging.error(msg)

    return matches


def get_matching_rows(
    art: Artifact,
    process: Process,
    view: ViewResults,
    run_name: str | None,
    qc: bool,
) -> list[Row]:
    """Find the rows in the database view that match the given artifact."""
    matching_rows = []

    # If the run name is supplied, query the database directly
    if run_name:
        logging.info(
            f"Full run name supplied. Quering the database for run '{run_name}'."
        )
        for row in view.rows:
            if run_name == row.key:
                matching_rows.append(row)

    # If run name is not supplied, try to find it in the database, assuming it follows the samplesheet naming convention
    else:
        minknow_sample_name = (
            strip_characters(art.name) if not qc else f"QC_{strip_characters(art.name)}"
        )
        minknow_experiment_name = process.id if not qc else f"QC_{process.id}"
        # Define query pattern
        if art.udf["ONT flow cell position"]:
            pattern = rf"{minknow_experiment_name}/{minknow_sample_name}/[^/]*_{art.udf['ONT flow cell position']}_{art.udf['ONT flow cell ID']}_[^/]*"
        else:
            pattern = rf"{minknow_experiment_name}/{minknow_sample_name}/[^/]*_{art.udf['ONT flow cell ID']}_[^/]*"
        logging.info(
            f"No run name supplied. Quering the database for run with path pattern '{pattern}'."
        )

        for row in view.rows:
            query = row.value["TACA_run_path"]
            if re.match(pattern, query):
                matching_rows.append(row)

    return matching_rows


def update_doc(doc, db: Database, process: Process, art: Artifact):
    """Update a given document with the given artifact's loading information."""

    # Info to add to the db doc
    dict_to_add = {
        "step_name": process.type.name,
        "step_id": process.id,
        "timestamp": TIMESTAMP,
        "operator": process.technician.name,
        "load_fmol": art.udf["ONT flow cell loading amount (fmol)"],
        "load_vol": art.udf["Volume to take (uL)"],
        "library_name": art.name,
        "library_id": art.id,
        "barcodes": art.reagent_labels,
        "projects": [
            {"name": project.name, "id": project.id}
            for project in set([sample.project for sample in art.samples])
        ],
        "samples": [{"name": sample.name, "id": sample.id} for sample in art.samples],
    }

    if "lims" not in doc:
        doc["lims"] = {}
    if "loading" not in doc["lims"]:
        doc["lims"]["loading"] = []
    doc["lims"]["loading"].append(dict_to_add)

    db[doc.id] = doc


def process_artifacts(process: Process, qc: bool):
    """Iterate across artifacts and update the database with the loading information."""
    arts: list[Artifact] = [
        art for art in process.all_outputs() if art.type == "Analyte"
    ]

    # Keep track of which artifacts were successfully updated
    arts_successful = []

    db: Database = get_ONT_db()
    view: ViewResults = db.view("info/all_stats")

    for art in arts:
        logging.info(f"Processing '{art.name}'...")

        run_name: str = udf_tools.fetch(art, "ONT run name", on_fail=None)
        if run_name:
            # Assert run name is not contradicted by other UDFs
            if not udfs_matches_run_name(art):
                logging.warning("Run name contradicted by other UDFs. Skipping.")
                continue

        # Get matching run docs
        matching_rows: list[Row] = get_matching_rows(art, process, view, run_name, qc)

        if len(matching_rows) == 0:
            logging.warning("Run was not found in the database. Skipping.")
            continue

        elif len(matching_rows) > 1:
            matching_run_names = [row.key for row in matching_rows]
            logging.warning("Query was found in multiple instances in the database: ")
            for run_name in matching_run_names:
                logging.warning(f"Matching run name: '{run_name}'.")
            logging.warning("Contact a database administrator. Skipping.")
            continue

        doc_run_name: str = matching_rows[0].key
        doc_id: str = matching_rows[0].id
        doc: Document = db[doc_id]

        logging.info(f"Found matching run '{doc_run_name}' in the database.")

        if run_name and run_name != doc_run_name:
            logging.error(
                f"UDF Run name '{run_name}' contradicted by database run name '{doc_run_name}'. Skipping."
            )
            continue
        else:
            logging.info(f"Assigning UDF 'ONT run name': '{doc_run_name}'.")
            udf_tools.put(art, "ONT run name", doc_run_name)

        update_doc(doc, db, process, art)
        logging.info(f"'{doc_run_name}' was found and updated successfully.")
        arts_successful.append(art)

    if len(arts_successful) < len(arts):
        raise AssertionError(
            f"Only {len(arts_successful)} out of {len(arts)} artifacts were successfully updated. Check log."
        )


def ont_send_loading_info_to_db(process: Process, lims: Lims, args: Namespace):
    """Executive script, called once."""

    # Get ONT samplesheet file artifact
    file_art: Artifact = [
        op for op in process.all_outputs() if op.name == args.samplesheet
    ][0]

    # If samplesheet file is loaded
    if file_art.files:
        logging.info("Detected samplesheet.")
        samplesheet_contents: str = lims.get_file_contents(uri=file_art.files[0].uri)

        logging.info("Checking that the loaded samplesheet is up to date...")
        assert_samplesheet_vs_udfs(process, args, samplesheet_contents)

    process_artifacts(process=process, qc=args.qc)


def main():
    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", type=str, help="Lims ID for current Process")
    parser.add_argument("--log", type=str, help="Which log file slot to use")
    parser.add_argument(
        "--samplesheet", type=str, help="Which samplesheet file slot to use"
    )
    parser.add_argument("--qc", action="store_true", help="Whether run is QC")
    args: Namespace = parser.parse_args()

    # Set up LIMS
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    process = Process(lims, id=args.pid)

    # Set up logging
    log_filename: str = (
        "_".join(
            [
                SCRIPT_NAME,
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

    # Start logging
    logging.info(f"Script '{SCRIPT_NAME}' started at {TIMESTAMP}.")
    logging.info(
        f"Launched in step '{process.type.name}' ({process.id}) by {process.technician.name}."
    )
    args_str = "\n\t".join([f"'{arg}': {getattr(args, arg)}" for arg in vars(args)])
    logging.info(f"Script called with arguments: \n\t{args_str}")

    try:
        ont_send_loading_info_to_db(process, lims, args)
    except Exception as e:
        # Post error to LIMS GUI
        logging.error(e)
        logging.shutdown()
        upload_file(
            file_path=log_filename,
            file_slot=args.log,
            process=process,
            lims=lims,
        )
        sys.stderr.write(str(e))
        sys.exit(2)
    else:
        logging.info("Script completed successfully.")
        logging.shutdown()
        upload_file(
            file_path=log_filename,
            file_slot=args.log,
            process=process,
            lims=lims,
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
