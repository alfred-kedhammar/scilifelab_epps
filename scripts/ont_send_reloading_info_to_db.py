#!/usr/bin/env python

import logging
import os
import re
import sys
from argparse import ArgumentParser
from datetime import datetime as dt

import couchdb
import yaml
from couchdb.client import Database, Document, Row, ViewResults
from generate_minknow_samplesheet import upload_file
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Artifact, Process
from genologics.lims import Lims

DESC = """Used to record the washing and reloading of ONT flow cells.

Information is parsed from LIMS and uploaded to the CouchDB database nanopore_runs.
"""

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")


def send_reloading_info_to_db(process: Process):
    """For all samples/flowcells, use the run name to find the correct database entry.

    Then update the document "lims" json object nest with the reloading information.
    """

    # Parse inputs and their UDFs
    arts: list[Artifact] = process.all_inputs()

    runs = []
    for art_tuple in arts:
        run: dict | None = parse_run(art_tuple)
        if run:
            runs.append(run)

    db: Database = get_ONT_db()
    view: ViewResults = db.view("info/all_stats")

    errors = False
    for run in runs:
        rows_matching_run: list[Row] = [
            row
            for row in view.rows
            if f'{run["run_name"]}' in row.value["TACA_run_path"]
        ]

        try:
            assert (
                len(rows_matching_run) > 0
            ), f"The database contains no document with run name '{run['run_name']}'. If the run was recently started, wait until it appears in GenStat."
            assert (
                len(rows_matching_run) == 1
            ), f"The database contains multiple documents with run name '{run['run_name']}'. Contact a database administrator."

            doc_id: str = rows_matching_run[0].id
            doc: Document = db[doc_id]

            dict_to_add = {
                "step_name": process.type.name,
                "step_id": process.id,
                "timestamp": TIMESTAMP,
                "operator": process.technician.name,
                "reload_times": run["reload_times"],
                "reload_fmols": run["reload_fmols"],
                "reload_lots": run["reload_lots"],
            }

            if "lims" not in doc:
                doc["lims"] = {}
            if "reloading" not in doc["lims"]:
                doc["lims"]["reloading"] = []
            doc["lims"]["reloading"].append(dict_to_add)

            db[doc.id] = doc

            logging.info(f"Run '{run['run_name']}' was updated successfully.")

        except AssertionError as e:
            errors = True
            logging.info(str(e))
            continue

    if errors:
        raise AssertionError()


def parse_run(art: Artifact) -> dict | None:
    """For each art, assert UDFs and return parsed dictionary"""

    fc = {}

    fc["run_name"] = art.udf["ONT run name"]

    fc["reload_times"] = (
        art.udf.get("ONT reload run time (hh:mm)").replace(" ", "").split(",")
        if art.udf.get("ONT reload run time (hh:mm)")
        else None
    )
    fc["reload_fmols"] = (
        art.udf.get("ONT reload amount (fmol)").replace(" ", "").split(",")
        if art.udf.get("ONT reload amount (fmol)")
        else None
    )
    fc["reload_lots"] = (
        art.udf.get("ONT reload wash kit").replace(" ", "").split(",")
        if art.udf.get("ONT reload wash kit")
        else None
    )

    if fc["reload_times"] or fc["reload_fmols"] or fc["reload_lots"]:
        assert (
            fc["reload_times"]
            and fc["reload_fmols"]
            and fc["reload_lots"]
            and len(fc["reload_times"])
            == len(fc["reload_fmols"])
            == len(fc["reload_lots"])
        ), "All reload UDFs within a row must have the same number of comma-separated values"

        assert check_csv_udf_list(
            r"^\d{1,3}:\d{2}$", fc["reload_times"]
        ), "Reload run times must be formatted as comma-separated h:mm"
        check_times_list(fc["reload_times"])
        assert check_csv_udf_list(
            r"^[0-9.]+$", fc["reload_fmols"]
        ), "Invalid flow cell reload amount(s)"
        assert check_csv_udf_list(
            r"^[0-9a-zA-Z.-_]+$", fc["reload_lots"]
        ), "Invalid Reload wash kit"

        return fc

    else:
        return None


def check_times_list(times_list: list[str]):
    """Check that a list of comma-separated times is sequential and valid."""
    prev_hours, prev_minutes = 0, 0
    for time in times_list:
        _hours, _minutes = time.split(":")
        hours, minutes = int(_hours), int(_minutes)
        assert hours > prev_hours or (
            hours == prev_hours and minutes > prev_minutes
        ), f"Times in field {times_list} are non-sequential."
        assert (
            minutes < 60
        ), f"Field {times_list} contains invalid entries (minutes >= 60)."

        prev_hours, prev_minutes = hours, minutes


def get_ONT_db() -> Database:
    """Mostly copied from write_notes_to_couchdb.py"""
    configf = "~/.statusdb_cred.yaml"

    with open(os.path.expanduser(configf)) as config_file:
        config = yaml.safe_load(config_file)

    url_string = f"https://{config['statusdb'].get('username')}:{config['statusdb'].get('password')}@{config['statusdb'].get('url')}"
    couch = couchdb.Server(url=url_string)

    return couch["nanopore_runs"]


def check_csv_udf_list(pattern: str, csv_udf_list: list[str]) -> bool:
    """For a UDF expected as a comma-separated list, assert format of all elements of the list."""
    if csv_udf_list:
        return all([re.match(pattern, element) for element in csv_udf_list])
    else:
        return True


def main():
    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    # Set up LIMS
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    process = Process(lims, id=args.pid)

    # Set up logging
    log_filename: str = (
        "_".join(
            [
                "ont-db-reloading",
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

    try:
        send_reloading_info_to_db(process, lims)
    except Exception as e:
        # Post error to LIMS GUI
        logging.error(e)
        logging.shutdown()
        upload_file(
            file_path=log_filename,
            file_slot="Database sync log",
            currentStep=process,
            lims=lims,
        )
        sys.stderr.write(str(e))
        sys.exit(2)
    else:
        logging.info("Script completed successfully.")
        logging.shutdown()
        upload_file(
            file_path=log_filename,
            file_slot="Database sync log",
            currentStep=process,
            lims=lims,
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
