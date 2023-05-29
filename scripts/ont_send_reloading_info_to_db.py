#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from datetime import datetime as dt
import re
import os
import couchdb
import yaml
import sys


DESC = """ Script for EPP "Send ONT flowcell info to StatusDB".
Used to record the washing and reloading of ONT flow cells.
Information is parsed from LIMS and uploaded to the CouchDB database nanopore_runs.
"""


def main(lims, args):
    """For all samples/flowcells, use the run name to find the correct database entry.

    Then update the document "lims" json object nest with the reloading information.

    TODO Get parent process ID on a sample-by-sample basis, rather than once for the entire step.
         Current approach may cause issues if samples originate from different steps.
    """

    currentStep = Process(lims, id=args.pid)
    timestamp = dt.now().strftime("%y%m%d_%H%M%S")

    # Parse inputs and their UDFs
    arts = currentStep.all_inputs()

    runs = []
    for art_tuple in arts:
        run = parse_run(art_tuple)
        if run:
            runs.append(run)

    db = get_ONT_db()
    view = db.view("info/all_stats")

    runtime_log = []
    errors = False
    for run in runs:
        rows_matching_run = [
            row
            for row in view.rows
            if f'{run["run_name"]}' in row.value["TACA_run_path"]
        ]

        try:
            assert (
                len(rows_matching_run) > 0
            ), f"The database contains no document with run name {run['run_name']}. If the run was recently started, wait until it appears in GenStat."
            assert (
                len(rows_matching_run) == 1
            ), f"The database contains multiple documents with run name {run['run_name']}. Contact a database administrator."

            doc_id = rows_matching_run[0].id
            doc = db[doc_id]

            dict_to_add = {
                "step_name": currentStep.type.name,
                "pid": currentStep.id,
                "timestamp": timestamp,
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

            runtime_log.append(f"Flowcell {run['run_name']} was updated successfully.")

        except AssertionError as e:
            errors = True
            runtime_log.append(str(e))
            continue

    if errors:
        raise AssertionError("\n".join(runtime_log))


def parse_run(art):
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
            "^\d{1,3}:\d{2}$", fc["reload_times"]
        ), "Reload run times must be formatted as comma-separated h:mm"
        check_times_list(fc["reload_times"])
        assert check_csv_udf_list(
            "^[0-9.]+$", fc["reload_fmols"]
        ), "Invalid flow cell reload amount(s)"
        assert check_csv_udf_list(
            "^[0-9a-zA-Z.-_]+$", fc["reload_lots"]
        ), "Invalid Reload wash kit"

        return fc

    else:
        return None


def check_times_list(times_list):
    prev_hours, prev_minutes = 0, 0
    for time in times_list:
        hours, minutes = time.split(":")
        hours, minutes = int(hours), int(minutes)
        assert hours > prev_hours or (
            hours == prev_hours and minutes > prev_minutes
        ), f"Times in field {times_list} are non-sequential."
        assert (
            minutes < 60
        ), f"Field {times_list} contains invalid entries (minutes >= 60)."

        prev_hours, prev_minutes = hours, minutes


def get_ONT_db():
    """Mostly copied from write_notes_to_couchdb.py"""
    configf = "~/.statusdb_cred.yaml"

    with open(os.path.expanduser(configf)) as config_file:
        config = yaml.safe_load(config_file)

    url_string = f"https://{config['statusdb'].get('username')}:{config['statusdb'].get('password')}@{config['statusdb'].get('url')}"
    couch = couchdb.Server(url=url_string)

    return couch["nanopore_runs"]


def check_csv_udf_list(pattern, csv_udf_list):
    if csv_udf_list:
        return all([re.match(pattern, element) for element in csv_udf_list])
    else:
        return True


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    try:
        main(lims, args)
    except AssertionError as e:
        sys.stderr.write(str(e))
        sys.exit(2)
