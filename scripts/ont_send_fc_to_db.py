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
from ont_generate_samplesheet import get_minknow_sample_id


DESC = """ Script for EPP "Send ONT flowcell info to StatusDB".
Used to record the washing and reloading of ONT flow cells.
Information is parsed from LIMS and uploaded to the CouchDB database nanopore_runs.
"""


def main(lims, args):
    """ For all samples/flowcells, use the flowcell ID to find the sequencing run entry in the nanopore_runs database.

    Then update the document "lims_fc_info" json object nest with the loading and reloading information.

    In the rare event that multiple sequencing runs are found for the same flowcell, 
    use the LIMS-ID of the previous process (the samplesheet generation step) to identify the correct run.

    Finish by also updating the step UDF log.

    TODO Get parent process ID on a sample-by-sample basis, rather than once for the entire step.
         Current approach may cause issues if samples originate from different steps.
    """

    try:

        currentStep = Process(lims, id=args.pid)

        timestamp = dt.now().strftime("%y%m%d_%H%M%S")

        # Parse inputs and their UDFs
        art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["uri"].type == "Analyte"]

        fcs = []
        for art_tuple in art_tuples:

            fc = parse_fc(art_tuple)
            fcs.append(fc)

        samplesheet_process_id = currentStep.parent_processes()[-1].id
        db = get_ONT_db()
        view = db.view("info/all_stats")

        runtime_log = []
        errors = False
        for fc in fcs:
            
            rows_matching_fc = [row for row in view.rows if f'_{fc["fc_id"]}_' in row.value["TACA_run_path"] and samplesheet_process_id in row.value["TACA_run_path"]]

            try:
                assert len(rows_matching_fc) > 0, f"The database contains no document with flow cell ID {fc['fc_id']} and experiment ID {samplesheet_process_id}. If the run was recently started, wait until it appears in GenStat."
                assert len(rows_matching_fc) == 1, f"The database contains multiple documents with flow cell ID {fc['fc_id']} and experiment ID {samplesheet_process_id}. Contact a database administrator."
                
                doc_id = rows_matching_fc[0].id
                doc = db[doc_id]

                dict_to_add = {
                    "pid": currentStep.id, 
                    "timestamp": timestamp,
                    "qc": fc["qc"],
                    "load_fmol": fc["load_fmol"],
                    "reload_times": fc["reload_times"],
                    "reload_fmols": fc["reload_fmols"],
                    "reload_lots": fc["reload_lots"]
                }

                try:
                    # Try to find pre-existing list to append to
                    lims_list = doc["lims_fc_info"]
                except KeyError:
                    # Create new entry
                    lims_list = []

                lims_list.append(dict_to_add)

                doc.update({"lims_fc_info" : lims_list})
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


def parse_fc(art_tuple):
    """ For each art_tuple, assert UDFs and return parsed dictionary """

    fc = {}
    fc["fc_id"] = art_tuple[0]["uri"].udf.get("ONT flow cell ID")
    fc["minknow_sample_id"] = get_minknow_sample_id(art_tuple[0]["uri"])
    fc["qc"] = art_tuple[0]["uri"].udf.get("ONT Flow Cell QC Pore Count")
    fc["load_fmol"] = art_tuple[0]["uri"].udf.get("ONT flow cell load amount (fmol)")

    fc["reload_times"] = art_tuple[1]["uri"].udf.get("ONT reload run time (hh:mm)").replace(" ","").split(",") if \
                         art_tuple[1]["uri"].udf.get("ONT reload run time (hh:mm)") else None
    fc["reload_fmols"] = art_tuple[1]["uri"].udf.get("ONT reload amount (fmol)").replace(" ","").split(",") if \
                         art_tuple[1]["uri"].udf.get("ONT reload amount (fmol)") else None
    fc["reload_lots"] =  art_tuple[1]["uri"].udf.get("ONT reload wash kit").replace(" ","").split(",") if \
                         art_tuple[1]["uri"].udf.get("ONT reload wash kit") else None
    
    if fc["reload_times"] or fc["reload_fmols"] or fc["reload_lots"]:
        
        assert fc["reload_times"] and fc["reload_fmols"] and fc["reload_lots"] and \
            len(fc["reload_times"]) == len(fc["reload_fmols"]) == len(fc["reload_lots"]), \
            "All reload UDFs within a row must have the same number of comma-separated values"
        
        assert check_csv_udf_list("^\d{1,3}:\d{2}$", fc["reload_times"]), \
            "Reload run times must be formatted as comma-separated h:mm"
        check_times_list(fc["reload_times"])
        assert check_csv_udf_list("^[0-9.]+$", fc["reload_fmols"]), \
            "Invalid flow cell reload amount(s)"
        assert check_csv_udf_list("^[0-9a-zA-Z.-_]+$", fc["reload_lots"]), \
            "Invalid Reload wash kit"
        
    return fc


def check_times_list(times_list):

    prev_hours, prev_minutes = 0, 0
    for time in times_list:

        hours, minutes = time.split(":")
        hours, minutes = int(hours), int(minutes)
        assert hours > prev_hours or (hours == prev_hours and minutes > prev_minutes), f"Times in field {times_list} are non-sequential."
        assert minutes < 60, f"Field {times_list} contains invalid entries (minutes >= 60)."
        
        prev_hours, prev_minutes = hours, minutes


def get_ONT_db():
    """Mostly copied from write_notes_to_couchdb.py"""
    configf = '~/.statusdb_cred.yaml'

    with open(os.path.expanduser(configf)) as config_file:
        config = yaml.safe_load(config_file)

    url_string = f"https://{config['statusdb'].get('username')}:{config['statusdb'].get('password')}@{config['statusdb'].get('url')}"
    couch = couchdb.Server(url=url_string)

    return couch['nanopore_runs']


def check_csv_udf_list(pattern, csv_udf_list):
    if csv_udf_list:
        return all([re.match(pattern, element) for element in csv_udf_list])
    else:
        return True


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)