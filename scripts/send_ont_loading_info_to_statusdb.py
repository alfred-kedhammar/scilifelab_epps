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
from generate_ont_samplesheet import get_minknow_sample_id
import log_udfs

DESC = """EPP used to record the library input and washing of ONT flow cells.
Information is parsed from LIMS and uploaded to the CouchDB database nanopore_runs"""


def main(lims, args):
    """ For all samples/flowcells, use the flowcell ID to find the sequencing run entry in the nanopore_runs database.

    Then update the document "lims_loading_and_washing" json object nest with the loading and reloading information.

    In the rare event that multiple sequencing runs are found for the same flowcell, 
    use the LIMS-ID of the previous process (the samplesheet generation step) to identify the correct run.
    """

    try:

        currentStep = Process(lims, id=args.pid)

        timestamp = dt.now().strftime("%y%m%d_%H%M%S")

        # Parse inputs and their UDFs
        art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["uri"].type == "Analyte"]

        fcs = []
        for art_tuple in art_tuples:

            fc = {}
            fc["fc_id"] = art_tuple[0]["uri"].udf.get("ONT flow cell ID")
            fc["minknow_sample_id"] = get_minknow_sample_id(art_tuple[0]["uri"])

            fc["reload_times"] = art_tuple[1]["uri"].udf.get("ONT reload run time (hh:mm)").replace(" ","").split(",") if \
                                art_tuple[1]["uri"].udf.get("ONT reload run time (hh:mm)") else None
            fc["reload_fmols"] = art_tuple[1]["uri"].udf.get("ONT reload amount (fmol)").replace(" ","").split(",") if \
                                art_tuple[1]["uri"].udf.get("ONT reload amount (fmol)") else None
            fc["reload_lots"] =  art_tuple[1]["uri"].udf.get("ONT reload wash kit").replace(" ","").split(",") if \
                                art_tuple[1]["uri"].udf.get("ONT reload wash kit") else None

            # Assert correct input
            assert check_csv_udf_list("^\d{2,3}:\d{2}$", fc["reload_times"]), \
                "Reload run times must be formatted as comma-separated hh:mm"
            check_times_list(fc["reload_times"])
            assert check_csv_udf_list("^[0-9.]+$", fc["reload_fmols"]), \
                "Invalid flow cell reload amount(s)"
            assert check_csv_udf_list("^[0-9a-zA-Z.-_]+$", fc["reload_lots"]), \
                "Invalid Reload wash kit"
            
            if fc["reload_times"] or fc["reload_fmols"] or fc["reload_lots"]:
                assert fc["reload_times"] and fc["reload_fmols"] and fc["reload_lots"] and \
                    len(fc["reload_times"]) == len(fc["reload_fmols"]) == len(fc["reload_lots"]), \
                    "All reload UDFs within a row must have the same number of comma-separated values"

            fcs.append(fc)


        samplesheet_process_id = currentStep.parent_processes()[-1].id
        db = get_ONT_db()
        view = db.view("info/all_stats")

        for fc in fcs:
            
            rows_matching_fc = [row for row in view.rows if f'_{fc["fc_id"]}_' in row.value["TACA_run_path"]]

            if len(rows_matching_fc) > 0:

                if len(rows_matching_fc) > 1:
                    """ Multiple runs match the FC ID, try to narrow down. 

                    If the target run was started using a samplesheet generated in the previous step, the LIMS-ID
                    of the step should be present in the experiment name.
                    """
                    rows_matching_fc = [row for row in rows_matching_fc if \
                                        samplesheet_process_id in row.value["TACA_run_path"] and \
                                        f'_{fc["fc_id"]}_' in row.value["TACA_run_path"]]
                    
                    if len(rows_matching_fc) == 1:
                        pass
                    else:
                        sys.stderr.write(f"The database contains multiple documents whose samplesheet LIMS ID "+
                                        f"{samplesheet_process_id} and flowcell ID {fc['fc_id']} are identical")
                        sys.exit(2)

                doc_id = rows_matching_fc[0].id
                doc = db[doc_id]

                dict_to_add = {
                    "pid": currentStep.id, 
                    "timestamp": timestamp,
                    "reload_times": fc["reload_times"],
                    "reload_fmols": fc["reload_fmols"],
                    "reload_lots": fc["reload_lots"]
                }

                try:
                    lims_list = doc["lims_fc_reloading"]
                except KeyError:
                    lims_list = []

                lims_list.append(dict_to_add)

                doc.update({"lims_fc_reloading" : lims_list})
                db[doc.id] = doc

            else:
                sys.stderr.write(f"The flowcell {fc['fc_id']} was not found in the database. "+
                                "If the run was recently started, wait until it appears in GenStat.")
                sys.exit(2)

        # Also update log after uploading to StatusDB
        log_udfs.main(lims, args)

    except AssertionError as e:
        sys.stderr.write(str(e))
        sys.exit(2)


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