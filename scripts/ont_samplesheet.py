#!/usr/bin/env python

from __future__ import division
import logging
import os
import sys
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from scilifelab_epps.epp import attach_file
from genologics.entities import Process
from datetime import datetime as dt

DESC = """EPP used to generate a MinKNOW sample sheet for ONT samples"""

def main(lims, args):

    currentStep = Process(lims, id=args.pid)

    file_meta = {"pid":currentStep.id, "timestamp":dt.now()}
    log = []

    rows = []
    arts = [art_tuple[0]['uri'] for art_tuple in currentStep.input_output_maps \
        if art_tuple[0]['uri'].type == "Analyte"]

    for art in arts:

        row = {
            "flow_cell_id": art.udf.get("ONT flow cell ID"),
            "sample_id": art.name, # Either pool or sample
            "experiment_id": art.samples[0].project.id,
            "flow_cell_product_code": get_fc_product_code(art),
            "kit": get_kit_string(art)
        }
        
        # Singleton sample
        if len(art.sampes) == 1:
            row["alias"] = ""
            row["barcode"] = ""

            rows.append(row)

        # Pool
        else:
            for sample in art.samples:
                row["alias"] = sample.name
                row["barcode"] = "" # TODO


def get_fc_product_code(sample):

    type_version = f"{sample.udf.get('ONT flow cell type')}_{sample.udf.get('ONT flow cell version')}"
    type_version_to_product_code = {
        "PromethION_R9.4.1"  : "FLO-PRO002",
        "MinION_R9.4.1"      : "FLO-MIN106D",
        "Flongle_R9.4.1"     : "FLO-FLG001",
        "PromethION_R10.4.1" : "FLO-PRO114M",
        "MinION_R10.4.1"     : "FLO-MIN114",
        "Flongle_R10.4.1"    : "FLO-FLG114",
    }

    return type_version_to_product_code[type_version]


def get_kit_string(sample):

    kit_string = sample.udf.get('ONT prep kit')

    if sample.udf.get('ONT expansion kit') != "None":
        kit_string += f" {sample.udf.get('ONT expansion kit')}"

    return kit_string


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)