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

    samples = [art_tuple[0]['uri'] for art_tuple in currentStep.input_output_maps \
        if art_tuple[0]['uri'].type == "Analyte"]

    for sample in samples:
        
        row = {
        "flow_cell_id": sample.udf.get("ONT flow cell ID"),
        "sample_id": sample.name,
        "experiment_id": "", #TODO
        "flow_cell_product_code": "", #TODO
        "kit": "", #TODO
        "alias": "", #TODO
        "barcode": "" #TODO
        }



if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)