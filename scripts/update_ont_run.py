#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from datetime import datetime as dt
import pandas as pd
import re
import shutil
import os

DESC = """EPP used to record the library input and washing of ONT flow cells.
Information is parsed from LIMS and uploaded to the CouchDB database nanopore_runs"""


def main(lims, args):
    """
    """

    currentStep = Process(lims, id=args.pid)

    arts = [art for art in currentStep.all_inputs() \
        if art.type == "Analyte"]
    
    timestamp = dt.now().strftime("%y%m%d_%H%M%S")

    rows = []
    for art in arts:

        row = {
            "fc_id": art.udf.get("ONT flow cell ID"),
            "load_fmol": art.udf.get('ONT flow cell load amount (fmol)'),
            "reload_times": art.udf.get("ONT reload run time (hh:mm)").replace(" ","").split(","),
            "reload_fmols": art.udf.get("ONT reload amount (fmol)").replace(" ","").split(","),
            "reload_lots":  art.udf.get("ONT reload wash kit").replace(" ","").split(",")
        }
        
        rows.append(row)

    #df = pd.DataFrame(rows)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)