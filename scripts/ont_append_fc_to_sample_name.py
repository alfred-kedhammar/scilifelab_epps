#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from ont_generate_samplesheet import get_minknow_sample_id

DESC = """ Script for EPP "Append ONT flow cell to sample name".
Append flow cell information from previous step to sample names in the current step.
"""


def main(lims, args):
    """ 
    Ex) 
    A sample named myPooledSample consisting of pool with ID 24-1234123 with sample all originating from project P12345,
    sequencing on the PromethION FC PAM12345 at position 1A.

    Sample name in --> sample name out
    ===================================================================
    myPooledSample --> myPooledSample (P12345_24-1234123, PAM12345, 1A)
    
    """

    currentStep = Process(lims, id=args.pid)
    art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["uri"].type == "Analyte"]

    for art_tuple in art_tuples:
        
        minknow_sample_id = get_minknow_sample_id(art_tuple[0]["uri"])
        fc_id = art_tuple[0]["uri"].udf["ONT flow cell ID"]
        fc_pos = art_tuple[0]["uri"].udf["ONT flow cell position"]

        if fc_pos == "None":
            new_name = f"{art_tuple[0]['uri'].name} ({minknow_sample_id}, {fc_id})"
        else:
            new_name = f"{art_tuple[0]['uri'].name} ({minknow_sample_id}, {fc_id}, {fc_pos})"

        art_tuple[1]["uri"].name = new_name
        art_tuple[1]["uri"].put()


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)