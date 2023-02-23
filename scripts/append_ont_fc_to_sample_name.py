#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process

DESC = """ Append flow cell information from previous step to sample names in the current step. """


def main(lims, args):

    currentStep = Process(lims, id=args.pid)
    art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["uri"].type == "Analyte"]

    for art_tuple in art_tuples:

        fc_id = art_tuple[0]["uri"].udf["ONT flow cell ID"]
        fc_pos = art_tuple[0]["uri"].udf["ONT flow cell position"]

        if fc_pos == "None":
            new_name = f"{art_tuple[0]['uri'].name} ({fc_id})"
        else:
            new_name = f"{art_tuple[0]['uri'].name} ({fc_id}, {fc_pos})"

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