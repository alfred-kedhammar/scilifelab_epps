#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process

DESC = """Simple script. For all input-output tuples, check which UDFs are present in both and copy from input to output."""

def main(lims, args):

    currentStep = Process(lims, id=args.pid)
    art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["uri"].type == "Analyte"]

    for art_tuple in art_tuples:

        input_udfs = [kv[0] for kv in art_tuple[0]["uri"].udf.items()]
        output_udfs = [kv[0] for kv in art_tuple[1]["uri"].udf.items()]

        for udf in output_udfs:
            if udf in input_udfs:
                art_tuple[1]["uri"].udf[udf] = art_tuple[0]["uri"].udf[udf]
        
        art_tuple[1]["uri"].put()





if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)