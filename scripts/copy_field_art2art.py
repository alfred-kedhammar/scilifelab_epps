#!/usr/bin/env python
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process



def main(args):
    lims = Lims(BASEURI,USERNAME,PASSWORD)
    process = Process(lims, id=args.pid)
    for io in process.input_output_maps:
        if io[1]['output-generation-type'] != 'PerInput':
            continue
        for field in args.fields:
            if field in io[0]['uri'].udf:
                io[1]['uri'].udf[field] = io[0]['uri'].udf[field]
        io[1]['uri'].put()






if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--pid',
                        help='Lims id for current Process', required=True)
    parser.add_argument('--field', '-f',
            dest="fields", action='append', help='fields to copy', required=True)
    args = parser.parse_args()
    main(args)
