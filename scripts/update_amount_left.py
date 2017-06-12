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
        if "Amount taken (ng)" in io[1]['uri'].udf:
            if "Amount left (ng)" in io[0]['uri'].udf:
                io[0]['uri'].udf["Amount left (ng)"] = io[0]['uri'].udf["Amount left (ng)"] - io[1]['uri'].udf["Amount taken (ng)"]
            else:
                io[0]['uri'].udf["Amount left (ng)"] = io[0]['uri'].udf["Amount(ng)"] - io[1]['uri'].udf["Amount taken (ng)"]
            io[0]['uri'].put()






if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--pid',
                        help='Lims id for current Process', required=True)
    args = parser.parse_args()
    main(args)
