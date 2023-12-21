#!/usr/bin/env python
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims


def main(args):
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    process = Process(lims, id=args.pid)
    for io in process.input_output_maps:
        if io[1]["output-generation-type"] != "PerInput":
            continue
        for idx, field in enumerate(args.fields):
            if field in io[0]["uri"].udf:
                if args.destfields:
                    io[1]["uri"].udf[args.destfields[idx]] = io[0]["uri"].udf[field]
                else:
                    io[1]["uri"].udf[field] = io[0]["uri"].udf[field]
        io[1]["uri"].put()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--pid", help="Lims id for current Process", required=True)
    parser.add_argument(
        "--field",
        "-f",
        dest="fields",
        action="append",
        help="fields to copy from",
        required=True,
    )
    parser.add_argument(
        "--destfield",
        "-d",
        dest="destfields",
        action="append",
        help="fields to copy to",
    )
    args = parser.parse_args()
    main(args)
