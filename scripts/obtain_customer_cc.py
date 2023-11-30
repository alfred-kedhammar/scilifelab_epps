#!/usr/bin/env python
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims


def main(lims, args):
    pro = Process(lims, id=args.pid)
    for inp in pro.all_inputs():
        updated = False
        if "Customer Conc" in inp.samples[0].udf:
            inp.udf["Concentration"] = inp.samples[0].udf["Customer Conc"]
            updated = True
        if "Customer RIN" in inp.samples[0].udf:
            inp.udf["RIN"] = inp.samples[0].udf["Customer RIN"]
            updated = True
        if "Customer Volume" in inp.samples[0].udf:
            inp.udf["Volume (ul)"] = inp.samples[0].udf["Customer Volume"]
            updated = True
        if updated:
            inp.put()


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
