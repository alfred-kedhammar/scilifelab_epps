#!/usr/bin/env python

DESC = """EPP script to calculating final concentration
by multiplying the current concentration with dilution fold.

Author: Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
"""
import sys
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import EppLogger


def multiply_with_dilution_factor(pro, aggregate):
    log = []
    if aggregate:
        artifacts = pro.all_inputs(unique=True)
    else:
        artifacts = pro.result_files()

    for art in artifacts:
        # Only do calculation when concentration value exists
        try:
            _org_conc = art.udf["Concentration"]
            try:
                # Multipy concentraion with dilution fold,and update dilution fold to 1 to avoid error due to multiple operations
                dilution_fold = art.udf["Dilution Fold"]
                art.udf["Concentration"] = art.udf["Concentration"] * dilution_fold
                art.udf["Dilution Fold"] = 1
                log.append(
                    "Sample {} original concentration {} multiplied with dilution fold {}. Dilution fold reset to 1. ".format(
                        art.name.split(" ")[0],
                        art.udf["Concentration"],
                        art.udf["Dilution Fold"],
                    )
                )
            except KeyError:
                log.append(
                    "Sample {} does not have a dilution fold. The original value is kept.".format(
                        art.name.split(" ")[0]
                    )
                )
        except KeyError:
            log.append(
                "Sample {} does not have a concentration value.".format(
                    art.name.split(" ")[0]
                )
            )
        art.put()
    print("".join(log), file=sys.stderr)


def main(lims, pid, aggregate, epp_logger):
    pro = Process(lims, id=pid)
    multiply_with_dilution_factor(pro, aggregate)


if __name__ == "__main__":
    # Initialize parser with standard arguments and description
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", dest="pid", help="Lims id for current Process")
    parser.add_argument(
        "--log",
        dest="log",
        help=(
            "File name for standard log file, " "for runtime information and problems."
        ),
    )
    parser.add_argument(
        "--aggregate",
        dest="aggregate",
        action="store_true",
        help=("Use this tag if current Process is an " "aggregate QC step"),
    )
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args.pid, args.aggregate, epp_logger)
