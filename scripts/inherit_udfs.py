from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from utils import udf_tools

DESC = """
EPP "inherit_udfs".

The UDFs specified in the args are read from the input artifacts and written to the output artifacts.
"""


def main(lims, args):
    currentStep = Process(lims, id=args.pid)

    art_tuples = [
        art_tuple
        for art_tuple in currentStep.input_output_maps
        if art_tuple[0]["uri"].type == "Analyte"
    ]

    for art_tuple in art_tuples:
        ip, op = art_tuple[0]["uri"], art_tuple[1]["uri"]

        if args.udfs:
            for target_udf in args.udfs:
                if udf_tools.is_filled(ip, target_udf):
                    val = udf_tools.fetch(ip, target_udf)
                    udf_tools.put(op, target_udf, val, on_fail=None)

        else:
            ip_udfs = udf_tools.list_udfs(ip)

            for udf in ip_udfs:
                udf_tools.put(op, udf, ip.udf[udf], on_fail=None)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    parser.add_argument(
        "--udfs", metavar="U", type=str, nargs="+", help="UDFs to inherit, as strings"
    )
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
