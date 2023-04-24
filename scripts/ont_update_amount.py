from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from utils import formula, udf_tools

DESC = """ EPP "ONT Update Amounts".

Calculate the sample amount based on new (or, if needed, previous) measurements. Written to run between the steps of the
Nanopore ligation library prep.

Alfred Kedhammar, NGI SciLifeLab
"""


def main(lims, args):
    currentStep = Process(lims, id=args.pid)

    art_tuples = udf_tools.get_art_tuples(currentStep)

    for art_tuple in art_tuples:
        # Calculate amount ng based on info in current step
        try:
            conc = udf_tools.fetch(
                art_tuple[1]["uri"], ["Final Concentration", "Concentration"]
            )
        except:
            conc = udf_tools.fetch_from_tuple(
                art_tuple, ["Final Concentration", "Concentration"]
            )

        try:
            vol = udf_tools.fetch(
                art_tuple[1]["uri"], ["Final Volume (uL)", "Volume (ul)"]
            )
        except:
            vol = udf_tools.fetch_from_tuple(
                art_tuple, ["Final Volume (uL)", "Volume (ul)"]
            )

        try:
            udf_tools.put(
                art_tuple[1]["uri"], "Amount (ng)", round(conc * vol, 2), on_fail=None
            )
        except TypeError:
            udf_tools.put(
                art_tuple[0]["uri"], "Amount (ng)", round(conc * vol, 2), on_fail=None
            )

        # Calculate amount fmol based on length in this, or previous, step
        size_bp = udf_tools.fetch_last(currentStep, art_tuple, "Size (bp)")

        try:
            udf_tools.put(
                art_tuple[1]["uri"],
                "Amount (fmol)",
                round(
                    formula.ng_to_fmol(art_tuple[1]["uri"].udf["Amount (ng)"], size_bp),
                    2,
                ),
                on_fail=None,
            )
        except TypeError:
            udf_tools.put(
                art_tuple[0]["uri"],
                "Amount (fmol)",
                round(
                    formula.ng_to_fmol(art_tuple[0]["uri"].udf["Amount (ng)"], size_bp),
                    2,
                ),
                on_fail=None,
            )


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
