from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from utils import formula, udf_tools

DESC = """ EPP "ONT Update Amounts".

Calculate and populate the ng and fmol amount of the output UDFs of the current step.

The calculation is based on the "Concentration", "Conc. Units" and "Volume (ul)" output UDFs as well as the last recorded "Size (bp)" UDF of the sample.

Alfred Kedhammar, NGI SciLifeLab
"""


def main(lims, args):
    currentStep = Process(lims, id=args.pid)

    art_tuples = udf_tools.get_art_tuples(currentStep)

    for art_tuple in art_tuples:
        # Get last known length
        size_bp = udf_tools.fetch_last(
            currentStep, art_tuple, "Size (bp)", on_fail=None
        )

        # Get current metrics
        vol = udf_tools.fetch(art_tuple[1]["uri"], "Volume (ul)")
        conc_units = udf_tools.fetch(art_tuple[1]["uri"], "Conc. Units")
        assert conc_units in [
            "ng/ul",
            "nM",
        ], f'Unsupported conc. units "{conc_units}" for art {art_tuple[1]["uri"].name}'

        # Fetch or calculate conc in ng/ul
        if conc_units == "nM" and size_bp:
            conc_ng_ul = formula.nM_to_ng_ul(
                nM=udf_tools.fetch(art_tuple[1]["uri"], "Concentration"), bp=size_bp
            )
        elif conc_units == "ng/ul":
            conc_ng_ul = udf_tools.fetch(art_tuple[1]["uri"], "Concentration")
        else:
            raise AssertionError(
                f'Cannot parse concentration of {art_tuple[1]["uri"].name}'
            )

        # Calculate and put ng amount
        amount_ng = round(conc_ng_ul * vol, 2)
        udf_tools.put(art_tuple[1]["uri"], "Amount (ng)", amount_ng, on_fail=None)

        # Calculate and put fmol amount
        udf_tools.put(
            art_tuple[1]["uri"],
            "Amount (fmol)",
            round(
                formula.ng_to_fmol(amount_ng, size_bp),
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
