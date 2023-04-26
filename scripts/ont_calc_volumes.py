from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from utils.udf_tools import fetch_last, fetch, put
from utils.formula import fmol_to_ng, ng_to_fmol

DESC = """
EPP "ONT calculate volumes"

Given any output UDF(s)
- Amount (fmol)
- Amount (ng)
- Volume to take (uL)

and last known UDFs
- Final Volume (uL) / Volume (uL)
- Concentration
- Size (bp)

Will use ONE of the output UDFs (prioritized in the listed order) to calculate all three output UDFs.
"""


def main(lims, args):
    currentStep = Process(lims, id=args.pid)

    art_tuples = [
        art_tuple
        for art_tuple in currentStep.input_output_maps
        if art_tuple[1]["output-type"] == "Analyte"
    ]

    for art_tuple in art_tuples:
        size = fetch_last(currentStep, art_tuple, "Size (bp)")
        conc_units = fetch(art_tuple[0]["uri"], "Conc. Units", on_fail="ng/ul")

        # Fetch target amount, either fmol or ng
        try:
            target_amt_fmol = art_tuple[1]["uri"].udf["Amount (fmol)"]
            target_amt_ng = fmol_to_ng(target_amt_fmol, size)
            basis = "fmol"
        except KeyError:
            try:
                target_amt_ng = art_tuple[1]["uri"].udf["Amount (ng)"]
                target_amt_fmol = ng_to_fmol(target_amt_ng, size)
                basis = "ng"
            except KeyError:
                target_vol = art_tuple[1]["uri"].udf["Volume to take (uL)"]
                basis = "vol"

        # Fetch last known sample volume
        prev_vol = fetch_last(
            currentStep, art_tuple, ["Final Volume (uL)", "Volume (ul)"]
        )

        # Calculate
        if basis == "fmol" or basis == "ng":
            if conc_units == "nM":
                target_vol = target_amt_fmol / fetch_last(
                    currentStep, art_tuple, ["Final Concentration", "Concentration"]
                )
            elif conc_units == "ng/ul":
                target_vol = target_amt_ng / fetch_last(
                    currentStep, art_tuple, ["Final Concentration", "Concentration"]
                )

        vol_to_take = min(target_vol, prev_vol)

        if conc_units == "nM":
            amt_taken_fmol = vol_to_take * fetch_last(
                currentStep, art_tuple, ["Final Concentration", "Concentration"]
            )
            amt_taken_ng = fmol_to_ng(amt_taken_fmol, size)

        elif conc_units == "ng/ul":
            amt_taken_ng = vol_to_take * fetch_last(
                currentStep, art_tuple, ["Final Concentration", "Concentration"]
            )
            amt_taken_fmol = ng_to_fmol(amt_taken_ng, size)

        put(art_tuple[1]["uri"], "Amount (ng)", round(amt_taken_ng, 1))
        put(art_tuple[1]["uri"], "Amount (fmol)", round(amt_taken_fmol, 1))
        put(art_tuple[1]["uri"], "Volume to take (uL)", round(vol_to_take, 1))


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
