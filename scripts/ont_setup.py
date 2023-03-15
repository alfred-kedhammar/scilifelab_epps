from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process

DESC = """
EPP "Calculate volumes"
Given a target fmol amount and total volume, use the fmol amount and volume of the input artifact to calculate the volume taken. Decrease target fmol amount if necessary.
"""

def main(lims, args):
        
        currentStep = Process(lims, id=args.pid)
        art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["output-type"] == "Analyte"]

        for art_tuple in art_tuples:
            
            # Fetch info
            target_amt = art_tuple[1]["uri"].udf["Amount (fmol)"]

            prev_amt = art_tuple[0]["uri"].udf["Amount (fmol)"]
            try:
                prev_vol = art_tuple[0]["uri"].udf["Final Volume (uL)"]
            except KeyError:
                 prev_vol = art_tuple[0]["uri"].udf["Volume (uL)"]

            # This is calculated
            vol_taken = min((target_amt / prev_amt) * prev_vol, prev_vol)

            # Put
            art_tuple[1]["uri"].udf["Amount (fmol)"] = min(prev_amt, target_amt)
            art_tuple[1]["uri"].udf["Volume to take (ul)"] = vol_taken
            art_tuple[1]["uri"].put()

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)