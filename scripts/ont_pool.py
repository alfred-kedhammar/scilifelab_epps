from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from ont_update_amount import fetch_last

DESC = """
EPP "ONT pooling"
Given a target fmol amount and total volume, try to create an equimolar pool.
"""

def main(lims, args):
        
        currentStep = Process(lims, id=args.pid)

        # TODO

        # Iterate across pools
        pools = [output for output in currentStep.all_outputs if output.type == "Analyte"]
        for pool in pools:
             

        d = {}
        for art_tuple in art_tuples:
            
            # Fetch info
            target_amt = art_tuple[1]["uri"].udf["Amount (fmol)"] / len(art_tuples)
            target_vol = art_tuple[1]["uri"].udf["Total Volume (uL)"]

            prev_amt = art_tuple[0]["uri"].udf["Amount (fmol)"]
            if fetch_last(currentStep, art_tuple, "Final Volume (uL)"):
                prev_vol = fetch_last(currentStep, art_tuple, "Final Volume (uL)")
            else:
                 prev_vol = fetch_last(currentStep, art_tuple, "Volume (ul)")

            # This is calculated
            vol_to_take = min((target_amt / prev_amt) * prev_vol, prev_vol, target_vol)

            # Put
            art_tuple[1]["uri"].udf["Amount (fmol)"] = min(prev_amt, target_amt)
            art_tuple[1]["uri"].udf["Volume to take (uL)"] = vol_to_take
            art_tuple[1]["uri"].put()

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)