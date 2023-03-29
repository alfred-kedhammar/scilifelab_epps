from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from ont_update_amount import fetch_last
from molar_concentration import fmol_to_ng, ng_to_fmol

DESC = """
EPP "ONT calculate volumes"
Given a target amount and total volume, calculate the volume to take. Decrease target amount if necessary.
Target amount can optionally be specified as fmol, which will populate the Amount (ng) UDF.
"""

def main(lims, args):
        
        currentStep = Process(lims, id=args.pid)
        art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["output-type"] == "Analyte"]

        for art_tuple in art_tuples:
            
            size = fetch_last(currentStep, art_tuple, "Size (bp)")

            # Fetch target amount, either fmol or ng
            try:
                target_amt_fmol = art_tuple[1]["uri"].udf["Amount (fmol)"]
                target_amt_ng = fmol_to_ng(target_amt_fmol, size)

            except KeyError:
                target_amt_ng = art_tuple[1]["uri"].udf["Amount (ng)"]
                target_amt_fmol = ng_to_fmol(target_amt_ng, size)

            # Fetch last known sample volume
            if fetch_last(currentStep, art_tuple, "Final Volume (uL)"):
                prev_vol = fetch_last(currentStep, art_tuple, "Final Volume (uL)")
            else:
                prev_vol = fetch_last(currentStep, art_tuple, "Volume (ul)")

            # Calculate how much sample to take
            conc_units = art_tuple[0]["uri"].udf['Conc. Units']
            if conc_units == "ng/ul":

                vol_to_take = min(
                    target_amt_ng / art_tuple[0]["uri"].udf["Concentration"],   # Enough sample
                    prev_vol                                                # Sample low conc  --> Use target volume                                                                                
                )

                amt_taken_ng = vol_to_take * art_tuple[0]["uri"].udf["Concentration"]
                amt_taken_fmol = ng_to_fmol(amt_taken_ng, size)

            elif conc_units == "nM":

                vol_to_take = min(
                    target_amt_fmol / art_tuple[0]["uri"].udf["Concentration"], # Enough sample
                    prev_vol                                                # Sample low conc  --> Use target volume
                )

                amt_taken_fmol = vol_to_take * art_tuple[0]["uri"].udf["Concentration"]
                amt_taken_ng = fmol_to_ng(amt_taken_fmol, size)

            if "Amount (ng)" in [t[0] for t in art_tuple[1]["uri"].udf.items()]:
                art_tuple[1]["uri"].udf["Amount (ng)"] = round(amt_taken_ng,1)
            art_tuple[1]["uri"].udf["Amount (fmol)"] = round(amt_taken_fmol,1)
            art_tuple[1]["uri"].udf["Volume to take (uL)"] = round(vol_to_take,1)
            art_tuple[1]["uri"].put()


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)