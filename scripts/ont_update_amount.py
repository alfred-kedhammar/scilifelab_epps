from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from utils import udf, formula

DESC = """ EPP "ONT Update Amounts".

Calculate the sample amount based on new (or, if needed, previous) measurements. Written to run between the steps of the
Nanopore ligation library prep.

Alfred Kedhammar, NGI SciLifeLab
"""

def main(lims, args):
        
        currentStep = Process(lims, id=args.pid)

        art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["output-type"] == "Analyte"]

        for art_tuple in art_tuples:
            
            # Fetch info
            art_out = art_tuple[1]["uri"]

            # Calculate amount ng based on info in current step
            conc = art_out.udf["Final Concentration"]
            vol = art_out.udf["Final Volume (uL)"]

            udf.put(art_out, "Amount (ng)", round(conc * vol, 2))
            
            # Calculate amount fmol based on length in this, or previous, step
            size_bp = udf.fetch_last(currentStep, art_tuple, "Size (bp)")

            udf.put(art_out, "Amount (fmol)", round(formula.ng_to_fmol(art_out.udf["Amount (ng)"], size_bp), 2))


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)