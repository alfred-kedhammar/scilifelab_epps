from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process

DESC = """ Calculate the sample metrics based on new and previous measurements. Written to run between the steps of the
Nanopore ligation library prep.

Alfred Kedhammar, NGI SciLifeLab
"""

def main(lims, args):
        
        currentStep = Process(lims, id=args.pid)

        art_tuples = [art_tuple for art_tuple in currentStep.input_output_maps if art_tuple[1]["output-type"] == "Analyte"]

        for art_tuple in art_tuples:
            
            art_in = art_tuple[0]["uri"]
            art_out = art_tuple[1]["uri"]

            try:
                conc = art_out.udf["Concentration"]
            except KeyError:
                conc = fetch_last(currentStep, art_tuple, "Concentration")

            try:
                vol = art_out.udf["Volume to take (uL)"]
            except KeyError:
                try:
                    vol = art_out.udf["Final Volume (uL)"]
                except:
                    raise AssertionError
            art_out.udf["Amount (ng)"] = round(conc * vol, 2)
            
            # Calculate fmol
            try:
                 # Use current length if present
                size_bp = art_out.udf["Size (bp)"]
            except KeyError:
                # Otherwise, use last known length
                size_bp = fetch_last(currentStep, art_tuple, "Size (bp)")
            art_out.udf["Amount (fmol)"] = round(calculate_fmol(art_out.udf["Amount (ng)"], size_bp), 2)

            if currentStep.parent_processes() != [None]:
                try:
                    # Calculate yield if amount (ng) present in previous step, otherwise pass
                    art_out.udf["% Yield (ng/ng)"] = round((art_out.udf["Amount (ng)"] / art_in.udf["Amount (ng)"]) * 100,2)
                    try:
                        # If no cumulative yield in previous field, use yield
                        cml_yield = art_in.udf["% Cumulative yield (ng/ng)"]
                        art_out.udf["% Cumulative yield (ng/ng)"] = round((art_out.udf["% Yield (ng/ng)"] * cml_yield) / 100,2)
                    except KeyError:
                        art_out.udf["% Cumulative yield (ng/ng)"] = round(art_out.udf["% Yield (ng/ng)"],2)
                except KeyError:
                    pass
            else:
                pass

            art_out.put()


def calculate_fmol(amount_ng, size_bp):
    # Formula based on NEBioCalculator
    # https://nebiocalculator.neb.com/#!/dsdnaamt
    return 10**6 * (amount_ng) / (size_bp * 617.96 + 36.04)


def fetch_last(currentStep, art_tuple, target_udf):

    # Return udf if present in input of current step
    if target_udf in [item_tuple[0] for item_tuple in art_tuple[0]["uri"].udf.items()]:
        return art_tuple[0]["uri"].udf[target_udf]

    # Start looking though previous steps. Use input articles.
    else:
        input_art = art_tuple[0]["uri"]
        # Traceback of artifact ID, step and UDFs
        history = [(input_art.id, currentStep.type.name, art_tuple[1]["uri"].udf.items())]
        
        while True:
            if input_art.parent_process:
                pp = input_art.parent_process
                pp_tuples = pp.input_output_maps

                # Find the input whose output is the current artifact
                pp_input_art = [pp_tuple[0]["uri"] for pp_tuple in pp_tuples if pp_tuple[1]["uri"].id == input_art.id][0]
                history.append((pp_input_art.id, pp.type.name, pp_input_art.udf.items()))

                if target_udf in [tuple[0] for tuple in pp_input_art.udf.items()]:
                    return pp_input_art.udf[target_udf]
                else:
                    input_art = pp_input_art

            else:
                return None


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)