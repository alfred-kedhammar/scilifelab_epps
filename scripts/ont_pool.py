from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from zika_utils import fetch_sample_data
from molar_concentration import calculate_fmol
from numpy import minimum

DESC = """
EPP "ONT pooling"
Given a target fmol amount and total volume, try to create an equimolar pool.
"""

def main(lims, args):
        """
        1) Find molar proportions of samples within each pool
        2) Apply inverse molar proprotions to target pool volume to get the sample
           volumes corresponding to equimolar pooling.
        3) 
        """

        currentStep = Process(lims, id=args.pid)

        pools = [art for art in currentStep.all_outputs() if art.type == "Analyte"]
        pools.sort(key=lambda pool: pool.name)
        
        to_fetch = {
            # Search within step
            "sample_name"          : "art_tuple[0]['uri'].name",
            "vol"                  : "art_tuple[0]['uri'].udf['Final Volume (uL)']",
            "conc_ng"              : "art_tuple[0]['uri'].udf['Final Concentration']",
            "dst_name"             : "art_tuple[1]['uri'].name",
            "dst_id"               : "art_tuple[1]['uri'].location[0].id",
            "pool_target_amt_fmol" : "art_tuple[1]['uri'].udf['Amount (fmol)']",
            "pool_target_vol"      : "art_tuple[1]['uri'].udf['Final Volume (uL)']",
            # Seach recursively
            "size"                 : "Size (bp)",
        }
            
        df = fetch_sample_data(currentStep, to_fetch)

        df["conc_nM"] = df.apply(lambda x: calculate_fmol(x["conc_ng"], x["size"]), axis = 1)
        df["amt_ng"] = df.conc_ng * df.vol
        df["amt_fmol"] = df.conc_nM * df.vol

        for pool in pools:
            # Subset data
            df_pool = df[df.dst_name == pool.name].copy()

            target_amt_fmol = df_pool.pool_target_amt_fmol[0] / len(df_pool)
            pool_target_amt_fmol = df_pool.pool_target_amt_fmol[0]

            df_pool["prop_nM"] = df_pool.conc_nM / sum(df_pool.conc_nM)
            df_pool["prop_nM_inv"] = (1 / df_pool["prop_nM"]) / sum((1 / df_pool["prop_nM"]))

            df_pool["sample_vol"] = minimum(df_pool.pool_target_amt_fmol[0] / df_pool.conc_nM, df_pool.vol)

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)