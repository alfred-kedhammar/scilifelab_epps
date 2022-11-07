#!/usr/bin/env python

import mosquito

import pandas as pd
import numpy as np
from datetime import datetime as dt



def zika_setup_QIAseq(currentStep):
        
    to_fetch = [
        "sample_name",
        "source_fc",
        "source_well",
        "conc_units",
        "conc",
        "vol",
        "amt",
        "dest_fc",
        "dest_well",
        "dest_fc_name",
        "target_vol",
        "target_amt"
    ]
    
    df = mosquito.fetch_sample_data(currentStep, to_fetch)
    assert all(df.conc_units == "ng/ul"), "All sample concentrations are expected in 'ng/ul'"

    # Constraints
    min_zika_vol = 0.1
    max_final_vol = 15

    # Make calculations
    df["target_conc"] = df.target_amt / df.target_vol
    df["min_transfer_amt"] = min_zika_vol * df.conc
    df["max_transfer_amt"] = df.target_vol * df.conc

    # Cases
    d = {"sample" : [], "buffer" : [], "tot_vol" : []}
    log = []
    for i, r in df.iterrows():

        # Sample too dilute
        if r.max_transfer_amt < r.target_amt:
            
            sample_vol = min(r.target_vol, r.vol)
            tot_vol = r.target_vol
            buffer_vol = tot_vol - sample_vol

            target_pc = round((sample_vol * r.conc / tot_vol) / r.target_conc * 100,2)
            log.append(f"WARNING: Insufficient amount of sample {r.sample_name} (conc {r.conc} ng/ul, vol {r.vol} ul)")
            log.append(f"\t--> Reaching {target_pc}% of target concentration")

            # TODO change udf accordingly

        # Ideal case
        elif r.min_transfer_amt <= r.target_amt <= r.max_transfer_amt:
            
            sample_vol = r.target_amt / r.conc
            buffer_vol = r.target_vol - sample_vol
            tot_vol = sample_vol + buffer_vol
        
        # Sample too concentrated -> Increase final volume if possible
        elif r.min_transfer_amt > r.target_amt:
            
            increased_vol = r.min_transfer_amt / r.target_conc
            assert increased_vol < max_final_vol, \
                f"Sample {r.name} is too concentrated ({r.conc} ng/ul) and must be diluted manually"

            tot_vol = increased_vol
            sample_vol = min_zika_vol
            buffer_vol = tot_vol - sample_vol

            log.append(f"WARNING: High concentration of sample {r.sample_name} (conc {r.conc} ng/ul, vol {r.vol} ul)")
            log.append(f"\t--> Adjusting total volume to {tot_vol} ul")

            # TODO change udf accordingly

        d["sample"].append(sample_vol)
        d["buffer"].append(buffer_vol)
        d["tot_vol"].append(tot_vol)
    
    df = df.join(pd.DataFrame(d))

    df = mosquito.format_worklist(df, buffer_strategy = "column")

    method_name = "setup_QIAseq"
    mosquito.write_worklist(
        data = df,
        method_name = method_name,
        pid = currentStep.id,
        strategy = "multi-aspirate"
        )


