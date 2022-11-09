#!/usr/bin/env python

DESC = """
Each function in this module corresponds to a single method / application and is tied to a
specific workflow step.

Written by Alfred Kedhammar
"""

import zika
import pandas as pd
import sys
import numpy as np


def setup_QIAseq(currentStep = None, lims = None, local_data = None):
    """
    Normalize to target amount and volume.

    Cases:
    1) Not enough sample       --> Decrease amount, flag
    2) Enough sample           --> OK
    3) Sample too concentrated --> Maintain target concentration, increase
                                   volume as needed up to max 15 ul, otherwise
                                   throw error and dilute manually.
    """

    # Create dataframe from lims or local csv file
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
        "target_amt",
    ]
    
    if local_data:
        df = zika.load_fake_samples(local_data, to_fetch)
    else:
        df = zika.fetch_sample_data(currentStep, to_fetch)

    assert all(df.conc_units == "ng/ul"), "All sample concentrations are expected in 'ng/ul'"
    assert all(df.target_amt > 0), "'Amount taken (ng)' needs to be set greater than zero"
    assert all(df.vol > 0), "Sample volume needs to be greater than zero" 

    # Define constraints
    min_zika_vol = 0.1
    max_final_vol = 15

    # Make calculations
    df["target_conc"] = df.target_amt / df.target_vol
    df["min_transfer_amt"] = np.minimum(df.vol, min_zika_vol) * df.conc
    df["max_transfer_amt"] = np.minimum(df.vol, df.target_vol) * df.conc

    # Define deck
    assert len(df.source_fc.unique()) == 1, "Only one input plate allowed"
    assert len(df.dest_fc.unique()) == 1, "Only one output plate allowed"
    deck = {
        "buffer_plate": 2,
        df.source_fc.unique()[0]: 3,
        df.dest_fc.unique()[0]: 4,
    }

    if not local_data:
        # Load outputs for changing UDF:s
        outputs = {art.name : art for art in currentStep.all_outputs() if art.type == "Analyte"}

    # Cases 1) - 3)
    d = {"sample": [], "buffer": [], "tot_vol": []}
    log = []
    for i, r in df.iterrows():

        # 1) Not enough sample
        if r.max_transfer_amt < r.target_amt:

            sample_vol = min(r.target_vol, r.vol)
            tot_vol = r.target_vol
            buffer_vol = tot_vol - sample_vol

            final_amt = sample_vol * r.conc
            final_conc = final_amt / tot_vol
            
            log.append(
                f"WARNING: Insufficient amount of sample {r.sample_name} (conc {r.conc} ng/ul, vol {r.vol} ul)"
            )
            log.append(f"\t--> Adjusted to {final_amt} ng in {tot_vol} ul ({final_conc} ng/ul)")

            if not local_data:
                op = outputs[r.sample_name]
                op.udf['Amount taken (ng)'] = final_amt
                op.put()

        # 2) Ideal case
        elif r.min_transfer_amt <= r.target_amt <= r.max_transfer_amt:

            sample_vol = r.target_amt / r.conc
            buffer_vol = r.target_vol - sample_vol
            tot_vol = sample_vol + buffer_vol

        # 3) Sample too concentrated -> Increase final volume if possible
        elif r.min_transfer_amt > r.target_amt:

            increased_vol = r.min_transfer_amt / r.target_conc
            assert (
                increased_vol < max_final_vol
            ), f"Sample {r.name} is too concentrated ({r.conc} ng/ul) and must be diluted manually"

            tot_vol = increased_vol
            sample_vol = min_zika_vol
            buffer_vol = tot_vol - sample_vol

            final_amt = sample_vol * r.conc
            final_conc = final_amt / tot_vol

            log.append(
                f"WARNING: High concentration of sample {r.sample_name} ({r.conc} ng/ul)"
            )
            log.append(f"\t--> Adjusted to {final_amt} in {tot_vol} ul ({final_conc} ng/ul)")
            
            if not local_data:
                op = outputs[r.sample_name]
                op.udf['Total Volume (uL)'] = tot_vol
                op.put()

        d["sample"].append(sample_vol)
        d["buffer"].append(buffer_vol)
        d["tot_vol"].append(tot_vol)

    df = df.join(pd.DataFrame(d))

    # Resolve buffer transfers
    df = zika.resolve_buffer_transfers(df, buffer_strategy="column")

    # Generate Mosquito-readable columns
    df = zika.format_worklist(df, deck=deck)

    # Comments to attach to the worklist header
    n_samples = len(df[df.src_type == "sample"])
    comments = [f"This worklist will enact normalization of {n_samples} samples"]

    # Write files and upload
    method_name = "setup_QIAseq"
    pid = "local" if local_data else currentStep.id
    wl_filename, log_filename = zika.get_filenames(method_name, pid)

    zika.write_worklist(
        df=df,
        deck=deck,
        wl_filename=wl_filename,
        comments=comments,
        strategy="multi-aspirate",
    )

    zika.write_log(log, log_filename)

    if not local_data:
        zika.upload_csv(currentStep, lims, wl_filename)
        zika.upload_log(currentStep, lims, log, log_filename)

    # Issue warnings, if any
    if any("WARNING:" in entry for entry in log):
        sys.stderr.write(
            "CSV-file generated with warnings, please check the Log file\n"
        )
        sys.exit(2)
