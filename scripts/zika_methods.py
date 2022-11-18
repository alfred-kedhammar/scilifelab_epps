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


def norm(
    currentStep=None, 
    lims=None, 
    local_data=None,        # Fetch sample data from local .tsv instead of LIMS
    user_stats=False,       # Fetch sample conc and vol from the user instead of RC
    volume_expansion=True,  # For samples that are too concentrated, increase target volume to obtain correct conc
    multi_aspirate=True     # Use multi-aspiration to fit buffer and sample into the same transfer, if possible
    ):
    """
    Normalize to target amount and volume.

    Cases:
    1) Not enough sample       --> Decrease amount, flag
    2) Enough sample           --> OK
    3) Sample too concentrated --> if volume_expansion:
                                    Increase volume to obtain target concentration
                                   else:
                                    Maintain target volume and allow sample to be above target concentration
    """

    # Define constraints    BRAVO (Assumed)
    zika_min_vol = 2        # 2
    well_dead_vol = 0       # 0
    well_max_vol = 180      # 180


    # Create dataframe from LIMS or local csv file

    to_fetch = [
        # Sample info
        "sample_name",
        # Plates and positions
        "source_fc",
        "source_well",
        "dest_fc",
        "dest_well",
        "dest_fc_name",
        # Target info
        "target_amt",
        "target_vol",
        # Changes to src
        "amt_taken",
        "vol_taken"
    ]

    if user_stats:
        to_fetch = to_fetch + ["user_conc", "user_vol"]
    else:
        to_fetch = to_fetch + ["conc", "conc_units", "vol"]
    
    if local_data:
        df = zika.load_fake_samples(local_data, to_fetch)
    else:
        df = zika.fetch_sample_data(currentStep, to_fetch)


    # Modify and assert fetched data
    
    if user_stats:
        df.rename(columns = {"user_conc" : "conc", "user_vol" : "vol"}, inplace = True)

    # Take dead volume into account
    df["full_vol"] = df.vol.copy()
    df.loc[:,"vol"] = df.vol - well_dead_vol

    if "conc_units" in df.columns:
        assert all(df.conc_units == "ng/ul"), "All sample concentrations are expected in 'ng/ul'"
    assert all(df.target_amt > 0), "Target amount needs to be greater than zero"
    assert all(df.vol > 0), f"Sample volume too low" 


    # Make calculations
    df["target_conc"] = df.target_amt / df.target_vol
    df["min_transfer_amt"] = np.minimum(df.vol, zika_min_vol) * df.conc
    df["max_transfer_amt"] = np.minimum(df.vol, df.target_vol) * df.conc

    # Define deck
    assert len(df.source_fc.unique()) == 1, "Only one input plate allowed"
    assert len(df.dest_fc.unique()) == 1, "Only one output plate allowed"
    deck = {
        "buffer_plate": 2,
        df.source_fc.unique()[0]: 3,
        df.dest_fc.unique()[0]: 4,
    }

    # Write log header
    log = []
    log.append("Log start\n")

    # Comments to attach to the worklist header
    comments = []
    n_samples = len(df)
    comments = [f"This worklist will enact normalization of {n_samples} samples"]

    # Load outputs for changing UDF:s
    if not local_data:
        outputs = {art.name : art for art in currentStep.all_outputs() if art.type == "Analyte"}

    # Cases
    d = {"sample_vol": [], "buffer_vol": [], "tot_vol": []}
    for i, r in df.iterrows():

        # 1) Not enough sample
        if r.max_transfer_amt < r.target_amt:

            sample_vol = min(r.target_vol, r.vol)
            tot_vol = r.target_vol
            buffer_vol = tot_vol - sample_vol

            final_amt = sample_vol * r.conc
            final_conc = final_amt / tot_vol

        # 2) Ideal case
        elif r.min_transfer_amt <= r.target_amt <= r.max_transfer_amt:

            sample_vol = r.target_amt / r.conc
            buffer_vol = r.target_vol - sample_vol
            tot_vol = sample_vol + buffer_vol

        # 3) Sample too concentrated -> Increase final volume if possible
        elif r.min_transfer_amt > r.target_amt:

            if volume_expansion:
                increased_vol = r.min_transfer_amt / r.target_conc
                assert (
                    increased_vol <= well_max_vol
                ), f"Sample {r.name} is too concentrated ({r.conc} ng/ul) and must be diluted manually"

                tot_vol = increased_vol
                sample_vol = zika_min_vol
                buffer_vol = tot_vol - sample_vol

            else:
                sample_vol = zika_min_vol
                tot_vol = r.target_vol
                buffer_vol = tot_vol - sample_vol

        final_amt = sample_vol * r.conc
        final_conc = final_amt / tot_vol

        # Flag sample in log if deviating by >= 1% from target
        amt_frac = final_amt / r.target_amt
        if abs(amt_frac - 1) >= 0.005:
            log.append(
                f"WARNING: Sample {r.sample_name} ({r.conc:.2f} ng/ul in {r.vol:.2f} ul accessible volume)"
            )
            log.append(f"\t--> Transferring {sample_vol:.2f} ul, resulting in {final_amt:.2f} ng in {tot_vol:.2f} ul ({final_conc:.2f} ng/ul)")
        else:
            log.append(f"Sample {r.sample_name} normalized to {final_amt:.2f} ng in {tot_vol:.2f} ul ({final_conc:.2f} ng/ul)")

        d["sample_vol"].append(sample_vol)
        d["buffer_vol"].append(buffer_vol)
        d["tot_vol"].append(tot_vol)

        # Change UDFs
        if not local_data:
            op = outputs[r.sample_name]
            op.udf['Amount taken (ng)'] = round(final_amt, 2)
            op.udf['Total Volume (uL)'] = round(tot_vol, 2)
            if final_amt < r.target_amt:
                op.udf['Target Amount (ng)'] = round(final_amt, 2)
            op.put()

    log.append("\nDone.\n")
    df = df.join(pd.DataFrame(d))

    # Resolve buffer transfers
    df = zika.resolve_buffer_transfers(df, buffer_strategy="column")

    # Format worklist
    df = zika.format_worklist(df, deck=deck, split_transfers=True)

    # Write files
    method_name = "setup_QIAseq"
    pid = "local" if local_data else currentStep.id
    wl_filename, log_filename = zika.get_filenames(method_name, pid)

    zika.write_worklist(
        df=df,
        deck=deck,
        wl_filename=wl_filename,
        comments=comments,
        multi_aspirate=multi_aspirate,
    )

    zika.write_log(log, log_filename)

    # Upload files
    if not local_data:
        zika.upload_csv(currentStep, lims, wl_filename)
        zika.upload_log(currentStep, lims, log_filename)

        # Issue warnings, if any
        if any("WARNING:" in entry for entry in log):
            sys.stderr.write(
                "CSV-file generated with warnings, please check the Log file\n"
            )
            sys.exit(2)

    return wl_filename, log_filename

