#!/usr/bin/env python

DESC = """
This module contains functions used to generate worklists for the Mosquito X1
instrument Zika using sample data fetched from Illumina Clarity LIMS.

Each function corresponds to a single method / application and is tied to a
specific workflow step.

Written by Alfred Kedhammar
"""

import zika
import pandas as pd
import sys


def setup_QIAseq(currentStep, lims):

    # Create dataframe
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
    df = zika.fetch_sample_data(currentStep, to_fetch)
    assert all(
        df.conc_units == "ng/ul"
    ), "All sample concentrations are expected in 'ng/ul'"

    # Define constraints
    min_zika_vol = 0.1
    max_final_vol = 15

    # Make calculations
    df["target_conc"] = df.target_amt / df.target_vol
    df["min_transfer_amt"] = min_zika_vol * df.conc
    df["max_transfer_amt"] = df.target_vol * df.conc

    # Define deck
    assert len(df.source_fc.unique()) == 1, "Only one input plate allowed"
    assert len(df.dest_fc.unique()) == 1, "Only one output plate allowed"
    deck = {
        "buffer_plate": 2,
        df.source_fc.unique()[0]: 3,
        df.dest_fc.unique()[0]: 4,
    }

    # Cases 1) - 3)
    d = {"sample": [], "buffer": [], "tot_vol": []}
    log = []
    for i, r in df.iterrows():

        # 1) Sample too dilute
        if r.max_transfer_amt < r.target_amt:

            sample_vol = min(r.target_vol, r.vol)
            tot_vol = r.target_vol
            buffer_vol = tot_vol - sample_vol

            target_pc = round((sample_vol * r.conc / tot_vol) / r.target_conc * 100, 2)
            log.append(
                f"WARNING: Insufficient amount of sample {r.sample_name} (conc {r.conc} ng/ul, vol {r.vol} ul)"
            )
            log.append(f"\t--> Reaching {target_pc}% of target concentration")

            # TODO change udf accordingly

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

            log.append(
                f"WARNING: High concentration of sample {r.sample_name} (conc {r.conc} ng/ul, vol {r.vol} ul)"
            )
            log.append(f"\t--> Adjusting total volume to {tot_vol} ul")

            # TODO change udf accordingly

        d["sample"].append(sample_vol)
        d["buffer"].append(buffer_vol)
        d["tot_vol"].append(tot_vol)

    df = df.join(pd.DataFrame(d))

    # Generate Mosquito-readable columns
    df = zika.format_worklist(df, deck=deck, buffer_strategy="column")

    # Comments to attach to the worklist header
    n_samples = len(df[df.src_type == "sample"])
    comments = [f"This worklist will enact normalization of {n_samples} samples"]

    # Write files and upload
    method_name = "setup_QIAseq"
    wl_filename, log_filename = zika.get_filenames(method_name, currentStep.id)

    zika.write_worklist(
        df=df,
        deck=deck,
        wl_filename=wl_filename,
        comments=comments,
        strategy="multi-aspirate",
    )

    zika.upload_log(currentStep, lims, log, log_filename)
    zika.upload_csv(currentStep, lims, wl_filename)

    # Issue warnings, if any
    if any("WARNING:" in entry for entry in log):
        sys.stderr.write(
            "CSV-file generated with warnings, please check the Log file\n"
        )
        sys.exit(2)
