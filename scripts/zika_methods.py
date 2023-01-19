#!/usr/bin/env python

DESC = """
This module contains methods for normalization and pooling on the Mosquito Zika instrument.

Written by Alfred Kedhammar
"""

import zika
import pandas as pd
import sys
import numpy as np
import sys

def pool(
    currentStep=None, 
    lims=None, 
    zika_min_vol=0.5,  # 0.5 lowest validated, 0.1 lowest possible
    well_dead_vol=5,   # 5 ul generous estimate of dead volume in TwinTec96
    well_max_vol=180   # TwinTec96
    ):
    """
    Pool samples.
    """

    # Write log header
    log = []
    for e in [
        f"Minimum pipetting volume: {zika_min_vol} ul",
        f"Applied dead volume: {well_dead_vol} ul",
        f"Maximum allowed pool volume: {well_max_vol} ul"
    ]:
        log.append(e)

    # See zika.fetch_sample_data for which stats correspond to which attributes
    to_fetch = [
        # Sample info
        "sample_name",
        "conc",
        "conc_units",
        "vol",
        # Plates and positions
        "source_fc",
        "source_well",
        "dest_fc",
        "dest_well",
        "dest_fc_name",
        # Changes to src
        "amt_taken",
        "pool_vol_final",
        "target_name"
    ]

    df_all = zika.fetch_sample_data(currentStep, to_fetch, log)

    # Assertions
    assert all(df_all.vol > well_dead_vol), f"The minimum required source volume is {well_dead_vol} ul"
    df_all["full_vol"] = df_all.vol.copy()
    df_all.loc[:,"vol"] = df_all.vol - well_dead_vol

    # Define deck
    assert len(df_all.source_fc.unique()) <= 4, "Only one to four input plates allowed"
    assert len(df_all.dest_fc.unique()) == 1, "Only one output plate allowed"
    deck = {}
    deck[df_all.dest_fc.unique()[0]] = 3
    available = [2, 4, 1, 5][0:len(df_all.source_fc.unique())]
    for plate, pos in zip(df_all.source_fc.unique(), available):
        deck[plate] = pos

    # Work through the pools one at a time
    pools = [art for art in currentStep.all_outputs() if art.type == "Analyte"]
    pools.sort(key=lambda pool: pool.name)

    df_wl = pd.DataFrame()
    buffer_vols = {}
    for pool in pools:

        # Replace commas with semicolons, so pool names can be printed in worklist
        pool.name = pool.name.replace(",",";")

        # Subset data
        df_pool = df_all[df_all.target_name == pool.name].copy()

        # Find target parameters
        target_pool_vol = float(pool.udf["Final Volume (uL)"])
        try:
            target_pool_conc = float(pool.udf["Pool Conc. (nM)"])
            amt_unit = "fmol"
            conc_unit = "nM"
        except KeyError:
            amt_taken = float(pool.udf["Amount taken (ng)"])
            target_pool_conc = amt_taken * len(df_pool) / target_pool_vol
            amt_unit = "ng"
            conc_unit = "ng/ul"

        # Append objective to log
        log.append(f"Pooling {len(df_pool)} samples into {pool.name}...")
        log.append(f"Target conc: {round(target_pool_conc, 2)} {conc_unit}, Target vol: {target_pool_vol} ul")

        # Set any negative concentrations to 0.01 and flag in log
        if not df_pool.loc[df_pool.conc < 0.01, "conc"].empty:
            neg_conc_sample_names = df_pool.loc[df_pool.conc < 0.01, "name"].sort_values()
            df_pool.loc[df_pool.conc < 0.01, "conc"] = 0.01
            log.append(f"WARNING: The following {len(neg_conc_sample_names)} sample(s) fell short of, and will be treated as, \
                0.01 {conc_unit}: {', '.join(neg_conc_sample_names)}")

        # Determine lowest / highest common transfer amount
        df_pool["min_amount"] = zika_min_vol * df_pool.conc
        df_pool["max_amount"] = df_pool.vol * df_pool.conc
        highest_min_amount = max(df_pool.min_amount)
        lowest_max_amount = min(df_pool.max_amount)
            
        df_pool["minimized_vol"] = np.minimum(highest_min_amount / df_pool.conc, df_pool.vol)
        well_min_vol = sum(df_pool.minimized_vol)
        if well_min_vol > well_max_vol:
            log.append(f"ERROR: Overflow in {pool.name}. Decrease number of samples or dilute highly concentrated outliers")
            highest_conc_sample_name, highest_conc_sample_conc = df_pool.loc[df_pool.conc.idxmax,["name","conc"]]
            log.append(f"Highest concentrated sample: {highest_conc_sample_name} at {round(highest_conc_sample_conc,2)} {conc_unit}")
            log.append(f"Pooling cannot be normalized to less than {round(well_min_vol,2)} ul")
            raise AssertionError

        # Given our input samples, which volumes / concs. are possible as output?
        # Minimize amount
        pool_max_conc = highest_min_amount * len(df_pool) / well_min_vol
        pool_min_conc = highest_min_amount * len(df_pool) / well_max_vol

        # Log perfect pool or not
        if highest_min_amount > lowest_max_amount:
            log.append("WARNING: Some samples will be depleted and under-represented in the final pool.\
            \nThe common sample transfer amount is minimized in order to get all samples as equal as possible")
            # No room to maximize amount
            well_min_vol2 = well_min_vol
            pool_min_conc2 = pool_min_conc
        else:
            # Maximize amount
            well_min_vol2 = min(well_min_vol*lowest_max_amount/highest_min_amount, well_max_vol)
            pool_min_conc2 = pool_max_conc * well_min_vol2 / well_max_vol

            log.append(f"Pool can be created for conc {round(pool_min_conc,2)}-{round(pool_max_conc,2)} nM and vol {round(well_min_vol,2)}-{round(well_max_vol,2)} ul")
            
        # Pack all metrics into a list, to decrease number of input arguments later
        pool_boundaries = [well_min_vol, well_min_vol2, well_max_vol, pool_min_conc, pool_min_conc2, pool_max_conc]

        # Nudge conc, if necessary
        if target_pool_conc > pool_max_conc:
            pool_conc = pool_max_conc
        elif target_pool_conc < pool_min_conc:
            pool_conc = pool_min_conc
        else:
            pool_conc = target_pool_conc
        if target_pool_conc != pool_conc:
            log.append(f"WARNING: Target pool conc is adjusted to {round(pool_conc,2)} {conc_unit}")

        #  Nudge vol, if necessary
        min_vol_given_pool_conc, max_vol_given_pool_conc = zika.conc2vol(pool_conc, pool_boundaries)
        if target_pool_vol < min_vol_given_pool_conc:
            pool_vol = min_vol_given_pool_conc
            log.append(f"INFO: Target pool vol is adjusted to {round(pool_vol,2)} ul")
        elif target_pool_vol > min_vol_given_pool_conc and highest_min_amount > lowest_max_amount:
            pool_vol = min_vol_given_pool_conc
            log.append(f"WARNING: Target pool vol is adjusted to {round(pool_vol,2)} ul")
        elif target_pool_vol > max_vol_given_pool_conc:
            pool_vol = max_vol_given_pool_conc
            log.append(f"WARNING: Target pool vol is adjusted to {round(pool_vol,2)} ul")
        else:
            pool_vol = target_pool_vol

        if highest_min_amount < lowest_max_amount and target_pool_vol == pool_vol and target_pool_conc == pool_conc:
            log.append("Pooling OK")

        # Append transfer volumes and corresponding fraction of target conc. for each sample
        sample_transfer_amount = pool_conc * pool_vol / len(df_pool)
        df_pool["transfer_vol"] = np.minimum(sample_transfer_amount / df_pool.conc, df_pool.vol)
        df_pool["final_target_fraction"] = round((df_pool.transfer_vol * df_pool.conc / pool_vol) / (pool_conc / len(df_pool)), 2)

        # Calculate and store pool buffer volume
        total_sample_vol = sum(df_pool["transfer_vol"])
        if pool_vol - total_sample_vol > 0.5:
            buffer_vols[pool.name] = pool_vol - total_sample_vol

        # Report low-conc samples
        low_samples = df_pool[df_pool.final_target_fraction < 0.995][["sample_name", "final_target_fraction"]].sort_values("sample_name")
        if not low_samples.empty:
            log.append("The following samples are pooled below target:")
            log.append("Sample\tFraction")
            for name, frac in low_samples.values:
                log.append(f"{name}\t{round(frac,2)}")

        df_wl = pd.concat([df_wl, df_pool], axis=0)

    # Format worklist 
    df_formatted = zika.format_worklist(df_wl, deck)
    
    # Write files
    wl_filename, log_filename = zika.get_filenames(
        method_name="pool",
        pid=currentStep.id
    )

    # Comments to attach to the worklist header
    comments = [
        f"This worklist will enact pooling of {len(df_all)} samples",
        "For detailed parameters see the worklist log"
    ]
    for pool in pools:
        comments.append(f"Add {round(buffer_vols[pool.name],1)} ul buffer to pool {pool.name} (well {pool.location[1]})")
    zika.write_worklist(
        df=df_formatted,
        deck=deck,
        wl_filename=wl_filename,
        comments=comments,
    )

    zika.write_log(log, log_filename)

    # Upload files
    zika.upload_csv(currentStep, lims, wl_filename)
    zika.upload_log(currentStep, lims, log_filename)

    # Issue warnings, if any
    if any("WARNING:" in entry for entry in log):
        sys.stderr.write(
            "CSV-file generated with warnings, please check the Log file\n"
        )
        sys.exit(2)

    return wl_filename, log_filename

# ===========================================================================================================

def norm(
    currentStep=None, 
    lims=None, 
    local_data=None,                # Fetch sample data from local .tsv instead of LIMS
    buffer_strategy="first_column", # Use first column of buffer plate as reservoir
    volume_expansion=True,          # For samples that are too concentrated, increase target volume to obtain correct conc
    multi_aspirate=True,            # Use multi-aspiration to fit buffer and sample into the same transfer, if possible
    zika_min_vol=0.1,               # 0.5 lowest validated, 0.1 lowest possible
    well_dead_vol=5,                # 5 ul generous estimate of dead volume in TwinTec96
    well_max_vol=15                 # 15 ul max well vol enables single-column buffer reservoir
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

    # Write log header
    log = []
    for e in [
        f"Expand volume to obtain target conc: {volume_expansion}"
        f"Multi-aspirate buffer-sample: {multi_aspirate}"
        f"Minimum pipetting volume: {zika_min_vol} ul"
        f"Applied dead volume: {well_dead_vol} ul"
        f"Maximum allowed dst well volume: {well_max_vol} ul"
    ]:
        log.append(e)


    # See zika.fetch_sample_data for which stats correspond to which attributes
    to_fetch = [
        # Sample info
        "sample_name",
        "conc",
        "conc_units",
        "vol",
        # User sample info
        "user_conc",
        "user_vol",
        # Plates and positions
        "source_fc",
        "source_well",
        "dest_fc",
        "dest_well",
        "dest_fc_name",
        # Changes to src
        "amt_taken",
        "vol_taken",
        # Target info
        "target_amt",
        "target_vol"
    ]
    
    if local_data:
        df = zika.load_fake_samples(local_data, to_fetch)
    else:
        df = zika.fetch_sample_data(currentStep, to_fetch, log)

    # Assertions
    assert all(df.vol > well_dead_vol), f"The minimum required source volume is {well_dead_vol} ul"
    df["full_vol"] = df.vol.copy()
    df.loc[:,"vol"] = df.vol - well_dead_vol

    if "conc_units" in df.columns:
        assert all(df.conc_units == "ng/ul"), "All sample concentrations are expected in 'ng/ul'"
    assert all(df.target_amt > 0), "Target amount needs to be greater than zero"

    # Define deck
    assert len(df.source_fc.unique()) == 1, "Only one input plate allowed"
    assert len(df.dest_fc.unique()) == 1, "Only one output plate allowed"
    deck = {
        "buffer_plate": 2,
        df.source_fc.unique()[0]: 3,
        df.dest_fc.unique()[0]: 4,
    }

    # Make calculations
    df["target_conc"] = df.target_amt / df.target_vol
    df["min_transfer_amt"] = np.minimum(df.vol, zika_min_vol) * df.conc
    df["max_transfer_amt"] = np.minimum(df.vol, df.target_vol) * df.conc

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
    df = zika.resolve_buffer_transfers(df, buffer_strategy=buffer_strategy)

    # Format worklist
    df = zika.format_worklist(df, deck=deck, split_transfers=True)

    # Write files
    method_name = "norm"
    pid = "local" if local_data else currentStep.id
    wl_filename, log_filename = zika.get_filenames(method_name, pid)

    # Comments to attach to the worklist header
    comments = [
        f"This worklist will enact normalization of {len(df)} samples",
        "For detailed parameters see the worklist log"
    ]
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

