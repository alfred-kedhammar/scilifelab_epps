#!/usr/bin/env python

DESC = """
This module contains methods for normalization and pooling on the Mosquito Zika instrument.

Written by Alfred Kedhammar
"""

from scripts import zika
import pandas as pd
import sys
import numpy as np
import sys


def pool(
    data=None,                  # Output of bravo_csv.make_datastructure()
    currentStep=None, 
    lims=None, 
    zika_min_vol=0.5,           # 0.5 lowest validated, 0.1 lowest possible
    well_dead_vol=5,            # 5 ul generous estimate of dead volume in TwinTec96
    well_max_vol=180,           # TwinTec96
    ):
    """
    Pool samples. 
    
    Input UDFs:
    - The target amount taken per sample (ng) OR target pool concentration (nM)
    - The target final pool volume (ul)

    Calculations:
    1) The inputs are translated to the desired pool concentration and volume.
    2) The desired pool concentration and volume is translated to the target transfer amount for each sample.
    3) The minimum and maximum transferrable amounts are calculated for each sample based on the Zika minimum transfer volume and the accessible sample volume, respectively
    
    Cases:
    - If the minimum transfer amount of the most highly concentrated sample is above target
        --> Expand the amount taken of all samples and the total volume to maintain the initially specified pool concentration
        --> If this causes volume overflow, the worklist will not be generated

    - If a sample has very low or negligible concentration
        --> Set concentration to 0.01
        --> # TODO use everything

    - If a sample does not have enough accessible volume to reach the target representation in the pool
        --> Let it be under-represented
    """

    method_name = "pool"
    pid = currentStep.id

    # Write log header
    log = []
    for e in [
        f"Minimum pipetting volume: {zika_min_vol} ul",
        f"Applied dead volume: {well_dead_vol} ul",
        f"Maximum allowed pool volume: {well_max_vol} ul"
    ]:
        log.append(e)

    pools = [art for art in currentStep.all_outputs() if art.type == "Analyte"]
    pools.sort(key=lambda pool: pool.name)
    output_udfs = [kv_pair[0] for kv_pair in pools[0].udf.items()]

    # Supplement df with additional info
    to_fetch = [
        # Sample info
        "sample_name",
        "conc_units",
        # Plates and positions
        "dst_fc_name",
        # Target info
        "target_name"
    ]
    
    df_fetched = zika.fetch_sample_data(currentStep, to_fetch, log)

    df_all = pd.DataFrame(data).merge(df_fetched, left_on="name", right_on="sample_name")

    # All samples should have accessible volume
    assert all(df_all.vol > well_dead_vol), f"The minimum required source volume is {well_dead_vol} ul"

    # Adjust for dead volume
    df_all["full_vol"] = df_all.vol.copy()
    df_all.loc[:,"vol"] = df_all.vol - well_dead_vol

    # Define deck, a dictionary mapping plate names to deck positions
    assert len(df_all.src_fc_id.unique()) <= 4, "Only one to four input plates allowed"
    assert len(df_all.dst_fc.unique()) == 1, "Only one output plate allowed"
    deck = {}
    deck[df_all.dst_fc.unique()[0]] = 3
    available = [2, 4, 1, 5][0:len(df_all.src_fc.unique())]
    for plate, pos in zip(df_all.src_fc.unique(), available):
        deck[plate] = pos

    # Work through the pools one at a time
    df_wl = pd.DataFrame()
    buffer_vols = {}
    errors = False
    for pool in pools:
        
        # === PREPARE CALCULATION INPUTS ===

        # Replace commas with semicolons, so pool names can be printed in the .csv worklist
        pool.name = pool.name.replace(",",";")

        # Subset data to current pool
        df_pool = df_all[df_all.target_name == pool.name].copy()

        # Find target parameters, amount and conentration will be either in ng and ng/ul or fmol and nM
        target_pool_vol = pool.udf["Final Volume (uL)"]
        if "Pool Conc. (nM)" in output_udfs:
            target_pool_conc = float(pool.udf["Pool Conc. (nM)"])
            target_amt_taken = target_pool_conc * target_pool_vol / len(df_pool)
            amt_unit = "fmol"
            conc_unit = "nM"
        elif 'Amount taken (ng)' in output_udfs:
            target_amt_taken = pool.udf['Amount taken (ng)']
            target_pool_conc = target_amt_taken * len(df_pool) / target_pool_vol
            amt_unit = "ng"
            conc_unit = "ng/ul"
        assert all(df_all.conc_units == conc_unit), "Samples and pools have different conc units"

        # All pools should have UDFs within the allowed range
        assert 0 < target_pool_vol <= well_max_vol, f"The target pool volume must be >0 - {well_max_vol} ul"
        assert target_amt_taken > 0, f"The target concentratinon of the pool must be >0"

        # Append objective to log
        log.append(f"\nPooling {len(df_pool)} samples into {pool.name}...")
        log.append(f"Target conc: {round(target_pool_conc, 2)} {conc_unit} ({target_amt_taken} {amt_unit} per sample), Target vol: {target_pool_vol} ul")

        # Set any negative or negligible concentrations to 0.01 and flag in log
        if not df_pool.loc[df_pool.conc < 0.01, "conc"].empty:
            neg_conc_sample_names = df_pool.loc[df_pool.conc < 0.01, "name"].sort_values()
            df_pool.loc[df_pool.conc < 0.01, "conc"] = 0.01
            log.append(f"WARNING: The following {len(neg_conc_sample_names)} sample(s) fell short of, and will be treated as, \
                0.01 {conc_unit}: {', '.join(neg_conc_sample_names)}")

        # === CALCULATE SAMPLE RANGES ===

        # Calculate the range of transferrable amount for each sample
        df_pool["min_amount"] = zika_min_vol * df_pool.conc
        df_pool["max_amount"] = df_pool.vol * df_pool.conc

        # === CALCULATE POSSIBLE OUTCOMES AND MAKE ADJUSTMENTS ===

        # Isolate highest concentrated sample
        highest_conc_sample = df_pool.sort_values(by = "conc", ascending = False).iloc[0]

        # Given the input samples, can an even pool be produced? I.e. is there an overlap in the transfer amount ranges of all samples?
        even_pool_is_possible = max(df_pool.min_amount) < min(df_pool.max_amount)

        if even_pool_is_possible:
            # The sample volumes can be expanded (to some extent) without changing the even-ness of the pool

            lowest_common_amount = max(df_pool.min_amount)
            highest_common_amount = min(df_pool.max_amount)
            
            # Calculate pool limits given samples
            pool_min_amt = lowest_common_amount  * len(df_pool)
            pool_min_sample_vol = sum(lowest_common_amount / df_pool.conc)
            pool_min_conc = pool_min_amt / well_max_vol
            pool_max_conc = pool_min_amt / pool_min_sample_vol # also equals pool_max_amt / pool_max_sample_vol because amt / vol proportions are the same between samples
            
            # Ensure that pool will not overflow
            if pool_min_sample_vol > well_max_vol:
                
                log.append(f"ERROR: Overflow in {pool.name}. Decrease number of samples or dilute highly concentrated outliers")
                log.append(f"Highest concentrated sample: {highest_conc_sample.sample_name} at {round(highest_conc_sample.conc,2)} {conc_unit}")
                log.append(f"Pooling cannot be normalized to less than {round(pool_min_sample_vol,2)} ul")

                errors = True

            log.append(f"Pool can be created for conc {round(pool_min_conc,2)}-{round(pool_max_conc,2)} {conc_unit} and vol {round(pool_min_sample_vol,2)}-{round(well_max_vol,2)} ul")

            # Nudge conc, if necessary
            if target_pool_conc > pool_max_conc:
                # Pool conc. has to be decreased from target to the maximum possible, given samples
                pool_conc = pool_max_conc
            elif target_pool_conc < pool_min_conc:
                # Pool conc. has to be increased from target to the minimum possible, given samples
                pool_conc = pool_min_conc
            else:
                # Pool conc. can be set to target
                pool_conc = target_pool_conc

            # Nudge vol, if necessary
            pool_min_vol_given_conc = min(pool_min_amt / pool_conc, well_max_vol)
            pool_max_vol_given_conc = min(highest_common_amount * len(df_pool) / pool_conc, well_max_vol)
            if target_pool_vol < pool_min_vol_given_conc:
                # Pool vol has to be increased from target to minimum possible, given samples
                pool_vol = pool_min_vol_given_conc
            elif target_pool_vol > pool_max_vol_given_conc:
                # Pool vol has to be decreased from target to maximum possible, given samples
                pool_vol =  pool_max_vol_given_conc
            else:
                # Pool vol can be set to target
                pool_vol = target_pool_vol

        else:
            # There is no common transfer amount, and sample volumes can NOT be expanded without changing the even-ness of the pool
            log.append(f"WARNING: Some samples will be depleted and under-represented in the final pool.\
            \nThe miminum transfer amount of the highest concentrated sample {highest_conc_sample.sample_name} ({highest_conc_sample.conc} {highest_conc_sample.conc_units}) will dictate the common transfer amount.")

            # Use the minimum transfer amount of the most concentrated sample as the common transfer amount
            target_transfer_amount = max(df_pool.min_amount)

            # Calculate pool limits...
            # --> Assuming all samples can meet the target common amount
            pool_flawed_min_amt = target_transfer_amount * len(df_pool)
            pool_flawed_min_sample_vol = sum(target_transfer_amount / df_pool.conc)
            pool_flawed_max_conc = pool_flawed_min_amt / pool_flawed_min_sample_vol
            pool_flawed_min_conc = pool_flawed_min_amt / well_max_vol

            # --> Taking into account sample depletion
            pool_real_min_amt = sum(np.minimum(target_transfer_amount, df_pool.max_amount))
            pool_real_min_sample_vol = sum(np.minimum(target_transfer_amount / df_pool.conc, df_pool.vol))
            pool_real_max_conc = pool_real_min_amt / pool_flawed_min_sample_vol
            pool_real_min_conc = pool_real_min_amt / well_max_vol

            # Ensure that pool will not overflow
            if pool_flawed_min_sample_vol > well_max_vol:
                
                log.append(f"ERROR: Overflow in {pool.name}. Decrease number of samples or dilute highly concentrated outliers")
                log.append(f"Highest concentrated sample: {highest_conc_sample.sample_name} at {round(highest_conc_sample.conc,2)} {conc_unit}")
                log.append(f"Pooling cannot be normalized to less than {round(pool_min_sample_vol,2)} ul")

                errors = True
    
            log.append(f"Can aim to create a pool as even as possible for target conc {round(pool_flawed_min_conc,2)}-{round(pool_flawed_max_conc,2)} {conc_unit} and vol {round(pool_flawed_min_sample_vol,2)}-{round(well_max_vol,2)} ul")
            log.append(f"WARNING: Due to sample depletion, the 'real' concentration of the pool will likely be {round(pool_real_min_conc,2)}-{round(pool_real_max_conc,2)} {conc_unit}")
            
            # Nudge conc, if necessary
            # Use the flawed target parameters for comparison and ignore sample depletion
            if target_pool_conc > pool_flawed_max_conc:
                pool_conc = pool_max_conc
            elif target_pool_conc < pool_flawed_min_conc:
                pool_conc = pool_min_conc
            else:
                pool_conc = target_pool_conc

            # No volume expansion is allowed, so pool volume is set to the minimum, given the conc
            pool_min_vol_given_conc = min(pool_flawed_min_amt / pool_conc, well_max_vol)
            pool_vol = pool_min_vol_given_conc

        # === STORE FINAL CALCULATION RESULTS ===

        # Append transfer volumes and corresponding fraction of target conc. for each sample
        sample_transfer_amount = pool_conc * pool_vol / len(df_pool)
        df_pool["transfer_vol"] = np.minimum(sample_transfer_amount / df_pool.conc, df_pool.vol)
        df_pool["transfer_amt"] = df_pool.transfer_vol * df_pool.conc
        df_pool["final_conc_fraction"] = round((df_pool.transfer_vol * df_pool.conc / pool_vol) / (pool_conc / len(df_pool)), 2)
        try:
            df_pool["final_amt_fraction"] = round(df_pool.transfer_amt / df_pool.amt_taken, 2)
        except:
            pass

        # Report adjustments in log
        if not zika.approx(target_pool_conc, pool_conc):
            log.append(f"WARNING: Target pool conc is adjusted to {round(pool_conc,2)} {conc_unit}")
            # TODO add per-sample information
        if not zika.approx(target_pool_vol, pool_vol):
            log.append(f"WARNING: Target pool vol is adjusted to {round(pool_vol,2)} ul")
        if zika.approx(target_pool_conc, pool_conc) and zika.approx(target_pool_vol, pool_vol):
            log.append("Pooling OK")
        if amt_unit == "ng":           
            amt_taken = df_pool["transfer_amt"].unique()[0] if even_pool_is_possible else target_transfer_amount
            if not zika.approx(amt_taken, target_amt_taken):
                log.append(f"INFO: Amount taken per sample is adjusted from {target_amt_taken} {amt_unit} to {round(amt_taken,2)} {amt_unit}")

        # Update UDFs TODO double check calcs and differentiate even vs uneven pools
        pool.udf["Final Volume (uL)"] = float(round(pool_vol,2))
        if amt_unit == "fmol":
            pool.udf["Pool Conc. (nM)"] = float(round(pool_conc,2))
        elif amt_unit == "ng":
            if even_pool_is_possible:
                pool.udf["Amount taken (ng)"] = float(round(df_pool["transfer_amt"].unique()[0], 2))
            else:
                pool.udf["Amount taken (ng)"] = float(round(target_transfer_amount, 2))
        pool.put()

        # Calculate and store pool buffer volume
        total_sample_vol = sum(df_pool["transfer_vol"])
        buffer_vol = pool_vol - total_sample_vol if pool_vol - total_sample_vol > 0.5 else 0
        buffer_vols[pool.name] = buffer_vol
        log.append(f"The final pool volume is {round(pool_vol,2)} ul ({round(total_sample_vol,2)} ul sample + {round(buffer_vol,2)} ul buffer)")         

        # === REPORT DEVIATING SAMPLES ===

        # Report deviating conc samples
        outlier_conc_samples = df_pool[np.logical_or(df_pool.final_conc_fraction < 0.995, df_pool.final_conc_fraction > 1.005)]\
            [["sample_name", "final_conc_fraction"]].sort_values("sample_name")
        if not outlier_conc_samples.empty:
            log.append("\nThe following samples deviate from the target concentration:")
            log.append("Sample\tFraction")
            for name, frac in outlier_conc_samples.values:
                log.append(f"{name}\t{round(frac,2)}")
        try:
            # Report deviating amt samples
            outlier_amt_samples = df_pool[np.logical_or(df_pool.final_amt_fraction < 0.995, df_pool.final_amt_fraction > 1.005)]\
                [["sample_name", "final_amt_fraction"]].sort_values("sample_name")
            if not outlier_amt_samples.empty:
                log.append("\nThe following samples deviate from the target amount:")
                log.append("Sample\tFraction")
                for name, frac in outlier_amt_samples.values:
                    log.append(f"{name}\t{round(frac,2)}")
            log.append("\n")
        except:
            pass

        df_wl = pd.concat([df_wl, df_pool], axis=0)

    if errors:
        raise zika.CheckLog(log, method_name, pid, lims, currentStep)

    # Format worklist 
    df_formatted = zika.format_worklist(df_wl, deck)
    
    # Write files
    wl_filename, log_filename = zika.get_filenames(method_name, pid)

    # Comments to attach to the worklist header
    comments = [
        f"This worklist will enact pooling of {len(df_all)} samples",
        "For detailed parameters see the worklist log"
    ]
    for pool in pools:
        if pool.name in buffer_vols.keys():
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
            op.udf['Amount taken (ng)'] = float(round(final_amt, 2))
            op.udf['Total Volume (uL)'] = float(round(tot_vol, 2))
            if final_amt < r.target_amt:
                op.udf['Target Amount (ng)'] = float(round(final_amt, 2))
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

