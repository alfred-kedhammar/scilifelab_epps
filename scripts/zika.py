#!/usr/bin/env python

DESC = """
This module contains functions used to generate worklists for the Mosquito X1
instrument Zika using sample data fetched from Illumina Clarity LIMS.

The functions are written with the intention of being re-useable for different
applications of the instrument.

Written by Alfred Kedhammar
"""

import pandas as pd
import numpy as np
from datetime import datetime as dt


def verify_step(currentStep, targets):
    """Verify the instrument, workflow and step for a given process is correct"""
    
    if currentStep.instrument.name != "Zika":
        return False

    sample_bools = []
    for art in currentStep.all_inputs():

        # For each sample, check whether there is any active process that matches any in the target list
        sample_bools.append(any([
            status == "IN_PROGRESS" and any(
                target_wf_prefix in wf.workflow.name and step == target_step 
                for target_wf_prefix, target_step in targets
                ) 
                for wf, status, step in art.workflow_stages_and_statuses
            ]))
        
    if all(sample_bools):
        return True
    else:
        return False
        

def fetch_sample_data(currentStep, to_fetch, log):
    """
    Within this function is the dictionary key2expr, its keys being the given name of a particular piece of information linked to a transfer input/output sample and it's values being the string that when evaluated will yield the desired info from whatever the variable "art_tuple" is currently pointing at. 
    Given the positional arguments of a LIMS transfer process and a list of keys, it will return a dataframe containing the information fetched from each transfer based on the keys.
    """

    key2expr = {
        # Sample info
        "sample_name":      "art_tuple[0]['uri'].name",
        # User sample info
        "user_conc":        "art_tuple[0]['uri'].samples[0].udf['Customer Conc']",
        "user_vol":         "art_tuple[0]['uri'].samples[0].udf['Customer Volume']",
        # RC sample info
        "conc_units":       "art_tuple[0]['uri'].samples[0].artifact.udf['Conc. Units']",
        "conc":             "art_tuple[0]['uri'].samples[0].artifact.udf['Concentration']",
        "vol":              "art_tuple[0]['uri'].samples[0].artifact.udf['Volume (ul)']",
        "amt":              "art_tuple[0]['uri'].samples[0].artifact.udf['Amount (ng)']",
        # Plates and wells
        "source_fc":        "art_tuple[0]['uri'].location[0].name",
        "source_well":      "art_tuple[0]['uri'].location[1]",
        "dest_fc_name":     "art_tuple[1]['uri'].location[0].name",
        "dest_fc":          "art_tuple[1]['uri'].location[0].id",
        "dest_well":        "art_tuple[1]['uri'].location[1]",
        # Target info: 
        "amt_taken":        "art_tuple[1]['uri'].udf['Amount taken (ng)']",           # The amount (ng) that is taken from the original sample plate
        "vol_taken":        "art_tuple[1]['uri'].udf['Total Volume (uL)']",           # The total volume of dilution
        "pool_vol_final":   "art_tuple[1]['uri'].udf['Final Volume (uL)']",           # Target pool volume
        "target_name":      "art_tuple[1]['uri'].name",                               # Target sample or pool name
        "target_amt":       "art_tuple[1]['uri'].udf['Target Amount (ng)']",          # The actual amount (ng) that is used as input for library prep
        "target_vol":       "art_tuple[1]['uri'].udf['Target Total Volume (uL)']"     # The actual total dilution volume that is used as input for library prep
    }

    replacement_stats = {
        # If target amt / vol is not stated, use synonymously with amt / vol taken
        "target_amt": "amt_taken",
        "target_vol": "vol_taken",
        # If conc / vol is missing, use the user-supplied values
        "conc": "user_conc",
        "vol": "user_vol"
    }

    # Verify all target metrics are keys in key2expr dict
    for k in to_fetch:
        assert k in key2expr.keys(), f"fetch_sample_data() is missing a definition for key {k}"

    # Fetch all input/output sample tuples
    art_tuples = [
        art_tuple for art_tuple in currentStep.input_output_maps
        if art_tuple[0]["uri"].type == art_tuple[1]["uri"].type == "Analyte"
    ]

    # Fetch all target data
    l = []
    replacements = []
    for art_tuple in art_tuples:
        key2val = {}
        for k in to_fetch:
            
            try:
                key2val[k] = eval(key2expr[k])
            # If a value is missing
            except KeyError:
                
                # try replacing it
                if k in replacement_stats:
                    key2val[k] = eval(key2expr[replacement_stats[k]])

                    missing_udf = key2expr[k].split("\'")[-2]
                    replacement_udf = key2expr[replacement_stats[k]].split("\'")[-2]
                    msg = f"'UDF {missing_udf}' not found, using '{replacement_udf}' instead"
                    if msg not in replacements:
                        replacements.append(msg)
                        log.append(msg)

                # or assume conc units are ng/ul
                elif k == "conc_units":
                    pass
                
                else:
                    raise

        l.append(key2val)

    # Compile to dataframe
    df = pd.DataFrame(l)

    log.append("\n")
    return df


def load_fake_samples(file, to_fetch):
    """This function is intended to output the same dataframe as fetch_sample_data(), but the input data is taken from a .csv-exported spreadsheet and is thus easier to change than data taken from upstream LIMS."""

    file_data = pd.read_csv(file, delimiter = "\t")

    assert all(
        [k in file_data.columns for k in to_fetch]
    ), "load_fake_samples() did not find all required columns"

    # Only retain specified columns
    df = file_data[to_fetch]

    return df


def format_worklist(df, deck, split_transfers = False):
    """
    - Add columns in Mosquito-intepretable format
    - Resolve multi-transfers
    - Sort by dest col, dest row, buffer, sample
    """

    # Add columns for plate positions
    df["src_pos"] = df["source_fc"].apply(lambda x: deck[x])
    df["dst_pos"] = df["dest_fc"].apply(lambda x: deck[x])

    # Convert volumes to whole nl
    df["transfer_vol"] = round(df.transfer_vol * 1000, 0)
    df["transfer_vol"] = df["transfer_vol"].astype(int)

    # Convert well names to r/c coordinates
    df["src_row"], df["src_col"] = well2rowcol(df.source_well)
    df["dst_row"], df["dst_col"] = well2rowcol(df.dest_well)

    # Sort df
    try:
        df.sort_values(by = ["dst_col", "dst_row", "src_type"], inplace = True)
    except KeyError:
        df.sort_values(by = ["dst_col", "dst_row"], inplace = True)

    if split_transfers:
        # Split >5000 nl transfers
        assert all(df.transfer_vol < 180000), "Some transfer volumes exceed 180 ul"
        max_vol = 5000
        df_split = pd.DataFrame(columns = df.columns)

        for idx, row in df.iterrows():

            if row.transfer_vol > max_vol:
                row_cp = row.copy()
                row_cp.loc["transfer_vol"] = max_vol

                while row.transfer_vol > max_vol:
                    df_split = df_split.append(row_cp)
                    row.loc["transfer_vol"] = row.transfer_vol - max_vol
                
            df_split = df_split.append(row)

        df_split.reset_index(inplace = True, drop = True)

        return df_split
    
    else:
        return df


def resolve_buffer_transfers(df, buffer_strategy):
    """
    Melt buffer and sample information onto separate rows to
    produce a "one row <-> one transfer" dataframe.
    """

    # Pivot buffer transfers
    df.rename(columns = {"sample_vol": "sample", "buffer_vol": "buffer"}, inplace = True)
    to_pivot = ["sample", "buffer"]
    to_keep = ["source_fc", "source_well", "dest_fc", "dest_well"]
    df = df.melt(
        value_vars=to_pivot,
        var_name="src_type",
        value_name="transfer_vol",
        id_vars=to_keep,
    )
    
    # Sort df
    split_dest_well = df.dest_well.str.split(":", expand = True)
    df["dest_well_row"] = split_dest_well[0]
    df["dest_well_col"] = split_dest_well[1]

    df.sort_values(by = ["dest_well_col", "dest_well_row", "src_type"], inplace = True)

    # Remove zero-vol transfers
    df = df[df.transfer_vol > 0]

    # Re-set index
    df = df.reset_index(drop=True)

    # Assign buffer transfers to buffer plate
    df.loc[df["src_type"] == "buffer", "source_fc"] = "buffer_plate"

    # Assign buffer source wells
    if buffer_strategy == "first_column":
        # Keep rows, but only use column 1
        df.loc[df["src_type"] == "buffer", "source_well"] = df.loc[
            df["src_type"] == "buffer", "source_well"
        ].apply(lambda x: x[0:-1] + "1")
    else:
        raise Exception("No buffer strategy defined")

    return df


def well2rowcol(well_iter):
    """
    Translates iterable of well names to list of row/column integer tuples to specify
    well location in Mosquito worklists.
    """

    # In an advanced worklist: startcol, endcol, row
    rows = []
    cols = []
    for well in well_iter:
        [row_letter, col_number] = str.split(well, sep=":")
        rowdict = {}
        for l, n in zip("ABCDEFGH", "12345678"):
            rowdict[l] = n
        rows.append(rowdict[row_letter])
        cols.append(col_number)
    return rows, cols


def conc2vol(conc, pool_boundaries):
    """
    Nudge target vol based on conc. and pool boundaries.
    """
    [pool_min_vol, pool_min_vol2, pool_max_vol, pool_min_conc, pool_min_conc2, pool_max_conc] = pool_boundaries
    assert pool_min_conc <= conc <= pool_max_conc

    min_vol = min(pool_max_vol, pool_min_vol * pool_max_conc / conc)
    max_vol = min(pool_max_vol, pool_min_vol2 * pool_max_conc / conc)
    return (min_vol, max_vol)


def get_filenames(method_name, pid):

    timestamp = dt.now().strftime("%y%m%d_%H%M%S")

    wl_filename = "_".join(["zika_worklist", method_name, pid, timestamp]) + ".csv"
    log_filename = "_".join(["zika_log", method_name, pid, timestamp]) + ".log"

    return wl_filename, log_filename


def write_worklist(df, deck, wl_filename, comments=None, multi_aspirate=False):
    """
    Write a Mosquito-interpretable advanced worklist.

    multi_aspirate -- If a buffer transfer is followed by a sample transfer
                      to the same well, and the sum of their volumes
                      is <= 5000 nl, use multi-aspiration.
    """

    # Format comments for printing into worklist
    if comments:
        comments = ["COMMENT, " + e for e in comments]

    # Default transfer type is simple copy
    df["transfer_type"] = "COPY"

    if multi_aspirate:
        filter = np.all(
            [
                # Use multi-aspirate IF...

                # End position of next transfer is the same
                df.dst_pos == df.shift(-1).dst_pos,
                # End well of the next transfer is the same
                df.dest_well == df.shift(-1).dest_well,
                # This transfer is buffer
                df.source_fc == "buffer_plate",
                # Next transfer is not buffer
                df.shift(-1).source_fc != "buffer_plate",
                # Sum of this and next transfer is <= 5 ul
                df.transfer_vol + df.shift(-1).transfer_vol <= 5000,
            ],
            axis=0,
        )
        df.loc[filter, "transfer_type"] = "MULTI_ASPIRATE"

    # PRECAUTION Keep tip change strategy in a single dict to avoid mix-ups
    tip_strats = {
        "always": ("[VAR1]", "TipChangeStrategy,always"),
        "never": ("[VAR2]", "TipChangeStrategy,never"),
    }

    # Convert all data to strings
    for c in df:
        df.loc[:, c] = df[c].apply(str)

    # Write worklist
    with open(wl_filename, "w") as wl:

        wl.write("worklist,\n")

        # Conditionals for worklist variables can be added here as needed
        wl.write("".join(tip_strats["always"]) + "\n")

        # Write header
        wl.write(f"COMMENT, This is the worklist {wl_filename}\n")
        if comments:
            for line in comments:
                wl.write(line + "\n")
        wl.write(get_deck_comment(deck))

        # Write transfers
        for i, r in df.iterrows():
            if r.transfer_type == "COPY":
                wl.write(
                    ",".join(
                        [
                            r.transfer_type,
                            r.src_pos,
                            r.src_col,
                            r.src_col,
                            r.src_row,
                            r.dst_pos,
                            r.dst_col,
                            r.dst_row,
                            r.transfer_vol,
                            tip_strats["always"][0],
                        ]
                    )
                    + "\n"
                )
            elif r.transfer_type == "MULTI_ASPIRATE":
                wl.write(
                    ",".join(
                        [
                            r.transfer_type,
                            r.src_pos,
                            r.src_col,
                            r.src_row,
                            "1",
                            r.transfer_vol,
                        ]
                    )
                    + "\n"
                )
            else:
                raise AssertionError("No transfer type defined")
        
        wl.write(f"COMMENT, Done")


def get_deck_comment(deck):
    """ Convert the plate:position 'decktionary' into a worklist comment
    """

    pos2plate = dict([(pos, plate) for plate, pos in deck.items()])

    l = [pos2plate[i].replace(",", "") if i in pos2plate else "[Empty]" for i in range(1, 6)]

    deck_comment = "COMMENT, Set up layout:    " + "     ".join(l) + "\n"

    return deck_comment

def write_log(log, log_filename):
    with open(log_filename, "w") as logContext:
        logContext.write("\n".join(log))

def upload_log(currentStep, lims, log_filename):
    for out in currentStep.all_outputs():
        if out.name == "Mosquito Log":
            for f in out.files:
                lims.request_session.delete(f.uri)
            lims.upload_new_file(out, log_filename)


def upload_csv(currentStep, lims, wl_filename):
    for out in currentStep.all_outputs():
        if out.name == "Mosquito CSV File":
            for f in out.files:
                lims.request_session.delete(f.uri)
            lims.upload_new_file(out, wl_filename)

