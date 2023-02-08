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
import sys


def verify_step(currentStep, targets):
    """Verify the instrument, workflow and step for a given process is correct"""

    # TODO if workflow or step is left blank, pass all
    
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


class CheckLog(Exception):

    def __init__(self, log, log_filename, lims, currentStep):

        write_log(log, log_filename)
        upload_log(currentStep, lims, log_filename)
        
        sys.stderr.write("ERROR: Check log for more info.")
        sys.exit(2)
        

def fetch_sample_data(currentStep, to_fetch):
    """ Given a dictionary "to_fetch" whose keys are the desired sample properties and whose values are the
    corresponding object paths, fetch the sample properties for all elements of currentStep.input_output_maps
    and return them in a dataframe.
    """

    object_paths = [
        # Current step input artifact
        "art_tuple[0]['uri'].name",                                 # Sample name
        "art_tuple[0]['uri'].id",                                   # Sample ID
        "art_tuple[0]['uri'].location[0].name",                     # Plate name
        "art_tuple[0]['uri'].location[0].id",                       # Plate ID
        "art_tuple[0]['uri'].location[1]",                          # Well
        "art_tuple[0]['uri'].udf['Conc. Units']",
        "art_tuple[0]['uri'].udf['Concentration']",
        "art_tuple[0]['uri'].udf['Volume (ul)']",
        "art_tuple[0]['uri'].udf['Amount (ng)']",

        # Current step output artifact
        "art_tuple[1]['uri'].udf['Amount taken (ng)']",             # The amount (ng) that is taken from the original sample plate
        "art_tuple[1]['uri'].udf['Total Volume (uL)']",             # The total volume of dilution
        "art_tuple[1]['uri'].udf['Final Volume (uL)']",             # Final pool / sample volume
        "art_tuple[1]['uri'].name", 
        "art_tuple[1]['uri'].id",
        "art_tuple[1]['uri'].udf['Target Amount (ng)']",            # In methods where the prep input is possibly different from the sample dilution, this is the target concentration and minimum volume of the prep input
        "art_tuple[1]['uri'].udf['Target Total Volume (uL)']",      # In methods where the prep input is possibly different from the sample dilution, this is the target concentration and minimum volume of the prep input
        "art_tuple[1]['uri'].location[0].name",                     # Plate name
        "art_tuple[1]['uri'].location[0].id",                       # Plate ID
        "art_tuple[1]['uri'].location[1]",                          # Well
       
        # Input sample info
        "art_tuple[0]['uri'].samples[0].name",
        "art_tuple[0]['uri'].samples[0].udf['Customer Conc']",      # ng/ul
        "art_tuple[0]['uri'].samples[0].udf['Customer Volume']",
        
        # Input sample RC measurements (?)
        "art_tuple[0]['uri'].samples[0].artifact.udf['Conc. Units']",
        "art_tuple[0]['uri'].samples[0].artifact.udf['Concentration']",
        "art_tuple[0]['uri'].samples[0].artifact.udf['Volume (ul)']",
        "art_tuple[0]['uri'].samples[0].artifact.udf['Amount (ng)']"
    ]

    # Verify all target metrics are found in object_paths, if not - add them
    for header, object_path in to_fetch.items():
        assert object_path in object_paths, f"fetch_sample_data() is missing the requested object path {object_path}"

    # Fetch all input/output sample tuples
    art_tuples = [
        art_tuple for art_tuple in currentStep.input_output_maps
        if art_tuple[0]["uri"].type == art_tuple[1]["uri"].type == "Analyte"
    ]

    # Fetch all target data
    list_of_dicts = []
    for art_tuple in art_tuples:
        dict = {}
        for header, object_path in to_fetch.items():
            dict[header] = eval(object_path)
        list_of_dicts.append(dict)

    # Compile to dataframe
    df = pd.DataFrame(list_of_dicts)

    return df


def load_fake_samples(file, to_fetch):
    """ This function is intended to output the same dataframe as fetch_sample_data(),
    but the input data is taken from a .csv-exported spreadsheet and is thus easier
    to change than data taken from upstream LIMS.
    """

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
    - Sort by dst col, dst row, buffer, sample
    """

    # Add columns for plate positions
    df["src_pos"] = df["src_name"].apply(lambda x: deck[x])
    df["dst_pos"] = df["dst_name"].apply(lambda x: deck[x])

    # Convert volumes to whole nl
    df["transfer_vol"] = round(df.transfer_vol * 1000, 0)
    df["transfer_vol"] = df["transfer_vol"].astype(int)

    # Convert well names to r/c coordinates
    df["src_row"], df["src_col"] = well2rowcol(df.src_well)
    df["dst_row"], df["dst_col"] = well2rowcol(df.dst_well)

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

class VolumeOverflow(Exception):
    pass

def resolve_buffer_transfers(df, buffer_strategy):
    """
    Melt buffer and sample information onto separate rows to
    produce a "one row <-> one transfer" dataframe.
    """

    # Pivot buffer transfers
    df.rename(columns = {"sample_vol": "sample", "buffer_vol": "buffer"}, inplace = True)
    to_pivot = ["sample", "buffer"]
    to_keep = ["src_name", "src_well", "dst_name", "dst_well"]
    df = df.melt(
        value_vars=to_pivot,
        var_name="src_type",
        value_name="transfer_vol",
        id_vars=to_keep,
    )
    
    # Sort df
    split_dst_well = df.dst_well.str.split(":", expand = True)
    df["dst_well_row"] = split_dst_well[0]
    df["dst_well_col"] = split_dst_well[1]

    df.sort_values(by = ["dst_well_col", "dst_well_row", "src_type"], inplace = True)

    # Remove zero-vol transfers
    df = df[df.transfer_vol > 0]

    # Re-set index
    df = df.reset_index(drop=True)

    # Assign buffer transfers to buffer plate
    df.loc[df["src_type"] == "buffer", "src_name"] = "buffer_plate"

    # Assign buffer src wells
    if buffer_strategy == "first_column":
        # Keep rows, but only use column 1
        df.loc[df["src_type"] == "buffer", "src_well"] = df.loc[
            df["src_type"] == "buffer", "src_well"
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
    
    TODO possible to avoid tip change between buffer transfers to clean dst well
    """

    # Replace all commas with semi-colons, so they can be printed without truncating the worklist
    for c, is_string in zip(df.columns, df.applymap(type).eq(str).all()):
        if is_string:
            df[c] = df[c].apply(lambda x: x.replace(",",";"))

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
                df.dst_well == df.shift(-1).dst_well,
                # This transfer is buffer
                df.src_name == "buffer_plate",
                # Next transfer is not buffer
                df.shift(-1).src_name != "buffer_plate",
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

