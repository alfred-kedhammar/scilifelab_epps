#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from datetime import datetime as dt
import pandas as pd
import re
import shutil
import os
import sys

DESC = """EPP used to generate MinKNOW samplesheets"""


def main(lims, args):
    """
    === Sample sheet columns ===

    flow_cell_id                -
    position_id                 [1-3A-G] for PromethION, else None
    sample_id                   For single samples: e.g. P12345_101, For pools: e.g. P12345_lims-pool-id
    experiment_id               lims-step_yymmdd_hhmmss_nickname
    flow_cell_product_code      -
    kit                         Product codes separated by spaces
    alias                       Only included for barcoded pools, sample name e.g. P12345_101
    barcode                     barcode01, barcode02, etc, excavated from LIMS

    === Constraints ===

    Must be the same across sheet:
    - kit
    - flow_cell_product_code
    - experiment_id

    Must be unique within sheet:
    - flow_cell_id
    - position_id
    - sample_id TODO
    
    Must be unique within the same flowcell
    - alias TODO
    - barcode TODO

    === Flowcell product codes ===

    FLO-PRO002 (PromethION R9.4.1)
    FLO-MIN106D (MinION R9.4.1)
    FLO-FLG001 (Flongle R9.4.1)
    FLO-PRO114M (PromethION R10.4.1)
    FLO-MIN114 (MinION R10.4.1)
    FLO-FLG114 (Flongle R10.4.1)

    === Outputs ===

    Samplesheet                 ONT_samplesheet_lims-step_yymmdd_hhmmss.csv
    """
    try:

        currentStep = Process(lims, id=args.pid)

        arts = [art for art in currentStep.all_outputs() \
            if art.type == "Analyte"]

        rows = []
        for art in arts:

            row = {
                "flow_cell_id": art.udf.get("ONT flow cell ID"),
                "position_id": art.udf.get("ONT flow cell position"),
                "sample_id": get_minknow_sample_id(art),
                "experiment_id": f"{currentStep.id}_{dt.now().strftime('%y%m%d_%H%M%S')}",
                "flow_cell_product_code": art.udf["ONT flow cell type"].split(" ")[0],
                "flow_cell_type": art.udf["ONT flow cell type"].split(" ")[1],
                "kit": get_kit_string(art)
            }

            if "PromethION" in row["flow_cell_type"]:
                assert row["position_id"] != "None", "Positions must be specified for PromethION flow cells."

            # Add extra column for positions
            if art.udf.get("ONT flow cell position") != "None":
                row["position_id"] = art.udf.get("ONT flow cell position")
            
            # Add extra columns for barcodes
            if art.udf.get('ONT expansion kit') != "None":
                assert len(art.reagent_labels) > 0, f"No barcodes found within pool {art.name}"
                for sample, label in zip(art.samples, art.reagent_labels):
                    row["alias"] = strip_characters(sample.name)
                    row["barcode"] = strip_characters("barcode" + label[0:2])   # TODO double check extraction of barcode number
                    rows.append(row.copy())

            assert "" not in row.values(), "All fields must be populated."

        df = pd.DataFrame(rows)

        if len(df) > 1:
            assert "PromethION" in df.flow_cell_type.unique()[0], "Only PromethION flowcells can be grouped together in the same sample sheet."
            assert len(df) <= 24, "Only up to 24 PromethION flowcells may be started at once."      
        assert len(df.flow_cell_product_code.unique()) == len(df.kit.unique()) == 1, "All rows must have the same flow cell type and kits"
        assert len(df.position_id.unique()) == len(df.flow_cell_id.unique()) == len(arts), "All rows must have different flow cell positions and IDs"

        file_name = write_csv(df)
        upload_file(file_name, currentStep, lims)

    except AssertionError as e:
        sys.stderr.write(str(e))
        sys.exit(2)


def upload_file(file_name, currentStep, lims):
    for out in currentStep.all_outputs():
        if out.name == "ONT sample sheet":
            for f in out.files:
                lims.request_session.delete(f.uri)
            lims.upload_new_file(out, file_name)


def write_csv(df):

    file_name = f"ONT_samplesheet_{df.experiment_id.unique()[0]}.csv"

    columns = [
        "flow_cell_id",
        "position_id",
        "sample_id",
        "experiment_id",
        "flow_cell_product_code",
        "kit"
    ]

    if "alias" in df.columns and "barcode" in df.columns:
        columns.append("alias")
        columns.append("barcode")
    
    df_csv = df.loc[:, columns]

    df_csv.to_csv(file_name, index = False)

    return file_name


def get_minknow_sample_id(art):
    """
    Assigns a MinKNOW sample ID based on the nature of the input artifact.
    Single samples, single-project pools and multi-project pools are treated differently.

    Type                    Contains                ID          Returns MinKNOW sample ID

    Sample sample           PAAAAA_101              12-345678   PAAAAA_101
    Single project pool     PAAAAA_101, PAAAAA_102  23-456789   PAAAAA_23-456789
    Multi project pool      PAAAAA_101, PBBBBB_101  34-567890   34-567890
    """

    sample_id_pattern = re.compile("(P\d{5})_(\d+)")

    # Single sample
    if len(art.samples) == 1:
        re_match = re.match(sample_id_pattern, art.samples[0].name)
        if re_match:
            return re_match.group()
        else:
            return None

    # Pool
    else:
        # Look at the name of the first sample in the pool
        re_match = re.match(sample_id_pattern, art.samples[0].name)
        # If all samples in the pool have the same project
        if all([re.match(sample_id_pattern, sample.name).groups()[0] == re_match.groups()[0] for sample in art.samples]):
            return f"{re_match.groups()[0]}_{art.id}"
        else:
            return art.id
        

def strip_characters(input_string):

    allowed_characters = re.compile("[^a-zA-Z0-9_-]")
    subbed_string = allowed_characters.sub("_", input_string)

    string_to_shorten = re.compile("__+")
    shortened_string = string_to_shorten.sub("_", subbed_string)

    return shortened_string


def get_kit_string(sample):

    kit_string = sample.udf.get('ONT prep kit')

    if sample.udf.get('ONT expansion kit') != "None":
        kit_string += f" {sample.udf.get('ONT expansion kit')}"

    return kit_string


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)