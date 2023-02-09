#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from datetime import datetime as dt
import pandas as pd
import re

DESC = """EPP used to generate a MinKNOW sample sheet for ONT samples"""


def main(lims, args):
    """ Barcoding vs no-barcoding, devices and experiments cannot be mixed in a samplesheet and must be split across multiple ones.
    Create many sample sheets and deliver them as a zipped file.

    experiment_id   The LIMS step generating the samplesheet(s), concatenated with the specified sample sheet name, e.g. "12-34567_P12345-many-flowcells"
    """

    currentStep = Process(lims, id=args.pid)

    arts = [art for art in currentStep.all_inputs() \
        if art.type == "Analyte"]
    
    timestamp = dt.now().strftime("%y%m%d_%H%M%S")

    rows = []
    for art in arts:

        row = {
            "sheet_name": strip_characters(art.udf.get('ONT sample sheet name')),
            "instrument": art.udf.get("ONT flow cell type"),
            "flow_cell_id": art.udf.get("ONT flow cell ID"),
            "sample_id": strip_characters(get_minknow_sample_id(art)),
            "experiment_id": f"{currentStep.id}_{timestamp}_{strip_characters(art.udf.get('ONT sample sheet name'))}",
            "flow_cell_product_code": get_fc_product_code(art),
            "kit": get_kit_string(art)
        }
        
        # Singleton sample
        if len(art.samples) == 1:
            row["alias"] = ""
            row["barcode"] = ""
            rows.append(row)

        # Pool
        else:
            for sample, label in zip(art.samples, art.reagent_labels):
                row["alias"] = strip_characters(sample.name)
                row["barcode"] = strip_characters("barcode" + label[0:2])
                rows.append(row.copy())

    df = pd.DataFrame(rows)

    # TODO Assertions
    # Check that all samples in a single sample sheet belong to the same project

    # Iterate across sheets
    sheets = df.sheet_name.unique()
    for sheet in sheets:
        
        # Subset dataframe to current sheet
        df_sheet = df[df.sheet_name == sheet]

        # Barcodes
        if df_sheet[df_sheet.alias == ""].empty and df_sheet[df_sheet.barcode == ""].empty:

            # Assert aliases and barcodes are unique
            assert len(df_sheet.alias.unique()) == len(df_sheet)
            assert len(df_sheet.barcode.unique()) == len(df_sheet)

        # No barcodes
        elif df_sheet[df_sheet.alias != ""].empty and df_sheet[df_sheet.barcode != ""].empty:
            
            # Trim away unused columns
            df_sheet = df_sheet.loc[:, "sheet_name" : "kit"]

        else:
            # Sheet dataframe rows must either all have barcodes or none have barcodes
            raise AssertionError

        # Assert sheet contains only one experiment
        assert len(df_sheet.experiment_id.unique()) == 1
        # Assert sheet contains only one instrument
        assert len(df_sheet.instrument.unique()) == 1

        file_list = []
        write_csv(df_sheet, file_list)

    upload_csv(df, currentStep)


def write_csv(df_sheet, file_list):

    file_name = f"ONT_samplesheet_{df_sheet.experiment_id.unique()[0]}.csv"
    file_list.append(file_name)

    df_csv = df_sheet.loc[:, [
        "flow_cell_id",
        "sample_id",
        "experiment_id",
        "flow_cell_product_code",
        "kit"
    ]]

    df_csv.to_csv(file_name, index = False)

def get_minknow_sample_id(art):

    sample_id_pattern = re.compile("(P\d{5})_(\d+)")

    if re.match(sample_id_pattern, art.name):
        return re.match(sample_id_pattern, art.name).group()
    
    elif re.match(sample_id_pattern, art.samples[0].name):
        return f"{re.match(sample_id_pattern, art.samples[0].name).groups()[0]}_{art.name}"

    else:
        return None


def strip_characters(input_string):

    allowed_characters = re.compile("[^a-zA-Z0-9_-]")
    subbed_string = allowed_characters.sub("_", input_string)

    string_to_shorten = re.compile("__+")
    shortened_string = string_to_shorten.sub("_", subbed_string)

    return shortened_string


def upload_csv(df, currentStep, lims):

    timestamp = dt.now().strftime("%y%m%d_%H%M%S")
    instrument = df.instrument[0]

    csv_name = f"ONT_{instrument}_sample_sheet_{timestamp}.csv"
    df.loc[:, "flow_cell_id" : "barcode"].to_csv(csv_name, index=False)
    
    for out in currentStep.all_outputs():
        if out.name == f"ONT {instrument} sample sheet":
            for f in out.files:
                lims.request_session.delete(f.uri)
            lims.upload_new_file(out, csv_name)


def get_fc_product_code(sample):

    type_version = f"{sample.udf.get('ONT flow cell type')} {sample.udf.get('ONT flow cell version')}"
    type_version_to_product_code = {
        "PromethION R9.4.1"  : "FLO-PRO002",
        "MinION R9.4.1"      : "FLO-MIN106D",
        "Flongle R9.4.1"     : "FLO-FLG001",
        "PromethION R10.4.1" : "FLO-PRO114M",
        "MinION R10.4.1"     : "FLO-MIN114",
        "Flongle R10.4.1"    : "FLO-FLG114",
    }

    return type_version_to_product_code[type_version]


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