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
    Barcoding vs no-barcoding, devices and experiments cannot be mixed in a samplesheet and must be split across multiple ones.
    Possible to create many sample sheets and deliver them as a zipped file.

    LIMS UDFs
    ONT samplesheet name        Freely decided by user. Shared between libraries that will start sequencing together.
    ONT flow cell type          PromethION or MinION

    Sample sheet columns      
    flow_cell_id                -
    sample_id                   For single samples: e.g. P12345_101, For pools: e.g. P12345_lims-pool-name
    experiment_id               lims-step_yymmdd_hhmmss_samplesheet-name
    flow_cell_product_code      -
    kit                         Product codes separated by spaces
    alias                       Only included for barcoded pools, sample name e.g. P12345_101
    barcorde                    barcode01, barcode02, etc, excavated from LIMS

    Outputs
    .zip file                   ONT_samplesheets_lims-step_yymmdd_hhmmss.zip
    Samplesheets                ONT_samplesheet_lims-step_yymmdd_hhmmss_samplesheet-name.csv
    """
    try:
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
            assert "" not in row.values(), "All fields must be populated."
            
            # Singleton sample
            if art.udf.get('ONT expansion kit') == "None":
                row["alias"] = ""
                row["barcode"] = ""
                rows.append(row)
            # Pool
            else:
                for sample, label in zip(art.samples, art.reagent_labels):
                    row["alias"] = strip_characters(sample.name)
                    row["barcode"] = strip_characters("barcode" + label[0:2])   # TODO double check extraction of barcode number
                    rows.append(row.copy())

        df = pd.DataFrame(rows)

        # Create output dir
        file_list = []
        dir_name = f"ONT_samplesheets_{currentStep.id}_{timestamp}"
        os.mkdir(dir_name)

        # Iterate across sheets
        sheets = df.sheet_name.unique()
        for sheet in sheets:
            
            # Subset dataframe to current sheet
            df_sheet = df[df.sheet_name == sheet]

            # Barcodes
            if df_sheet[df_sheet.alias == ""].empty and df_sheet[df_sheet.barcode == ""].empty:
                pass

            # No barcodes
            elif df_sheet[df_sheet.alias != ""].empty and df_sheet[df_sheet.barcode != ""].empty:
                # Trim away unused columns
                df_sheet = df_sheet.loc[:, "sheet_name" : "kit"]

            else:
                raise AssertionError("Barcoded and non-barcoded libraries can not be mixed in the same sample sheet.")

            assert len(df_sheet.experiment_id.unique()) == 1, "Assert sheet contains only one experiment."
            assert len(df_sheet.instrument.unique()) == 1, "Assert sheet contains only one instrument."
            if len(df_sheet) > 1:
                assert df_sheet.instrument.unique()[0] == "PromethION", "Only PromethION flowcells can be grouped together in the same sample sheet."

            file_list = write_csv(df_sheet, dir_name, file_list)

        if len(sheets) > 1:
            shutil.make_archive(dir_name, "zip", dir_name)
            upload_file(f"{dir_name}.zip", currentStep, lims)
        else:
            upload_file(f"{os.path.join(dir_name, file_list[0])}", currentStep, lims)
        
    except Exception as e:
        sys.stderr.write(e)
        sys.exit(2)


def upload_file(file_name, currentStep, lims):
    for out in currentStep.all_outputs():
        if out.name == f"ONT PromethION sample sheet": # TODO fix output file slot
            for f in out.files:
                lims.request_session.delete(f.uri)
            lims.upload_new_file(out, file_name)


def write_csv(df_sheet, dir_name, file_list):

    file_name = f"ONT_samplesheet_{df_sheet.experiment_id.unique()[0]}.csv"
    file_list.append(file_name)

    columns = [
        "flow_cell_id",
        "sample_id",
        "experiment_id",
        "flow_cell_product_code",
        "kit"
    ]

    if "alias" in df_sheet.columns and "barcode" in df_sheet.columns:
        columns.append("alias")
        columns.append("barcode")
    
    df_csv = df_sheet.loc[:, columns]

    df_csv.to_csv(os.path.join(dir_name, file_name), index = False)
    return file_list


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