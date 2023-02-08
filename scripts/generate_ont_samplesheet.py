#!/usr/bin/env python

from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
from datetime import datetime as dt
import pandas as pd

DESC = """EPP used to generate a MinKNOW sample sheet for ONT samples"""


def main(lims, args):
    """ Barcoding vs no-barcoding, devices and experiments cannot be mixed in a samplesheet and must be split across multiple ones.
    Create many sample sheets and deliver them as a zipped file.
    """

    currentStep = Process(lims, id=args.pid)

    arts = [art for art in currentStep.all_inputs() \
        if art.type == "Analyte"]

    rows = []
    for art in arts:

        row = {
            "sheet_name": art.udf.get('ONT sample sheet name'),
            "instrument": art.udf.get("ONT flow cell type"),
            "flow_cell_id": art.udf.get("ONT flow cell ID"),
            "sample_id": art.name,
            "experiment_id": f"{art.samples[0].project.id}_{art.udf.get('ONT sample sheet name')}",
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
                row["alias"] = sample.name
                row["barcode"] = "barcode" + label[0:2]
                rows.append(row.copy())

    df = pd.DataFrame(rows)

    # TODO Assertions
    # Check that all samples in a single sample sheet belong to the same project

    # Iterate across sheets
    sheets = df.sheet_name.unique()
    common_file_suffix = f"{currentStep.id}_{dt.now().strftime('%y%m%d_%H%M%S')}"
    for sheet in sheets:
        
        df_sheet = df[df.sheet_name == sheet]

        if 
        df_csv = df_sheet.loc[:, "flow_cell_id" : "barcode"]
        write_csv(df_sheet, common_file_suffix)

    upload_csv(df, currentStep)


def write_csv(df_sheet):
    


def zip_csvs(filenames):
    pass


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