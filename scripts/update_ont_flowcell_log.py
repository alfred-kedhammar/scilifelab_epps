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

DESC = """EPP used to record how ONT flowcells are washed and reloaded during a sequencing run."""


def main(lims, args):
    """
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
        assert "" not in row.values()
        
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

        file_list = write_csv(df_sheet, dir_name, file_list)

    if len(sheets) > 1:
        shutil.make_archive(dir_name, "zip", dir_name)
        upload_file(f"{dir_name}.zip", currentStep, lims)
    else:
        upload_file(f"{os.path.join(dir_name, file_list[0])}", currentStep, lims)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)