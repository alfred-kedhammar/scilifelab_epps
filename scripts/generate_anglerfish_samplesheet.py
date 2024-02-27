#!/usr/bin/env python

import logging
import os
import re
import shutil
import sys
from argparse import ArgumentParser
from datetime import datetime as dt

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from data.Chromium_10X_indexes import Chromium_10X_indexes as idxs_10x
from epp_utils import udf_tools
from epp_utils.formula import well_name2num_96plate as well2num
from scripts.generate_minknow_samplesheet import upload_file

DESC = """Script to generate Anglerfish samplesheet for ONT runs.
"""

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")
SCRIPT_NAME: str = os.path.basename(__file__).split(".")[0]


def generate_anglerfish_samplesheet(process):
    measurements = []

    # Differentiate file outputs from measurements outputs by name, i.e. "P12345_101" vs "Scilifelab SampleSheet"
    sample_pattern = re.compile(r"P\d{5}_\d{3,4}")
    for art in process.all_outputs():
        if re.search(sample_pattern, art.name):
            measurements.append(art)

    ont_barcode_bools = [
        udf_tools.fetch(art, "ONT Barcode Well", on_fail=None) is not None
        for art in measurements
    ]

    if all(ont_barcode_bools):
        ont_barcodes = True
    elif not any(ont_barcode_bools):
        ont_barcodes = False
    else:
        raise AssertionError(
            "ONT barcodes must be present either for all samples or for none."
        )

    rows = []

    # Iterate through the samples
    for sample in measurements:
        if ont_barcodes:
            barcode_well = udf_tools.fetch(sample, "ONT Barcode Well")

            if barcode_well not in well2num:
                barcode_well = barcode_well[0] + ":" + barcode_well[1:]
            barcode_int = well2num[barcode_well]

            fastq_path = f"./fastq_pass/barcode{str(barcode_int).zfill(2)}/*.fastq.gz"  # Assuming the Anglerfish working dir is the ONT run dir TODO

        elif not ont_barcodes:
            fastq_path = "./fastq_pass/*.fastq.gz"

        index_seq_list, adaptors_name = get_index_info(sample)

        # For multi-index samples, append multiple rows
        for index_seq in index_seq_list:
            row = {
                "sample_name": sample.name,
                "adaptors": adaptors_name,
                "index": index_seq,
                "fastq_path": fastq_path,
            }

            rows.append(row)

    df = pd.DataFrame(rows)
    df.sort_values(by="sample_name", inplace=True)

    file_name = f"Anglerfish_samplesheet_{process.id}_{TIMESTAMP}.csv"
    df.to_csv(
        file_name,
        header=False,
        index=False,
    )

    return file_name


def get_index_info(sample):
    """
    Input: LIMS API measurement object

    Output: tuple(
        List of indexes (either i7 or i7-i5),
        The name of the adaptors as defined in Anglerfish config
        )
    """

    index_seq = None

    assert (
        len(sample.reagent_labels) == 1
    ), f"Multiple reagent labels found for sample {sample.name}"

    label = sample.reagent_labels[0]

    index_pattern = re.compile("[ACTG]{4,}-?[ACTG]{4,}")

    ### Get the index sequence ####

    # 1) Look for idx sequence contained directly in .reagent_labels attribute
    index_search = re.search(index_pattern, label)

    if index_search:
        index_seq = index_search.group()

    else:
        # 2) Look for idx among 10X idxs
        if label in idxs_10x:
            idx_10x_list = idxs_10x[label]

            if len(idx_10x_list) == 2:
                # Return i7-i5
                index_seq = "-".join(idx_10x_list)
            elif len(idx_10x_list) == 4:
                # Return list of combination i7 idxs
                index_seq = idx_10x_list
            else:
                raise AssertionError("Unrecognized format of 10X index.")

    ### Get the name of the adaptors ###

    # For now, only support truseq and truseq_dual adaptors TODO
    if "-" in index_seq:
        adaptors_name = "truseq_dual"
    else:
        adaptors_name = "truseq"

    # Return

    if index_seq:
        if not isinstance(index_seq, list):
            index_seq = [index_seq]
        return index_seq, adaptors_name

    else:
        assert index_search, f"No index information found for sample {sample.name}"


def main():
    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", type=str, help="Lims ID for current Process")
    parser.add_argument("--log", type=str, help="Which log file slot to use")
    args = parser.parse_args()

    # Set up LIMS
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    process = Process(lims, id=args.pid)

    # Set up logging
    log_filename: str = (
        "_".join(
            [
                SCRIPT_NAME,
                process.id,
                TIMESTAMP,
                process.technician.name.replace(" ", ""),
            ]
        )
        + ".log"
    )

    logging.basicConfig(
        filename=log_filename,
        filemode="w",
        format="%(levelname)s: %(message)s",
        level=logging.INFO,
    )

    # Start logging
    logging.info(f"Script '{SCRIPT_NAME}' started at {TIMESTAMP}.")
    logging.info(
        f"Launched in step '{process.type.name}' ({process.id}) by {process.technician.name}."
    )
    args_str = "\n\t".join([f"'{arg}': {getattr(args, arg)}" for arg in vars(args)])
    logging.info(f"Script called with arguments: \n\t{args_str}")

    try:
        file_name = generate_anglerfish_samplesheet(process)

        logging.info("Uploading samplesheet to LIMS...")
        upload_file(
            file_name,
            args.file,
            process,
            lims,
        )

        logging.info("Moving samplesheet to ngi-nas-ns...")
        try:
            shutil.move(
                file_name,
                f"/srv/ngi-nas-ns/samplesheets/nanopore/{dt.now().year}/{file_name}",
            )
        except:
            logging.error("Failed to move samplesheet to ngi-nas-ns.")
        else:
            logging.info("Samplesheet moved to ngi-nas-ns.")

    except Exception as e:
        # Post error to LIMS GUI
        logging.error(str(e), exc_info=True)
        logging.shutdown()
        upload_file(
            file_name=log_filename,
            file_slot=args.log,
            process=process,
            lims=lims,
        )
        os.remove(log_filename)
        sys.stderr.write(str(e))
        sys.exit(2)
    else:
        logging.info("")
        logging.info("Script completed successfully.")
        logging.shutdown()
        upload_file(
            file_name=log_filename,
            file_slot=args.log,
            process=process,
            lims=lims,
        )
        # Check log for errors and warnings
        log_content = open(log_filename).read()
        os.remove(log_filename)
        if "ERROR:" in log_content or "WARNING:" in log_content:
            sys.stderr.write(
                "Script finished successfully, but log contains erros or warnings, please have a look."
            )
            sys.exit(2)
        else:
            sys.exit(0)


if __name__ == "__main__":
    main()
