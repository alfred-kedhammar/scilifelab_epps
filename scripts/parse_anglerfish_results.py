#!/usr/bin/env python
import glob
import logging
import os
import sys
from argparse import ArgumentParser
from datetime import datetime as dt

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Artifact, Process
from genologics.lims import Lims

from scilifelab_epps.epp import upload_file

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")
SCRIPT_NAME: str = os.path.basename(__file__).split(".")[0]


def find_run(process: Process) -> str:
    """From the current step, use the ONT run info from previous step to find the run path."""

    assert len(process.all_inputs()) == 1, "Expected exactly one input artifact"

    run_name = process.all_inputs()[0].udf["ONT run name"]

    # Slap the ONT run name and GenStat link onto the LIMS step for good measure
    process.udf["ONT run name"] = os.path.basename(run_name)
    process.udf["GenStat link"] = (
        f"https://genomics-status.scilifelab.se/flowcells_ont/{run_name}"
    )
    process.put()

    run_query = f"/srv/ngi-nas-ns/minion_data/qc/{run_name}"
    logging.info(f"Looking for path {run_query}")

    run_glob = glob.glob(run_query)
    assert len(run_glob) != 0, f"Path {run_query} doesn't exist"
    assert len(run_glob) == 1, f"Multiple paths found for query {run_query}"

    run_path = run_glob[0]
    logging.info(f"Using run path {run_path}")

    return run_path


def find_latest_anglerfish_run(run_path: str) -> str:
    anglerfish_query = f"{run_path}/**/anglerfish_run*"
    logging.info(f"Looking for Anglerfish runs with query {anglerfish_query}")
    anglerfish_glob = glob.glob(anglerfish_query, recursive=True)

    assert (
        len(anglerfish_glob) != 0
    ), f"No Anglerfish runs found for query {anglerfish_query}"

    if len(anglerfish_glob) > 1:
        runs_list = "\n".join(anglerfish_glob)
        logging.warning(f"Multiple Anglerfish runs detected:\n{runs_list}")
    latest_anglerfish_run_path = max(anglerfish_glob, key=os.path.getctime)
    logging.info(f"Using latest Anglerfish run {latest_anglerfish_run_path}")

    return latest_anglerfish_run_path


def upload_anglerfish_text_results(
    lims: Lims, process: Process, latest_anglerfish_run_path: str
):
    logging.info("Uploading Anglerfish results .txt-file to LIMS")

    anglerfish_file_slot: Artifact = [
        outart
        for outart in process.all_outputs()
        if outart.name == "Anglerfish Result File"
    ][0]

    file_name = os.path.join(latest_anglerfish_run_path, "anglerfish_stats.txt")
    assert os.path.exists(file_name), f"File {file_name} does not exist"

    # Upload results to LIMS
    lims.upload_new_file(anglerfish_file_slot, file_name)


def get_anglerfish_dataframe(latest_anglerfish_run_path: str) -> pd.DataFrame:
    file_name = "anglerfish_dataframe.csv"
    file_path = os.path.join(latest_anglerfish_run_path, file_name)
    assert os.path.exists(file_path), f"File {file_path} does not exist"

    df_raw = pd.read_csv(file_path)

    return df_raw


def parse_data(df_raw: pd.DataFrame):
    df = df_raw.copy()

    # Add additional metrics
    df["repr_total_pc"] = df["num_reads"] / df["num_reads"].sum() * 100
    df["repr_within_barcode_pc"] = df.apply(
        # Sample reads divided by sum of all sample reads w. the same barcode
        lambda row: row["num_reads"]
        / df[df["ont_barcode"] == row["ont_barcode"]]["num_reads"].sum()
        * 100
        if not pd.isna(row["ont_barcode"])
        else None,
        axis=1,
    )

    return df


def fill_udfs(process: Process, df: pd.DataFrame):
    errors = False

    # Get Illumina samples
    measurements = []
    ops = process.all_outputs()
    for op in ops:
        if op.name in list(df.sample_name) and len(op.samples) == 1:
            measurements.append(op)
    measurements.sort(key=lambda x: x.name)

    assert len(measurements) == len(
        df["sample_name"].isin([m.name for m in measurements])
    ), "Number of samples demultiplexed in LIMS does not correspond to \
    number of sample rows in Anglerfish results."

    # Get barcode number from ID
    df["ont_barcode_id"] = df["ont_barcode"].apply(lambda x: int(x[-2:]))

    # Relate UDF names to dataframe column names
    udf2col = {
        "# Reads": "num_reads",
        "Avg. Read Length": "mean_read_len",
        "Std. Read Length": "std_read_len",
        "Representation Within Run (%)": "repr_total_pc",
        "Representation Within Barcode (%)": "repr_within_barcode_pc",
        "ONT Barcode ID": "ont_barcode_id",
    }

    for measurement in measurements:
        sample_name = measurement.name
        sample_row = df[df["sample_name"] == sample_name]

        # Assign UDFs
        for udf, col in udf2col.items():
            try:
                value = float(sample_row[col].values[0])
                measurement.udf[udf] = value
            except:
                errors = True
                logging.error(
                    f"Could not assign UDF '{udf}' value '{value}' for sample {sample_name}, skipping...",
                )
                continue
        try:
            measurement.put()
        except:
            errors = True
            logging.error(f"Could not update sample {sample_name}, skipping...")
            continue

    if errors:
        raise AssertionError("Errors when populating sample UDFs.")


def parse_anglerfish_results(process, lims):
    run_path = find_run(process)

    latest_anglerfish_run_path = find_latest_anglerfish_run(run_path)

    upload_anglerfish_text_results(lims, process, latest_anglerfish_run_path)

    # Get file contents
    df_raw: pd.DataFrame = get_anglerfish_dataframe(latest_anglerfish_run_path)

    # Parse the Anglerfish output
    df_parsed: pd.DataFrame = parse_data(df_raw)

    # Populate sample fields with Anglerfish results
    fill_udfs(process, df_parsed)


def main():
    # Parse args
    parser = ArgumentParser()
    parser.add_argument(
        "--pid", default="24-594126", dest="pid", help="Lims id for current Process"
    )
    parser.add_argument(
        "--log",
        required=True,
        type=str,
        help="Which log file slot to use",
    )
    parser.add_argument(
        "--file",
        required=True,
        type=str,
        help="Which file slot to use for the Anglerfish results",
    )
    args = parser.parse_args()

    # Set up LIMS
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    process = Process(lims, id=args.pid)

    # Set up logging
    log_filename = (
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
        parse_anglerfish_results(process, lims)
    except Exception as e:
        # Post error to LIMS GUI
        logging.error(e, exc_info=True)
        logging.shutdown()
        upload_file(
            file_path=log_filename,
            file_slot=args.log,
            process=process,
            lims=lims,
        )
        sys.stderr.write(str(e))
        sys.exit(2)
    else:
        logging.info("Script completed successfully.")
        logging.shutdown()
        upload_file(
            file_path=log_filename,
            file_slot=args.log,
            process=process,
            lims=lims,
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
