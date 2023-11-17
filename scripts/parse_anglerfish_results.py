#!/usr/bin/env python
import os
import pandas as pd
import glob

from datetime import datetime as dt
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process, Artifact
from epp_utils.udf_tools import put, fetch
from epp_utils import formula


def get_anglerfish_output_file(lims: Lims, currentStep: Process, log: list):
    flowcell_id: str = currentStep.udf["ONT flow cell ID"].upper().strip()
    anglerfish_file_slot: Artifact = [
        outart
        for outart in currentStep.all_outputs()
        if outart.name == "Anglerfish Result File"
    ][0]

    # Try to load file from LIMS
    if anglerfish_file_slot.files:
        loaded_file_name = anglerfish_file_slot.files[0].original_location.split("/")[
            -1
        ]
        log.append(
            f"Anglerfish Result File '{loaded_file_name}' detected in the step, loading it directly"
        )
        bytes_content = lims.get_file_contents(
            id=anglerfish_file_slot.files[0].id
        ).readlines()
        content = [x.decode("utf-8") for x in bytes_content]

    # Try to load file from ngi-nas-ns
    else:
        log.append(
            "No 'Anglerfish Result File' detected in the step, trying to fetch it from ngi-nas-ns"
        )

        # Find latest run
        run_query = f"/srv/ngi-nas-ns/minion_data/qc/*{flowcell_id}*"
        run_glob = glob.glob(run_query)
        assert (
            len(run_glob) != 0
        ), f"No runs with flowcell ID {flowcell_id} found on path {run_query}"
        if len(run_glob) > 1:
            runs_list = "\n".join(run_glob)
            log.append(
                f"WARNING: Multiple runs with flowcell ID {flowcell_id} detected:\n{runs_list}"
            )
        latest_run_path = max(run_glob, key=os.path.getctime)
        log.append(f"INFO: Using run {latest_run_path}")

        # Find latest Anglerfish results of run
        anglerfish_results_query = (
            f"{latest_run_path}/*anglerfish*/anglerfish_stats.txt"
        )
        anglerfish_results_glob = glob.glob(anglerfish_results_query)
        assert (
            len(anglerfish_results_glob) != 0
        ), f"No Anglerfish results found for query {anglerfish_results_query}"
        if len(anglerfish_results_glob) > 1:
            results_list = "\n".join(anglerfish_results_glob)
            log.append(
                f"WARNING: Multiple Anglerfish results detected:\n{results_list}"
            )
        latest_anglerfish_results_path = max(
            anglerfish_results_glob, key=os.path.getctime
        )
        log.append(f"INFO: Using Anglerfish results {latest_anglerfish_results_path}")

        # Upload results to LIMS
        lims.upload_new_file(anglerfish_file_slot, latest_anglerfish_results_path)

        # Load file
        content = open(latest_anglerfish_results_path, "r").readlines()

    return content


def get_data(content: list, log: list):
    data = []
    header = None

    # Extract sample data
    for line in content:
        # Search for header
        if "sample_name" in line and "#reads" in line:
            header = [e.strip() for e in line.split("\t")]
            continue

        # Parse tsv body
        if (header) and (line != "\n"):
            data.append([e.strip() for e in line.split("\t")])

        # Ready tsv body until an empty line
        if (header) and (line == "\n"):
            break

        else:
            continue

    # Compile data into dataframe
    df = pd.DataFrame(data, columns=header)
    df = df.astype(
        {
            "sample_name": str,
            "#reads": int,
            "mean_read_len": float,
            "std_read_len": float,
            "i5_reversed": bool,
            "ont_barcode": str,
        }
    )

    # Add additional metrics
    df["repr_total_pc"] = df["#reads"] / df["#reads"].sum() * 100
    df["repr_within_barcode_pc"] = df.apply(
        # Sample reads divided by sum of all sample reads w. the same barcode
        lambda row: row["#reads"]
        / df[df["ont_barcode"] == row["ont_barcode"]]["#reads"].sum()
        * 100,
        axis=1,
    )

    return df


def fill_udfs(currentStep: Process, df: pd.DataFrame, log: list):
    # Dictate which LIMS UDF corresponds to which column in the dataframe
    udfs_to_cols = {
        "# Reads": "#reads",
        "Avg. Read Length": "mean_read_len",
        "Std. Read Length": "std_read_len",
        "Representation Within Run (%)": "repr_total_pc",
        "Representation Within Barcode (%)": "repr_within_barcode_pc",
    }

    # Get Illumina pools
    illumina_pools = [
        input_art
        for input_art in currentStep.all_inputs()
        if input_art.type == "Analyte"
    ]

    for illumina_pool in illumina_pools:
        try:
            # Get Illumina samples in the current pool
            illumina_samples = [
                output
                for output in currentStep.all_outputs()
                if output.type == "ResultFile"
                and output.input_artifact_list()[0].name == illumina_pool.name
                and output.name in list(df["sample_name"])
            ]

            for illumina_sample in illumina_samples:
                try:
                    # Translate the ONT barcode well to the barcode string used by Anglerfish
                    barcode_well: str = fetch(illumina_sample, "ONT Barcode Well")
                    # Add colon if not present
                    if not ":" in barcode_well:
                        barcode_well = f"{barcode_well[0]}:{barcode_well[1:]}"
                    # Get the number corresponding to the well (column-wise)
                    barcode_num_str = str(formula.well_name2num_96plate[barcode_well])
                    # Pad barcode number with leading zero if necessary
                    if len(barcode_num_str) < 2:
                        barcode_num_str = f"0{barcode_num_str}"
                    barcode_name = f"barcode{barcode_num_str}"

                    # Find the dataframe row matching the LIMS output artifact
                    df_barcode = df[df["ont_barcode"] == barcode_name]
                    df_match = df_barcode[
                        df_barcode["sample_name"] == illumina_sample.name
                    ]
                    assert (
                        len(df_match) == 1
                    ), f"Multiple entries matching both Illumina sample name {illumina_sample.name} and ONT barcode {barcode_name} was found in the dataframe."

                    # Start putting UDFs
                    for udf, col in udfs_to_cols.items():
                        try:
                            value = float(df_match[col].values[0])
                            put(
                                illumina_sample,
                                udf,
                                value,
                            )
                        except:
                            log.append(
                                f"ERROR: Could not assign UDF '{udf}' value '{value}' for sample {illumina_sample.name}"
                            )
                            continue

                except:
                    log.append(
                        f"ERROR: Could not process sample {illumina_sample.name}"
                    )
                    continue

        except:
            log.append(f"ERROR: Could not process pool {illumina_pool.name}")
            continue


def write_log(log, currentStep):
    timestamp = dt.now().strftime("%y%m%d_%H%M%S")
    log_filename = f"parse_anglerfish_results_log_{currentStep.id}_{timestamp}_{currentStep.technician.name.replace(' ','')}"
    with open(log_filename, "w") as logContext:
        logContext.write("\n".join(log))
    return log_filename


def upload_log(currentStep, lims, log_filename):
    log_file_slot = [
        slot
        for slot in currentStep.all_outputs()
        if slot.name == "Parse Anglerfish Results Log"
    ][0]
    for f in log_file_slot.files:
        lims.request_session.delete(f.uri)
    lims.upload_new_file(log_file_slot, log_filename)

    # Remove originally written file
    os.remove(log_filename)


def main(lims: Lims, currentStep: Process):
    # Instantiate log file
    log = []

    # Get file contents
    file_content: list = get_anglerfish_output_file(lims, currentStep, log)

    # Parse the Anglerfish output
    df: pd.DataFrame = get_data(file_content, log)

    # Populate sample fields with Anglerfish results
    fill_udfs(df)

    # Add sample comments
    # TODO

    # Write log
    log_filename = write_log(log, currentStep)

    # Upload log
    upload_log(currentStep, lims, log_filename)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--pid", default="24-594126", dest="pid", help="Lims id for current Process"
    )
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    currentStep = Process(lims, id=args.pid)

    main(lims, currentStep)
