#!/usr/bin/env python
import os
import pandas as pd
import re
import glob

from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process, Artifact


def get_anglerfish_output_file(lims: Lims, currentStep: Process, log: list):

    flowcell_id: str = currentStep.udf['ONT flow cell ID'].upper().strip()
    anglerfish_file_slot: Artifact = [outart for outart in currentStep.all_outputs() if outart.name == "Anglerfish Result File"][0]
    assert anglerfish_file_slot

    # Try to load file from LIMS
    if anglerfish_file_slot.files:
        log.append("'Anglerfish Result File' detected in the step, loading it directly")
        bytes_content = lims.get_file_contents(id=anglerfish_file_slot.files[0].id).readlines()
        content = [x.decode('utf-8') for x in bytes_content]
    
    # Try to load file from ngi-nas-ns
    else:
        log.append("No 'Anglerfish Result File' detected in the step, trying to fetch it from ngi-nas-ns")

        # Find latest run
        run_query = f"/srv/ngi-nas-ns/minion_data/qc/*{flowcell_id}*"
        run_glob = glob.glob(run_query)
        assert len(run_glob) != 0, f"No runs with flowcell ID {flowcell_id} found on path {run_query}"
        if len(run_glob) > 1:
            runs_list = "\n".join(run_glob)
            log.append(f"WARNING: Multiple runs with flowcell ID {flowcell_id} detected:\n{runs_list}") 
        latest_run_path = max(run_glob, key=os.path.getctime)
        log.append(f"INFO: Using run {latest_run_path}")

        # Find latest Anglerfish results of run
        anglerfish_results_query = f"{latest_run_path}/*anglerfish*/anglerfish_stats.txt"
        anglerfish_results_glob = glob.glob(anglerfish_results_query)
        assert len(anglerfish_results_glob) != 0, f"No Anglerfish results found for query {anglerfish_results_query}"
        if len(anglerfish_results_glob) > 1:
            results_list = "\n".join(anglerfish_results_glob)
            log.append(f"WARNING: Multiple Anglerfish results detected:\n{results_list}") 
        latest_anglerfish_results_path = max(anglerfish_results_glob, key=os.path.getctime)
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
        if 'sample_name' in line and '#reads' in line:
            header = [e.strip() for e in line.split("\t")]
            continue

        # Parse tsv body
        if (header) and (line != '\n'):
            data.append([e.strip() for e in line.split("\t")])

        # Ready tsv body until an empty line
        if (header) and (line == '\n'):
            break

        else:
            continue

    # Compile data into dataframe
    df = pd.DataFrame(data, columns = header)
    df = df.astype({
        "sample_name": str,
        "#reads": int,
        "mean_read_len": float,
        "std_read_len": float,
        "i5_reversed": bool,
        "ont_barcode": str,
    })

    # Add additional metrics
    df["repr_total_pc"] = df["#reads"] / df["#reads"].sum() * 100
    df["repr_within_barcode_pc"] = df.apply(
        lambda row: row["#reads"] / df[df["ont_barcode"] == row["ont_barcode"]]["#reads"].sum() * 100,
        axis = 1,
    )

    return df


def fill_udfs(currentStep: Process, df: pd.DataFrame):
    
    samples = [output for output in currentStep.all_outputs() if output.type == "Analyte" and output.name in list(df["sample_name"])]

    udfs_to_columns = {

    }

    for sample in samples:



def main(lims: Lims, process: Process):

    # Instantiate log file
    log = []

    # Get file contents
    file_content: list = get_anglerfish_output_file(lims, process, log)

    # Parse the Anglerfish output
    df = get_data(file_content, log)

    # Populate sample fields with Anglerfish results
    fill_udfs(df)


if __name__ == "__main__":
    
    parser = ArgumentParser()
    parser.add_argument('--pid', default = '24-594126', dest = 'pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI,USERNAME,PASSWORD)
    lims.check_version()

    main(lims, args.pid)
