#!/usr/bin/env python

import logging
import os
import shutil
import sys
from argparse import ArgumentParser
from datetime import datetime as dt

from generate_minknow_samplesheet import get_pool_sample_label_mapping
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from data.Chromium_10X_indexes import Chromium_10X_indexes
from epp_utils.udf_tools import get_art_tuples
from scilifelab_epps.epp import upload_file

DESC = """Script to generate an AVITI run manifest.
"""

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")
SCRIPT_NAME: str = os.path.basename(__file__).split(".")[0]


def generate_aviti_run_manifest(process):
    """
    - For each lane
        - For each pool in the lane
            - For each sample in the pool
                Compile:
                - Name
                - Project
                - Lane
                - Index

    """

    pass


def main():
    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument(
        "--pid",
        required=True,
        type=str,
        help="Lims ID for current Process.",
    )
    parser.add_argument(
        "--log",
        required=True,
        type=str,
        help="Which file slot to use for the script log.",
    )
    parser.add_argument(
        "--file",
        required=True,
        type=str,
        help="Which file slot to use for the run manifest.",
    )
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
        file_name = generate_aviti_run_manifest(process)

        logging.info("Uploading samplesheet to LIMS...")
        upload_file(
            file_name,
            args.file,
            process,
            lims,
        )

        logging.info("Moving samplesheet to ngi-nas-ns...")
        try:
            shutil.copyfile(
                file_name,
                f"/srv/ngi-nas-ns/samplesheets/aviti/{dt.now().year}/{file_name}",
            )
            os.remove(file_name)
        except:
            logging.error("Failed to move samplesheet to ngi-nas-ns.")
        else:
            logging.info("Samplesheet moved to ngi-nas-ns.")

    except Exception as e:
        # Post error to LIMS GUI
        logging.error(str(e), exc_info=True)
        logging.shutdown()
        upload_file(
            file_path=log_filename,
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
            file_path=log_filename,
            file_slot=args.log,
            process=process,
            lims=lims,
        )
        # Check log for errors and warnings
        log_content = open(log_filename).read()
        os.remove(log_filename)
        if "ERROR:" in log_content or "WARNING:" in log_content:
            sys.stderr.write(
                "Script finished successfully, but log contains errors or warnings, please have a look."
            )
            sys.exit(2)
        else:
            sys.exit(0)


if __name__ == "__main__":
    main()
