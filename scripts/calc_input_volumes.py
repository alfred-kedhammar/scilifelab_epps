#!/usr/bin/env python

import logging
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime as dt

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims
from ont_generate_samplesheet import upload_file

from epp_utils import formula, udf_tools

DESC = """Calculate how much volume to use, given the necessary UDF paremeters as arguments.

    Used for calculation:
    - Volume of input artifact, UDF specified from argument
    - Target amount of output artifact, UDF specified from argument
    - UDFs 'Concentration' and 'Conc. Units' of input artifact
    - The last recorded UDF 'Size (bp)' is fetched recursively

Outputs the volume to obtain the target amount into the UDF specified from argument.

"""

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")


def calc_input_volume(process: Process, lims: Lims, args: Namespace):
    art_tuples = udf_tools.get_art_tuples(process)

    for art_tuple in art_tuples:
        try:
            art_in = art_tuple[0]["uri"]
            art_out = art_tuple[1]["uri"]
            logging.info(f"Input '{art_in.name}' --> Output '{art_out.name}'")

            # Get last known length
            size_bp, size_bp_history = udf_tools.fetch_last(
                process,
                art_tuple,
                target_udfs="Size (bp)",
                on_fail=None,
                print_history=True,
            )
            logging.info(
                f"Fetched 'Size (bp)': {size_bp}\nFetch history: \n{size_bp_history}"
            )

            # Get info from input artifact
            input_vol = udf_tools.fetch(art_in, args.udf_vol_in)
            logging.info(f"Input '{args.udf_vol_in}': {round(input_vol,2)}")
            input_conc = udf_tools.fetch(art_in, "Concentration")
            logging.info(f"Input 'Concentration': {round(input_conc,2)}")
            input_conc_units = udf_tools.fetch(art_in, "Conc. Units")
            logging.info(f"Input 'Conc. Units': {input_conc_units}")
            assert input_conc_units in [
                "ng/ul",
                "nM",
            ], f'Unsupported conc. units "{input_conc_units}" for art {art_in.name}'

            # Get info from output artifact (UDFs writeable in this step)
            output_amt = udf_tools.fetch(art_out, args.udf_amt_out)
            logging.info(f"Output '{args.udf_amt_out}': {output_amt}")

            # Calculate required volume
            if input_conc_units == "nM":
                vol_required = output_amt / input_conc
            elif input_conc_units == "ng/ul":
                vol_required = min(
                    formula.fmol_to_ng(output_amt, size_bp) / input_conc, input_vol
                )
            logging.info(f"Calculated required volume {round(vol_required,2)} uL.")

            # Adress case of volume depletion
            if vol_required > input_vol:
                logging.warning(
                    f"Volume required ({round(vol_required, 2)} uL) is greater than the available input '{args.udf_vol_in}': {input_vol}."
                )
                logging.warning("Using all available volume.")
                vol_to_take = input_vol

                new_output_amt = vol_to_take * input_conc
                logging.info(
                    f"Updating amount used --> '{args.udf_amt_out}': {output_amt} -> {new_output_amt}"
                )
            else:
                vol_to_take = vol_required

            logging.info(
                f"Calculated vol to take --> '{args.udf_vol_out}': {round(vol_to_take, 2)}"
            )

            # Update volume UDF of output artifact
            udf_tools.put(art_out, args.udf_vol_out, round(vol_to_take, 2))
            logging.info(f"Assigned UDF '{args.udf_vol_out}': {round(vol_to_take, 2)}")
            if vol_required > input_vol:
                logging.warning(
                    f"Changed UDF '{args.udf_amt_out}': {output_amt} -> {new_output_amt}"
                )
        except AssertionError as e:
            logging.error(f"Assertion error: \n{str(e)}")
            logging.warning(f"Skipping artifact {art_out.name}.")
            continue


def main():
    """Example call:

        python calc_input_volumes.py \
        --pid {processLuid} \
        --udf_vol_in 'Volume (ul)' \
        --udf_amt_out 'Amount (fmol)' \
        --udf_vol_out 'Volume to take (uL)'


    """
    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", type=str, help="Lims id for current Process")
    parser.add_argument(
        "--udf_vol_in", type=str, help="UDF for volume of ingoing artifact"
    )
    parser.add_argument(
        "--udf_amt_out", type=str, help="UDF for amount of outgoing artifact"
    )
    parser.add_argument(
        "--udf_vol_out", type=str, help="UDF for volume of outgoing artifact"
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
                "calc-input-volumes",
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

    try:
        calc_input_volume(process, lims, args)
    except Exception as e:
        # Post error to LIMS GUI
        logging.error(e)
        logging.shutdown()
        upload_file(
            file_name=log_filename,
            file_slot="Volume Calculation Log",
            currentStep=process,
            lims=lims,
        )
        sys.stderr.write(str(e))
        sys.exit(2)
    else:
        logging.info("Script completed successfully.")
        logging.shutdown()
        upload_file(
            file_name=log_filename,
            file_slot="Volume Calculation Log",
            currentStep=process,
            lims=lims,
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
