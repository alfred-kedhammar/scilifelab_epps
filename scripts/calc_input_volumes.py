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
    - Input volume, specified from argument
    - Target output amount, specified from argument
    - UDFs 'Concentration' and 'Conc. Units' of input artifact
    - The last recorded UDF 'Size (bp)' is fetched recursively

Outputs the volume to obtain the target amount into the UDF specified from argument.

"""

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")


def calc_input_volume(process: Process, args: Namespace):
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
            if args.vol_in["source"] == "input":
                input_vol = udf_tools.fetch(art_in, args.udf_vol_in)
            elif args.vol_in["source"] == "output":
                input_vol = udf_tools.fetch(art_out, args.udf_vol_in)

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


def parse_udf_arg(arg_string: str) -> dict:
    """Parse UDF argument string into key-value pairs, and validate.

    E.g. the argument

        --vol_in udf='Volume (uL)',source='input',recursive=False

    will assign

        args.vol_in = {
            'udf': 'Volume (uL)',
            'source': 'input',
            'recursive': False
        }

    """
    kv_pairs: str = arg_string.split(",")

    arg_dict = {}
    for kv_pair in kv_pairs:
        key, value = kv_pair.split("=")

        if key == "udf":
            arg_dict[key] = value
        elif key == "source":
            assert value in ["input", "output"]
            arg_dict[key] = value
        elif key == "recursive":
            assert value in ["True", "False"]
            arg_dict[key] = eval(value)
        else:
            raise AssertionError(
                f"Invalid value '{value}' for key '{key}' in argument '{arg_string}'"
            )

    # Apply defaults
    if "recursive" not in arg_dict:
        arg_dict["recursive"] = False
    if "source" not in arg_dict:
        arg_dict["source"] = "output"

    return arg_dict


def main():
    """Example call:

        python calc_input_volumes.py \
        --pid {processLuid} \
        --vol_in 'current:input:Volume (ul)' \
        --amt_out 'current:output:Amount (fmol)' \
        --vol_out 'current:output:Volume to take (uL)'


    """
    ## Parse args
    parser = ArgumentParser(description=DESC)

    # Process ID
    parser.add_argument("--pid", type=str, help="Lims ID for current Process")

    # UDFs
    parser.add_argument(
        "--vol_in",
        type=parse_udf_arg,
        help="Ingoing volume, specified.",
    )
    parser.add_argument(
        "--amt_out",
        type=parse_udf_arg,
        help="Outgoing amount, specified.",
    )
    parser.add_argument(
        "--vol_out",
        type=parse_udf_arg,
        help="Outgoing volume, to be calculated.",
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
        calc_input_volume(process, args)
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
