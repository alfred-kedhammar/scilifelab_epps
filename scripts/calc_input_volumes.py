#!/usr/bin/env python
import os
import logging
import sys
from argparse import ArgumentParser, Namespace
from datetime import datetime as dt

import yaml
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims
from ont_generate_samplesheet import upload_file

from epp_utils import formula, udf_tools

DESC = """Given a target amount, calculate how much volume to use.

The script is written with the intention of reusability,
so the script arguments specify which UDFs to use and
from where their values should be fetched.
"""

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")


def fetch_from_arg(
    art_tuple: tuple, arg_dict: dict, process: Process
) -> int | float | str:
    """Branching decision-making function. Determine HOW to fetch UDFs given the argument dictionary."""

    if arg_dict["recursive"]:
        if arg_dict["source"] == "input":
            use_current = False
        elif arg_dict["source"] == "output":
            use_current = True
        else:
            raise AssertionError(f"Invalid source '{arg_dict['source']}'")
        value, history = udf_tools.fetch_last(
            currentStep=process,
            art_tuple=art_tuple,
            target_udfs=arg_dict["udf"],
            use_current=use_current,
            print_history=True,
        )
    else:
        if arg_dict["source"] == "input":
            art = art_tuple[0]["uri"]
        elif arg_dict["source"] == "output":
            art = art_tuple[1]["uri"]
        else:
            raise AssertionError(f"Invalid source '{arg_dict['source']}'")
        history = None
        value = udf_tools.fetch(art, arg_dict["udf"])

    log_str = " ".join(
        [
            f"{'Fetched' if not arg_dict['recursive'] else 'Recusively fetched'}",
            f"UDF '{arg_dict['udf']}': {value}",
            f"from {arg_dict['source']} artifact",
            f"'{art_tuple[0]['uri'].name if arg_dict['source'] == 'input' else art_tuple[0]['uri'].name }'.",
        ]
    )
    logging.info(log_str)
    if history:
        history_yaml = yaml.load(history, Loader=yaml.FullLoader)
        last_step_name = history_yaml[-1]["Step name"]
        last_step_id = history_yaml[-1]["Step ID"]
        logging.info(f"UDF fetched from: '{last_step_name}' (ID: '{last_step_id}')")

    return value


def calc_input_volume(process: Process, args: Namespace):
    art_tuples = udf_tools.get_art_tuples(process)

    for art_tuple in art_tuples:
        try:
            art_in = art_tuple[0]["uri"]
            art_out = art_tuple[1]["uri"]
            logging.info(
                f"Processing input '{art_in.name}' -> output '{art_out.name}'..."
            )

            # Get info specified by script arguments
            size_bp = fetch_from_arg(art_tuple, args.size_in, process)
            input_conc = fetch_from_arg(art_tuple, args.conc_in, process)
            input_vol = fetch_from_arg(art_tuple, args.vol_in, process)
            output_amt = fetch_from_arg(art_tuple, args.amt_out, process)
            if args.conc_units_in:
                input_conc_units = fetch_from_arg(
                    art_tuple, args.conc_units_in, process
                )
                assert input_conc_units in [
                    "ng/ul",
                    "nM",
                ], f'Unsupported conc. units "{input_conc_units}" for art {art_in.name}'
            else:
                if "ng" in args.conc_in["udf"]:
                    input_conc_units = "nM"
                elif "nM" in args.conc_in["udf"]:
                    input_conc_units = "nM"
                else:
                    raise AssertionError(
                        f"No concentration units can inferred for {art_out.name}."
                    )

            # Calculate required volume
            if input_conc_units == "nM":
                vol_required = output_amt / input_conc
            elif input_conc_units == "ng/ul":
                vol_required = min(
                    formula.fmol_to_ng(output_amt, size_bp) / input_conc, input_vol
                )
            logging.info(f"Calculated required volume {vol_required:.2f} uL.")

            # Adress case of volume depletion
            if vol_required > input_vol:
                logging.warning(
                    f"Volume required ({vol_required:.2f} uL) is greater than the available input '{args.vol_in['udf']}': {input_vol:.2f}."
                )
                logging.warning("Using all available volume.")
                vol_to_take = input_vol

                new_output_amt = vol_to_take * input_conc
                logging.info(
                    f"Updating amount used -> '{args.amt_out['udf']}': {output_amt} -> {new_output_amt:.2f}"
                )
            else:
                vol_to_take = vol_required

            logging.info(
                f"Calculated vol to take -> '{args.vol_out['udf']}': {vol_to_take:.2f}"
            )

            # Update volume UDF of output artifact
            udf_tools.put(art_out, args.vol_out["udf"], vol_to_take)
            logging.info(f"Assigned UDF '{args.vol_out['udf']}': {vol_to_take:.2f}")
            if vol_required > input_vol:
                udf_tools.put(art_out, args.amt_out["udf"], new_output_amt)
                logging.warning(
                    f"Changed UDF '{args.amt_out['udf']}': {output_amt} -> {new_output_amt:.2f}"
                )
        except AssertionError as e:
            logging.error(f"Assertion error: \n{str(e)}")
            logging.warning(f"Skipping artifact {art_out.name}.")
            continue


def parse_udf_arg(arg_string: str) -> dict:
    """Parse UDF argument string into key-value pairs, and validate.

    Example:

        the argument

            --vol_in udf='Volume (uL)',source='input',recursive=False

        will assign

            args.vol_in = {
                'udf': 'Volume (uL)',
                'source': 'input',
                'recursive': False
            }

    The keys "source" and "recursive" have default values.
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
    """Set up log, LIMS instance and parse args.

    Example 1:

        python scripts/calc_input_volumes.py \
        --pid           24-885698 \
        --vol_in        udf='Volume (ul)',source='input' \
        --conc_in       udf='Concentration',source='input' \
        --conc_units_in udf='Conc. Units',source='input' \
        --size_in       udf='Size (bp)',source='output',recursive=True \
        --amt_out       udf='Input Amount (fmol)' \
        --vol_out       udf='Volume (ul)'

    Example 2:

        python scripts/calc_input_volumes.py \
        --pid           24-885698 \
        --vol_in        udf='Library volume (uL)' \
        --conc_in       udf='Library Conc. (ng/ul)' \
        --size_in       udf='Size (bp)',source='output',recursive=True \
        --amt_out       udf='ONT flow cell loading amount (fmol)' \
        --vol_out       udf='Library to load (uL)'
    """

    ## Parse args
    parser = ArgumentParser(description=DESC)

    # Process ID
    parser.add_argument("--pid", type=str, help="Lims ID for current Process")

    ## UDFs
    # To use for calculations
    parser.add_argument(
        "--vol_in",
        type=parse_udf_arg,
        help="Ingoing volume.",
    )
    parser.add_argument(
        "--size_in",
        type=parse_udf_arg,
        help="Ingoing size.",
    )
    parser.add_argument(
        "--conc_in",
        type=parse_udf_arg,
        help="Ingoing concentration.",
    )
    parser.add_argument(
        "--conc_units_in",
        default=None,
        type=parse_udf_arg,
        help="Ingoing concentration units.",
    )
    parser.add_argument(
        "--amt_out",
        type=parse_udf_arg,
        help="Outgoing amount.",
    )
    # To calculate
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
        os.remove(log_filename)
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
        # Check log for erros and warnings
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
