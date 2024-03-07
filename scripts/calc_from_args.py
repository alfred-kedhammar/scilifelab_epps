#!/usr/bin/env python
import logging
import os
import sys
from argparse import ArgumentParser
from datetime import datetime as dt

import yaml
from calc_from_args import methods
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Artifact, Process
from genologics.lims import Lims

from epp_utils import udf_tools
from scripts.generate_minknow_samplesheet import upload_file

DESC = """UDF-agnostic script to perform calculations across all artifacts of a step.

The script is written with the intention of reusability,
so the script arguments specify which UDFs to use and
from where their values should be fetched.
"""

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")
SCRIPT_NAME: str = os.path.basename(__file__).split(".")[0]


def fetch_from_arg(
    art_tuple: tuple, arg_dict: dict, process: Process, on_fail=AssertionError
) -> int | float | str:
    """Branching decision-making function. Determine HOW to fetch UDFs given the argument dictionary."""

    history = None

    # Explicate from where the UDF was fetched
    if arg_dict["source"] == "input":
        source_name = art_tuple[0]["uri"].name
    elif arg_dict["source"] == "output":
        source_name = art_tuple[1]["uri"].name
    elif arg_dict["source"] == "step":
        source_name = process.type.name
    else:
        raise AssertionError(f"Invalid source for {arg_dict}")

    try:
        # Start branching decision-making
        if arg_dict["source"] == "step":
            # Fetch UDF from master step field of current step
            value = process.udf[arg_dict["udf"]]
        else:
            if arg_dict["recursive"]:
                # Fetch UDF recursively, back-tracking the input-output tuple
                if arg_dict["source"] == "input":
                    use_current = False
                else:
                    assert arg_dict["source"] == "output"
                    use_current = True

                value, history = udf_tools.fetch_last(
                    currentStep=process,
                    art_tuple=art_tuple,
                    target_udfs=arg_dict["udf"],
                    use_current=use_current,
                    print_history=True,
                )
            else:
                # Fetch UDF from either input or output artifact
                if arg_dict["source"] == "input":
                    art = art_tuple[0]["uri"]
                elif arg_dict["source"] == "output":
                    art = art_tuple[1]["uri"]
                else:
                    raise AssertionError
                value = udf_tools.fetch(art, arg_dict["udf"])

    except AssertionError:
        if isinstance(on_fail, type) and issubclass(on_fail, Exception):
            raise on_fail(
                f"Could not find matching UDF '{arg_dict['udf']}' from {arg_dict['source']} '{source_name}'"
            )
        else:
            return on_fail

    # Log what has been done
    log_str = f"Fetched UDF '{arg_dict['udf']}': {value} from {arg_dict['source']} '{source_name}'."

    if history:
        history_yaml = yaml.load(history, Loader=yaml.FullLoader)
        last_step_name = history_yaml[-1]["Step name"]
        last_step_id = history_yaml[-1]["Step ID"]
        log_str += f"\n\tUDF recusively fetched from step: '{last_step_name}' (ID: '{last_step_id}')"

    logging.info(log_str)

    return value


def get_UDF_source(
    art_tuple: tuple, arg_dict: dict, process: Process
) -> Artifact | Process:
    """Fetch UDF source for current input-output tuple and UDF arg."""

    if arg_dict["source"] == "input":
        source = art_tuple[0]["uri"]
    elif arg_dict["source"] == "output":
        source = art_tuple[1]["uri"]
    elif arg_dict["source"] == "step":
        source = process
    else:
        raise AssertionError

    return source


def get_UDF_source_name(art_tuple: tuple, arg_dict: dict, process: Process) -> str:
    """Fetch name of UDF source for current input-output tuple and UDF arg."""

    if arg_dict["source"] == "input":
        source_name = art_tuple[0]["uri"].name
    elif arg_dict["source"] == "output":
        source_name = art_tuple[1]["uri"].name
    elif arg_dict["source"] == "step":
        source_name = process.type.name
    else:
        raise AssertionError

    return source_name


def parse_udf_arg(arg_string: str) -> dict:
    """Parse UDF argument string into a dictionary.

    Example:

        the argument

            --vol_in udf='Volume (ul)',source='input',recursive=False

        will assign

            args.vol_in = {
                'udf': 'Volume (ul)',
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
            assert value in ["input", "output", "step"]
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
    f"""Set up log, LIMS instance and parse args.

    Example 1:

        python {__file__} \
        --pid		    '24-885762' \
        --calc          'volume_to_use' \
        --log           'Calculate input volume log' \
        --vol_in        udf='Volume (ul)',source='input' \
        --conc_in       udf='Concentration',source='input' \
        --conc_units_in udf='Conc. Units',source='input' \
        --size_in       udf='Size (bp)',source='output',recursive=True \
        --amt_out       udf='Input Amount (fmol)' \
        --vol_out       udf='Input Volume (uL)'

    Example 2:

        python {__file__} \
        --pid		    '24-885762' \
        --calc          'amount' \
        --log           'Calculate library amount log' \
        --vol_in        udf='Eluted Library Volume (ul)',source='step' \
        --conc_in       udf='Eluted Library Conc. (ng/ul)',source='step' \
        --size_in       udf='Eluted Library Size (bp)',source='step' \
        --amt_out       udf='Eluted Library Amount (fmol)',source='step'

    Example 3:

        python {__file__} \
        --pid		    '24-885762' \
        --calc          'volume_to_use' \
        --log           'Calculate loading volume log' \
        --vol_in        udf='Eluted Library Volume (ul)',source='step' \
        --conc_in       udf='Eluted Library Conc. (ng/ul)',source='step' \
        --size_in       udf='Eluted Library Size (bp)',source='step' \
        --amt_out       udf='Library Loading Amount (fmol)',source='step' \
        --vol_out       udf='Library Volume to Use for Loading (ul)',source='step'

    Example 4:

        python {__file__} \
        --pid		    '24-885819' \
        --calc          'equimolar_pooling' \
        --log           'Calculate pooling log' \
        --vol_in        udf='Eluted Volume (ul)' \
        --conc_in       udf='Eluted Concentration (ng/ul)' \
        --size_in       udf='Size (bp)',recursive=True \
        --amt_out       udf='Amount (fmol)' \
        --vol_out       udf='Total Volume (uL)'

    """

    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", type=str, help="Lims ID for current Process")
    parser.add_argument(
        "--calc",
        type=str,
        choices=["volume_to_use", "amount"],
        help="Which function to use for calculations",
    )
    parser.add_argument("--log", type=str, help="Which log file slot to use")
    # UDFs to use for calculations
    udf_args = ["vol_in", "size_in", "conc_in", "conc_units_in", "amt_out", "vol_out"]
    for udf_arg in udf_args:
        parser.add_argument(f"--{udf_arg}", type=parse_udf_arg)
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
                args.calc,
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
        function_to_use = methods.eval(args.calc)
        function_to_use(process, args)
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
