#!/usr/bin/env python
from argparse import ArgumentParser
from datetime import datetime as dt

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.calc_from_args import calculation_methods
from scilifelab_epps.wrapper import epp_decorator

DESC = """UDF-agnostic script to perform calculations across all artifacts of a step.

The script is written with the intention of reusability,
so the script arguments specify which UDFs to use and
from where their values should be fetched.
"""

TIMESTAMP: str = dt.now().strftime("%y%m%d_%H%M%S")


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

    The keys "source" and "recursive" have default values "output" and False respectively.
    """
    kv_pairs: list[str] = arg_string.split(",")

    arg_dict: dict[str, str | bool] = {}
    for kv_pair in kv_pairs:
        key, value = kv_pair.split("=")

        if key == "udf":
            arg_dict[key] = value
        elif key == "source":
            assert value in ["input", "output", "step"]
            arg_dict[key] = value
        elif key == "recursive":
            assert value in ["True", "False"]
            # Eval is usually scary, but we are very strict about what value is here
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


@epp_decorator(script_path=__file__, timestamp=TIMESTAMP)
def main(args):
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

    # Set up LIMS
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    process = Process(lims, id=args.pid)

    function_to_use = getattr(calculation_methods, args.calc)
    function_to_use(process, args)


if __name__ == "__main__":
    # Parse args
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", type=str, help="Lims ID for current Process")
    parser.add_argument(
        "--calc",
        type=str,
        choices=["volume_to_use", "amount", "equimolar_pooling", "summarize_pooling"],
        help="Which function to use for calculations",
    )
    parser.add_argument("--log", type=str, help="Which log file slot to use")

    # UDFs to use for calculations
    udf_args = [
        "vol_in",
        "size_in",
        "conc_in",
        "conc_units_in",
        "amt_out",
        "vol_out",
        "size_out",
    ]
    for udf_arg in udf_args:
        parser.add_argument(f"--{udf_arg}", type=parse_udf_arg)

    args = parser.parse_args()

    main(args)
