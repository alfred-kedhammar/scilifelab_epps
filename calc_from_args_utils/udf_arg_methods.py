#!/usr/bin/env python
import logging
from typing import Any

import yaml
from genologics.entities import Artifact, Process

from epp_utils import udf_tools


def fetch_from_arg(
    art_tuple: tuple, arg_dict: dict, process: Process, on_fail=AssertionError
) -> Any:
    """Branching decision-making function. Determine HOW to fetch UDFs given the argument dictionary."""

    history: str | None = None
    source: Artifact | Process
    source_name: str

    # Explicate from where the UDF was fetched
    if arg_dict["source"] == "input":
        source = art_tuple[0]["uri"]
        source_name = source.name
    elif arg_dict["source"] == "output":
        source = art_tuple[1]["uri"]
        source_name = source.name
    elif arg_dict["source"] == "step":
        source = process
        source_name = source.type.name
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
                # Fetch UDF from input or output artifact
                value = udf_tools.fetch(source, arg_dict["udf"])

    except AssertionError:
        if isinstance(on_fail, type) and issubclass(on_fail, Exception):
            found_udfs = source.udf.items()
            msg = "\n".join(
                [
                    f"Could not find matching UDF '{arg_dict['udf']}' from {arg_dict['source']} '{source_name}'",
                    f"Found UDFs: {found_udfs}",
                ]
            )
            raise on_fail(msg)
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
