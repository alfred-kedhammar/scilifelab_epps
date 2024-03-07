import json
from typing import Union

from genologics.entities import Artifact, Process
from requests.exceptions import HTTPError

DESC = """This is a submodule for defining reuasble functions to handle artifact
UDFs in in the Genologics Clarity LIMS API.
"""


def put(target: Artifact | Process, target_udf: str, val, on_fail=AssertionError):
    """Try to put UDF on artifact or process, optionally without causing fatal error.
    Evaluates true on success and error (default) or on_fail param on failue.
    """

    target.udf[target_udf] = val

    try:
        target.put()
        return True

    except HTTPError:
        del target.udf[target_udf]
        if isinstance(on_fail, type) and issubclass(on_fail, Exception):
            raise on_fail(
                f"Can't put UDF '{target_udf}' on '{target.name if isinstance(target, Artifact) else target.type.name}'"
            )
        else:
            return on_fail


def is_filled(art: Artifact, target_udf: str) -> bool:
    """Check whether current UDF is populated for current article."""
    try:
        art.udf[target_udf]
        return True
    except KeyError:
        return False


def no_outputs(currentStep: Process) -> bool:
    """Check whether step has outputs or not"""

    art_tuples = get_art_tuples(currentStep)

    if art_tuples:
        none_outputs = [t[1] is None for t in art_tuples]

        if all(none_outputs):
            return True
        else:
            return False
    else:
        return True


def get_art_tuples(currentStep: Process) -> list:
    """Return I/O tuples whose elements are either
    1) both analytes
        or
    2) an analyte and None
    """

    art_tuples = []
    for art_tuple in currentStep.input_output_maps:
        if art_tuple[0] and art_tuple[1]:
            if art_tuple[0]["uri"].type == art_tuple[1]["uri"].type == "Analyte":
                art_tuples.append(art_tuple)
        elif art_tuple[0] and not art_tuple[1]:
            if art_tuple[0]["uri"].type == "Analyte":
                art_tuples.append(art_tuple)
        elif not art_tuple[0] and art_tuple[1]:
            if art_tuple[1]["uri"].type == "Analyte":
                art_tuples.append(art_tuple)

    # Sort
    art_tuples.sort(key=lambda t: t[1]["uri"].name if t[1] else t[0]["uri"].name)

    return art_tuples


def fetch_from_tuple(art_tuple: tuple, target_udfs: str | list, on_fail=AssertionError):
    """Try to fetch UDF based on input/output tuple of step that is missing either input or output artifacts,
    optionally without causing fatar error.

    Target UDF can be supplied as a string, or as a prioritized list of strings.
    """

    if isinstance(target_udfs, str):
        target_udfs = [target_udfs]

    for target_udf in target_udfs:
        try:
            return art_tuple[1]["uri"].udf[target_udf]
        except:
            try:
                return art_tuple[0]["uri"].udf[target_udf]
            except:
                continue

    if isinstance(on_fail, type) and issubclass(on_fail, Exception):
        raise on_fail(
            f"Could not find matching UDF(s) [{', '.join(target_udfs)}] for artifact tuple {art_tuple}"
        )
    else:
        return on_fail


def fetch(art: Artifact, target_udfs: Union[str, list], on_fail=AssertionError):
    """Try to fetch UDF from artifact, optionally without causing fatar error.

    Target UDF can be supplied as a string, or as a prioritized list of strings.
    """

    if isinstance(target_udfs, str):
        target_udfs = [target_udfs]

    for target_udf in target_udfs:
        try:
            return art.udf[target_udf]
        except KeyError:
            continue

    if isinstance(on_fail, type) and issubclass(on_fail, Exception):
        raise on_fail(
            f"Could not find matching UDF(s) [{', '.join(target_udfs)}] for artifact {art.name}"
        )
    else:
        return on_fail


def list_udfs(art: Artifact) -> list:
    return [item_tuple[0] for item_tuple in art.udf.items()]


def fetch_last(
    currentStep: Process,
    art_tuple: tuple,
    target_udfs: str | list,
    use_current=True,
    print_history=False,
    on_fail=AssertionError,
):
    """Recursively look for target UDF.

    Target UDF can be supplied as a string, or as a prioritized list of strings.

    If "print_history" == True, will return both the target metric and the lookup history as a string.
    """

    # Convert to list, to enable iteration
    if isinstance(target_udfs, str):
        target_udfs = [target_udfs]

    history = []

    while True:
        history.append({"Step name": currentStep.type.name, "Step ID": currentStep.id})

        # Try to grab input and output articles, if possible
        try:
            input_art = art_tuple[0]["uri"]
        except:
            input_art = None
        try:
            output_art = art_tuple[1]["uri"]
        except:
            output_art = None

        if len(history) == 1 and use_current is not True:
            # If we are in the original step and "use_current" is false, skip
            pass
        else:
            # Look trough outputs
            if output_art:
                history[-1].update(
                    {
                        "Derived sample ID": output_art.id,
                        "Derived sample UDFs": dict(output_art.udf.items()),
                    }
                )

                for target_udf in target_udfs:
                    if target_udf in list_udfs(output_art):
                        if print_history is True:
                            return output_art.udf[target_udf], json.dumps(
                                history, indent=2
                            )
                        else:
                            return output_art.udf[target_udf]

            # Look through inputs
            if input_art:
                if input_art.parent_process:
                    history[-1].update(
                        {
                            "Input sample parent step name": input_art.parent_process.type.name,
                            "Input sample parent step ID": input_art.parent_process.id,
                        }
                    )
                history[-1].update(
                    {
                        "Input sample ID": input_art.id,
                        "Input sample UDFs": dict(input_art.udf.items()),
                    }
                )
                for target_udf in target_udfs:
                    if target_udf in list_udfs(input_art):
                        if print_history is True:
                            return input_art.udf[target_udf], json.dumps(
                                history, indent=2
                            )
                        else:
                            return input_art.udf[target_udf]

        # Cycle to previous step, if possible
        try:
            pp = input_art.parent_process
            pp_tuples = get_art_tuples(pp)
            matching_tuples = []
            for pp_tuple in pp_tuples:
                try:
                    pp_input = pp_tuple[0]["uri"]
                except:
                    pp_input = None
                try:
                    pp_output = pp_tuple[1]["uri"]
                except:
                    pp_output = None

                if (pp_input and pp_input.id == input_art.id) or (
                    pp_output and pp_output.id == input_art.id
                ):
                    matching_tuples.append(pp_tuple)

            assert (
                len(matching_tuples) == 1
            ), "Target artifact matches multiple inputs/outputs in previous step."

            # Back-tracking successful, re-assign variables to represent previous step
            currentStep = pp
            art_tuple = matching_tuples[0]

        except:
            if isinstance(on_fail, type) and issubclass(on_fail, Exception):
                if print_history is True:
                    print(json.dumps(history, indent=2))
                raise on_fail(
                    f"Could not find matching UDF(s) [{', '.join(target_udfs)}] for artifact tuple {art_tuple}"
                )
            else:
                if print_history is True:
                    print(json.dumps(history, indent=2))
                return on_fail
