from requests.exceptions import HTTPError
from genologics.entities import Artifact, Process
import json


DESC = """This is a submodule for defining reuasble functions to handle artifact
UDFs in in the Genonolics Clarity LIMS API.
"""


def put(art: Artifact, target_udf: str, val, on_fail=AssertionError()):
    """Try to put UDF on artifact, optionally without causing fatal error.
    Evaluates true on success and error (default) or on_fail param on failue.
    """

    art.udf[target_udf] = val

    try:
        art.put()
        return True

    except HTTPError as e:
        del art.udf[target_udf]
        if issubclass(type(on_fail), BaseException):
            raise on_fail
        else:
            return on_fail


def exists(art: Artifact, target_udf: str) -> bool:
    """Check whether UDF exists (is assignable) for current article."""

    dummy_value = 0

    # Assign dummy value to UDF, adapt the data type
    try:
        art.udf[target_udf] = dummy_value
    except TypeError:
        art.udf[target_udf] = str(dummy_value)

    # Check whether UDF is assignable
    try:
        art.put()
        del art.udf[target_udf]
        return True
    except HTTPError as e:
        del art.udf[target_udf]
        return False


def is_filled(art: Artifact, target_udf: str) -> bool:
    """Check whether current UDF is populated for current article."""
    try:
        art.udf[target_udf]
        return True
    except KeyError:
        return False


def get_art_tuples(currentStep: Process) -> list:
    """Return i/o tuples whose input OR output is an Analyte."""

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
    return art_tuples


def fetch_from_tuple(
    art_tuple: tuple, target_udfs: str or list, on_fail=AssertionError()
):
    """Try to fetch UDF based on input/output tuple of step that is missing either input or output artifacts,
    optionally without causing fatar error.

    Target UDF can be supplied as a string, or as a prioritized list of strings.
    """

    if type(target_udfs) == str:
        target_udfs = [target_udfs]

    for target_udf in target_udfs:
        try:
            return art_tuple[1]["uri"].udf[target_udf]
        except:
            try:
                return art_tuple[0]["uri"].udf[target_udf]
            except:
                continue

    if issubclass(type(on_fail), BaseException):
        raise on_fail
    else:
        return on_fail


def fetch(art: Artifact, target_udfs: str or list, on_fail=AssertionError()):
    """Try to fetch UDF from artifact, optionally without causing fatar error.

    Target UDF can be supplied as a string, or as a prioritized list of strings.
    """

    if type(target_udfs) == str:
        target_udfs = [target_udfs]

    for target_udf in target_udfs:
        try:
            return art.udf[target_udf]
        except KeyError:
            continue

    if issubclass(type(on_fail), BaseException):
        raise on_fail
    else:
        return on_fail


def list_udfs(art: Artifact) -> list:
    return [item_tuple[0] for item_tuple in art.udf.items()]


def fetch_last(
    currentStep: Process,
    art_tuple: tuple,
    target_udfs: str or list,
    use_current=True,
    print_history=False,
    on_fail=AssertionError(),
):
    """Recursively look for target UDF.

    Target UDF can be supplied as a string, or as a prioritized list of strings.
    """

    # Convert to list, to enable iteration
    if type(target_udfs) == str:
        target_udfs = [target_udfs]

    history = []

    while True:
        history.append({"step_name": currentStep.type.name})

        try:
            input_art = art_tuple[0]["uri"]
        except:
            input_art = None
        try:
            output_art = art_tuple[1]["uri"]
        except:
            output_art = None

        if len(history) == 1 and use_current != True:
            # If we are in the original step and "use_current" is false, skip
            pass
        else:
            # Look trough outputs
            if output_art:
                history[-1].update(
                    {
                        "Output article ID": output_art.id,
                        "Output article UDFs": dict(output_art.udf.items()),
                    }
                )

                for target_udf in target_udfs:
                    if target_udf in list_udfs(output_art):
                        return output_art.udf[target_udf]

            # Look through inputs
            if input_art:
                history[-1].update(
                    {
                        "Input article ID": input_art.id,
                        "Input article UDFs": dict(input_art.udf.items()),
                    }
                )

                for target_udf in target_udfs:
                    if target_udf in list_udfs(input_art):
                        return input_art.udf[target_udf]

        # Cycle to previous step, if possible
        try:
            pp = input_art.parent_process
            pp_tuples = get_art_tuples(currentStep)
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
            art_tuple = pp_tuples[0]

        except:
            if issubclass(type(on_fail), BaseException):
                if print_history == True:
                    print(json.dumps(history, indent=2))
                raise on_fail
            else:
                if print_history == True:
                    print(json.dumps(history, indent=2))
                return on_fail
