from requests.exceptions import HTTPError
import json


def put(art, target_udf, val):
    "Try to put UDF on artifact without causing fatal error."

    art.udf[target_udf] = val

    try:
        art.put()
        return True
    except HTTPError as e:
        del art.udf[target_udf]
        return False
    

def exists(art, target_udf):
    "Check whether UDF exists (is assignable) for current article"

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
    

def is_filled(art, target_udf):
    "Check whether current UDF is populated for current article"
    try:
        art.udf[target_udf]
        return True
    except KeyError:
        return False
    

def fetch(art, target_udf, return_failed=None):
    "Try to fetch UDF from artifact without causing fatar error."

    try:
        return art.udf[target_udf]
    except KeyError:
        return return_failed
    

def list_udfs(art):
    return [item_tuple[0] for item_tuple in art.udf.items()]
    

def fetch_last(art_tuple, target_udfs, use_current=True, print_history=False):
    """Recursively look for target UDF. 

    Target UDF can be supplied as a string, or as a prioritized list of strings.
    """

    input_art = art_tuple[0]["uri"]
    output_art = art_tuple[1]["uri"]

    # Convert to list, to enable iteration
    if type(target_udfs) == str:
        target_udfs = [target_udfs]

    # Traceback of artifact ID, step and UDFs
    history = [
        {
            "Step name": art_tuple[1]["uri"].parent_process.type.name,
            "Output article ID": output_art.id,
            "Output article UDFs": dict(output_art.udf.items())
        }
    ]

    # Return UDF if present in output of current step
    if use_current:
        for target_udf in target_udfs:
            if target_udf in list_udfs(output_art):
                if print_history == True:
                    for j in history:
                        print(json.dumps(j, indent=2))
                return output_art.udf[target_udf]
    

    # Use input article if no parent process can be found
    if not input_art.parent_process:
        history.append(            
            {
                "Step name": "-",
                "Output article ID": input_art.id,
                "Output article UDFs": dict(input_art.udf.items())
            }
        )
        for target_udf in target_udfs:
            if target_udf in list_udfs(input_art):
                if print_history == True:
                    for j in history:
                        print(json.dumps(j, indent=2))
                return input_art.udf[target_udf]

    # Start looking though previous steps.
    while True:
        if input_art.parent_process:
            pp = input_art.parent_process
            pp_tuples = pp.input_output_maps

            # Find the input whose output is the current artifact
            pp_tuple = [pp_tuple for pp_tuple in pp_tuples if pp_tuple[1]["uri"].id == input_art.id][0]
            pp_input = pp_tuple[0]["uri"]
            pp_output = pp_tuple[1]["uri"]

            history.append(            
                {
                    "Step name": pp.type.name,
                    "Output article ID": pp_output.id,
                    "Output article UDFs": dict(pp_output.udf.items())
                }
            )

            for target_udf in target_udfs:
                if target_udf in list_udfs(pp_output):
                    if print_history == True:
                        for j in history:
                            print(json.dumps(j, indent=2))
                    return pp_output.udf[target_udf]

            input_art = pp_input

        else:
            return False