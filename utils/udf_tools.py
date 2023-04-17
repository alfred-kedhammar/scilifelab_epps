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
    

def fetch_last(currentStep, art_tuple, target_udfs, current=True, print_history=False):
    """Recursively look for target UDF. 

    Target UDF can be supplied as a string, or as a prioritized list of strings.
    """

    if type(target_udfs) == str:
        target_udfs = [target_udfs]

    # Return UDF if present in output of current step
    if current:
        for target_udf in target_udfs:
            if target_udf in list_udfs(art_tuple[1]["uri"]):
                return art_tuple[1]["uri"].udf[target_udf]

    # Return UDF if present in input of current step
    for target_udf in target_udfs:
        if target_udf in list_udfs(art_tuple[0]["uri"]):
            return art_tuple[0]["uri"].udf[target_udf]

    # Start looking though previous steps. Use input articles.
    else:
        input_art = art_tuple[0]["uri"]
        # Traceback of artifact ID, step and UDFs
        history = [
            {
                "Input article ID": input_art.id,
                "Step name": currentStep.type.name,
                "Step output UDFs": dict(art_tuple[1]["uri"].udf.items())
            }
        ]
        
        while True:
            if input_art.parent_process:
                pp = input_art.parent_process
                pp_tuples = pp.input_output_maps

                # Find the input whose output is the current artifact
                pp_input_art = [pp_tuple[0]["uri"] for pp_tuple in pp_tuples if pp_tuple[1]["uri"].id == input_art.id][0]
                history.append(            
                    {
                        "Input article ID": pp_input_art.id,
                        "Step name": pp.type.name,
                        "Step output UDFs": dict(pp_input_art.udf.items())
                    }
                )

                for target_udf in target_udfs:
                    if target_udf in list_udfs(pp_input_art):
                        if print_history == True:
                            for j in history:
                                print(json.dumps(j, indent=2))
                        return pp_input_art.udf[target_udf]

                input_art = pp_input_art

            else:
                return False