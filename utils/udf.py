from requests.exceptions import HTTPError


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

    dummy_value = "test"

    art.udf[target_udf] = dummy_value

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
    

def fetch(art, target_udf, default=None):
    "Try to fetch UDF from artifact without causing fatar error."

    try:
        return art.udf[target_udf]
    except KeyError:
        return default
    

def fetch_last(currentStep, art_tuple, target_udf, current=True):
    "Recursively look for target UDF."

    # Return udf if present in output of current step
    if current:
        if target_udf in [item_tuple[0] for item_tuple in art_tuple[1]["uri"].udf.items()]:
            return art_tuple[1]["uri"].udf[target_udf]

    # Return udf if present in input of current step
    if target_udf in [item_tuple[0] for item_tuple in art_tuple[0]["uri"].udf.items()]:
        return art_tuple[0]["uri"].udf[target_udf]

    # Start looking though previous steps. Use input articles.
    else:
        input_art = art_tuple[0]["uri"]
        # Traceback of artifact ID, step and UDFs
        history = [(input_art.id, currentStep.type.name, art_tuple[1]["uri"].udf.items())]
        
        while True:
            if input_art.parent_process:
                pp = input_art.parent_process
                pp_tuples = pp.input_output_maps

                # Find the input whose output is the current artifact
                pp_input_art = [pp_tuple[0]["uri"] for pp_tuple in pp_tuples if pp_tuple[1]["uri"].id == input_art.id][0]
                history.append((pp_input_art.id, pp.type.name, pp_input_art.udf.items()))

                if target_udf in [tuple[0] for tuple in pp_input_art.udf.items()]:
                    return pp_input_art.udf[target_udf]
                else:
                    input_art = pp_input_art

            else:
                return False