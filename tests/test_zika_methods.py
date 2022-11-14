#!/usr/bin/env python

DESC = """
Run this script with pytest 

Written by Alfred Kedhammar
"""


import os
import sys
sys.path.append(
    "../scripts"
)
import zika_methods


def compare(ref_path, wl_filename, log_filename):
      
    # Load reference
    with open(ref_path, "r") as f:
        ref = f.readlines()

    # Load output
    with open(wl_filename, "r") as f:
        op = f.readlines()

    # Remove all comment lines except deck layout
    for wl in [ref, op]:
        to_remove = []
        for l in wl:
            if "COMMENT, " in l and "layout" not in l:
                to_remove.append(l)
        for l in to_remove:
            wl.remove(l)

    # Carry out test
    test_result = ref == op

    if test_result:
        # Clean up files
        os.remove(wl_filename)
        os.remove(log_filename)

    return test_result


def test_setup_QIAseq():

    # Generate data from local input
    wl_filename, log_filename = zika_methods.setup_QIAseq(
        local_data = "setup_QIAseq_input.tsv"
        )
    
    test_result = compare(
        ref_path = "setup_QIAseq_ref.csv",
        wl_filename = wl_filename,
        log_filename = log_filename
    )

    return test_result


def test_amp_norm():

    # Generate data from local input
    wl_filename, log_filename = zika_methods.amp_norm(
        local_data = "amp_norm_input.tsv"
        )
    
    test_result = compare(
        ref_path = "amp_norm_ref.csv",
        wl_filename = wl_filename,
        log_filename = log_filename
    )

    return test_result

def reset_reference(method, input_name, new_ref_name):

    # Generate data from local input
    wl_filename, log_filename = method(
        local_data = input_name
        )

    os.rename(wl_filename, new_ref_name)
    os.remove(log_filename)

