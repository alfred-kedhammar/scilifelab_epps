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


def test_setup_QIAseq():

    # Generate data from local input
    wl_filename, log_filename = zika_methods.setup_QIAseq(
        local_data = "setup_QIAseq_input.csv"
        )
    
    # Load reference
    ref_path = "setup_QIAseq_ref.csv"
    with open(ref_path, "r") as f:
        ref = f.readlines()

    # Load output
    with open(wl_filename, "r") as f:
        op = f.readlines()

    # Remove line containing timestamp
    ref.pop(2)
    op.pop(2)

    # Carry out test
    test_result = ref == op

    # Clean up files
    os.remove(wl_filename)
    os.remove(log_filename)

    return test_result

