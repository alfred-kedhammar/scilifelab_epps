#!/usr/bin/env python

DESC = """
This module contains tests in which a method from zika_methods.py is run and the "transfer portion" of the resulting worklist is asserted against a pre-generated one.

Written by Alfred Kedhammar
"""

import pandas as pd

from zika_methods import setup_QIAseq

def test_setup_QIAseq():

    wl_filename, log_filename = setup_QIAseq(local_data = "test_QIAseq_input.csv")
    ref = "zika_worklist_setup_QIAseq_local_221109_170723.csv"

