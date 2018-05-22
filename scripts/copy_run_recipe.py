#!/usr/bin/env python

import re
import os
import sys
import json

from argparse import ArgumentParser
from datetime import datetime
from genologics.lims import Lims
from genologics.entities import Process
from genologics.config import BASEURI, USERNAME, PASSWORD

DESC = """EPP for copying run recipe"""

def main(lims, args):
    process = Process(lims, id=args.pid)
    # Copy Read and index parameter from the step "Load to Flowcell (NovaSeq 6000 v2.0)"
    UDF_to_copy = ['Read 1 Cycles', 'Read 2 Cycles', 'Index Read 1', 'Index Read 2']
    for i in UDF_to_copy:
        if process.parent_processes()[0].udf.get(i):
            process.udf[i]=process.parent_processes()[0].udf[i]
    process.put()
    # Read in run recipe file
    for outart in process.all_outputs():
        if outart.type == 'ResultFile' and outart.name == 'Run Recipe':
            try:
                fid = outart.files[0].id
                file_name = outart.files[0].original_location
                content = lims.get_file_contents(id=fid).read()
            except:
                raise(RuntimeError("Cannot access the run recipe file."))
            break

    with open("/srv/mfs/NovaSeq_data/gls_recipe_novaseq/gls_recipe_ingrid/{}".format(file_name), 'w') as sf:
        sf.write(content)

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
