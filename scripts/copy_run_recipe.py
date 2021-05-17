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
    # Read in run recipe file
    for outart in process.all_outputs():
        if outart.type == 'ResultFile' and outart.name == 'Run Recipe':
            try:
                fid = outart.files[0].id
                file_name = outart.files[0].original_location
                content = lims.get_file_contents(id=fid).read()
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
            except:
                raise RuntimeError("Cannot access the run recipe file.")
            break

    with open("/srv/mfs/NovaSeq_data/gls_recipe_novaseq/{}".format(file_name), 'w') as sf:
        sf.write(content)

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
