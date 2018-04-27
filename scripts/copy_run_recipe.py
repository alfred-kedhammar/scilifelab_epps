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
    log=[]
    content = None
    process = Process(lims, id=args.pid)
    # Read in run recipe file
    for outart in process.all_outputs():
        if outart.type == 'ResultFile' and outart.name == 'Run Recipe':
            try:
                fid = outart.files[0].id
                content = lims.get_file_contents(id=fid).read()
                run_name = json.loads(content)["run_name"].encode("utf-8")
            except:
                raise(RuntimeError("Cannot access the run recipe file."))
            break
    # Write file to the server
    if os.path.exists("/srv/mfs/NovaSeq_data/gls_recipe_novaseq/gls_recipe_ingrid"):
        try:
            with open("/srv/mfs/NovaSeq_data/gls_recipe_novaseq/gls_recipe_ingrid/{}.json".format(run_name), 'w') as sf:
                sf.write(content)
                os.chmod("/srv/mfs/NovaSeq_data/gls_recipe_novaseq/gls_recipe_ingrid/{}.json".format(run_name), 0664)
        except Exception as e:
            log.append(str(e))
    else:
        print content
        print log

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
