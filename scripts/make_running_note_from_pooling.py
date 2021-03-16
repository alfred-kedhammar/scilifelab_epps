#!/usr/bin/env python
DESC="""EPP used to create running notes from the pooling step"""

from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from scilifelab_epps.epp import attach_file, EppLogger
from genologics.entities import Process, Project
from datetime import datetime

import json
import sys
import os

from write_notes_to_couchdb import write_note_to_couch


def main(lims, args):

    p=Process(lims, id=args.pid)
    log=[]
    datamap={}
    wsname=None
    username="{0} {1}".format(p.technician.first_name, p.technician.last_name)
    user_email=p.technician.email
    for art in p.all_inputs():
        if len(art.samples)!=1:
            log.append("Warning : artifact {0} has more than one sample".format(art.id))
        for sample in art.samples:
           #take care of lamda DNA
           if sample.project:
                if sample.project.id not in datamap:
                    datamap[sample.project.id]=[sample.name]
                else:
                    datamap[sample.project.id].append(sample.name)

    for art in p.all_outputs():
        try:
            wsname=art.location[0].name
            break
        except:
            pass

    now=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    for pid in datamap:
        pj=Project(lims, id=pid)
        if len(datamap[pid]) > 1:
            rnt="{0} samples planned for {1}".format(len(datamap[pid]), wsname)
        else:
            rnt="{0} sample planned for {1}".format(len(datamap[pid]), wsname)

        running_note = {"note": rnt, "user": username, "email": user_email, "category": "Workset"}
        write_note_to_couch(pid, now, running_note, lims.get_uri())
        log.append("Updated project {0} : {1}, {2} samples in this workset".format(pid,pj.name, len(datamap[pid])))



    with open("EPP_Notes.log", "w") as flog:
        flog.write("\n".join(log))
    for out in p.all_outputs():
        #attach the log file
        if out.name=="RNotes Log":
            attach_file(os.path.join(os.getcwd(), "EPP_Notes.log"), out)

    sys.stderr.write("Updated {0} projects successfully".format(len(datamap.keys())))

if __name__=="__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
