#!/usr/bin/env python
import os
import sys
import pandas as pd
import re
import glob

from datetime import datetime
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process

NGITENXSAMPLE_PAT = re.compile("P[0-9]+_[0-9]+_[0-9]+")
NGISAMPLE_PAT =re.compile("P[0-9]+_[0-9]+")



def get_anglerfish_output_file(lims, currentStep):

    log = []

    content = None
    flowcell_id = currentStep.udf['ONT flow cell ID'].upper().strip()
    anglerfish_file_slot = [outart for outart in currentStep.all_outputs() if outart.name == "Anglerfish Result File"][0]
    assert anglerfish_file_slot

    # Try to load file from LIMS
    try:
        fid = anglerfish_file_slot.files[0].id
        content = lims.get_file_contents(id=fid).readlines()
        log.append("Step already has an uploaded 'Anglerfish Result File', using it.")
    except:
        log.append("No 'Anglerfish Result File' detected in the step, trying to fetch it from ngi-nas-ns.")
        try:
            run_glob = max(glob.glob(f"/srv/ngi-nas-ns/minion_data/qc/*{flowcell_id}*"),key=os.path.getctime)
            if len(run_glob) == 0:
                raise AssertionError # TODO
            elif len(run_glob) > 1:
                raise AssertionError # TODO
            else:
                run_path = run_glob[0]
                anglerfish_run_stats_glob = glob.glob(f"{run_path}/*anglerfish*/anglerfish_stats.txt")
                if len(anglerfish_run_stats_glob) == 0:
                    raise AssertionError # TODO
                elif len(anglerfish_run_stats_glob) > 1:
                    raise AssertionError # TODO
                else:
                    anglerfish_run_stats_path = anglerfish_run_stats_glob[0]
            



            if len(anglerfish_file_glob) > 0:
                lims.upload_new_file(anglerfish_file_slot, anglerfish_file_glob[0])
            else:
                raise RuntimeError("No Anglerfish output file available")
        except:
            raise RuntimeError("Cannot access the folder for Anglerfish output file")

    if isinstance(content[0], bytes):
        content = [x.decode('utf-8') for x in content]

    return content


def get_data(content, log):
    read=False
    raw_data={}
    tenx_samples={}
    results={}
    header_flag = True
    for line in content:
        #Search for header
        if 'sample_name' in line and '#reads' in line:
            header_flag = False
            continue
        if (not header_flag) and (line != '\n'):
            if '\t' in line:
                sample_id = line.split('\t')[0]
                read_count = int(line.split('\t')[1].replace('\n',''))
            else:
                sample_id = line.split()[0]
                read_count = int(line.split()[1])
            raw_data.update({sample_id: read_count})
        # Read file until an empty line
        if (not header_flag) and (line == '\n'):
            break
        else:
            continue
    #Process raw data
    for k,v in raw_data.items():
        #Case of 10X samples
        if NGITENXSAMPLE_PAT.findall(k):
            tenx_sample_id = k.split('_')[0]+'_'+k.split('_')[1]
            if tenx_samples.get(tenx_sample_id):
                tenx_samples[tenx_sample_id] = tenx_samples[tenx_sample_id] + v
            else:
                tenx_samples.update({tenx_sample_id:v})
        else:
            results.update({k:v})

    #Combine ordinary and 10X samples:
    if tenx_samples:
        tenx_samples_copy = tenx_samples.copy()
        results.update(tenx_samples_copy)

    return results


def main(lims, process):

    # Instantiate log file
    log = []

    # Get file contents by parsing lims artifacts
    file_content = get_anglerfish_output_file(lims, process)

    # Parse the Anglerfish output
    data = get_data(file_content, log)


if __name__ == "__main__":
    
    parser = ArgumentParser()
    parser.add_argument('--pid', default = '24-594126', dest = 'pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI,USERNAME,PASSWORD)
    lims.check_version()

    main(lims, args.pid)
