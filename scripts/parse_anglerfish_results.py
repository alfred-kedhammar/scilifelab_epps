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


def get_anglerfish_output_file(lims, process):

    thisyear=datetime.now().year
    content = None
    flowcell_id = process.udf['ONT flow cell ID'].upper()
    
    for outart in process.all_outputs():
        # First try fetching the Anglerfish result file from the uploaded file in LIMS
        if outart.type == 'ResultFile' and outart.name == 'Anglerfish Result File':
            try:
                fid = outart.files[0].id
                content = lims.get_file_contents(id=fid).readlines()
            except:
                # Second try fetching the Anglerfish result file from the storage server
                try:
                    with open("/srv/ngi-nas-ns/minion_data/qc/anglerfish_stats_{}.txt".format(thisyear, flowcell_id), 'r') as asf:
                        content = asf.readlines()
                    lims.upload_new_file(outart,max(glob.glob("/srv/ngi-nas-ns/nanopore_results/anglerfish/{}/anglerfish_stats_{}.txt".format(thisyear, flowcell_id)),key=os.path.getctime))
                except:
                    raise RuntimeError("No Anglerfish output file available")
                else:
                    raise RuntimeError("Cannot access the folder for Anglerfish output file")
            break
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
