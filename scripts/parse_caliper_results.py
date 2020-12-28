#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

DESC = """
Python script for parsing output file from Caliper
And copy data in correspoding step in Clarity LIMS
Written by Chuan Wang
"""

import os
import sys
import logging
import re
import csv

from datetime import datetime
from argparse import ArgumentParser
from requests import HTTPError
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process
from scilifelab_epps.epp import EppLogger
from scilifelab_epps.epp import ReadResultFiles
from scilifelab_epps.epp import set_field

NGISAMPLE_PAT = re.compile("P[0-9]+_[0-9]+")
CALIPER_PAT = re.compile("CaliperGX \([D|R]NA\) (.*)")
SAMPLENAME_PAT = re.compile("[A-H][1-9][0-2]?_(.*)_[0-9]+-[0-9]+_([0-9]+-[0-9]+)*")

# Get file
def get_caliper_output_file(process, log):
    content = None
    for outart in process.all_outputs():
        # Try fetching the Caliper result file from the uploaded file in LIMS
        if outart.type == 'ResultFile' and outart.name == 'CaliperGX WellTable (required)':
            try:
                fid = outart.files[0].id
                content = lims.get_file_contents(id=fid)
            except:
                log.append('No Caliper WellTable file found')
            break
    return content

# Parse file content
def get_data(content, log):
    data = dict()
    headers = dict()
    dialect = csv.Sniffer().sniff(content)
    pf = csv.reader(content.splitlines(), dialect=dialect)
    for row in pf:
        try:
            # Header line
            if 'Sample Name' in row:
                for item in row:
                    headers[item] = row.index(item)
            else:
                sample_data = dict()
                for k, v in headers.items():
                    sample_data[k] = row[v]
                data.update({row[headers['Sample Name']]: sample_data})
        except:
            log.append('Caliper WellTable file in bad format')
    # Process data to include sample ID and well
    for k, v in data.items():
        data[k]['Sample'] = SAMPLENAME_PAT.findall(k)[0][0]
        data[k]['Well'] = v['Well Label'][:1] + ':' + str(int(v['Well Label'][1:]))
    return data

def parse_caliper_results(process):
    # Sample UDF and data map
    map = []
    map_RNA = [('RIN', 'RNA Quality Score'), ('Concentration', 'Total Conc. (ng/ul)')]
    map_DNA = [('Concentration', 'Smear Conc. (ng/ul)'),('Size (bp)', 'Smear Size [BP]'), ('Conc. nM', 'Smear Molarity (nmol/l)')]

    if 'RNA' in process.type.name:
        map = map_RNA
    elif 'DNA' in process.type.name:
        map = map_DNA

    #strings returned to the EPP user
    log = []
    # Get file contents by parsing lims artifacts
    content = get_caliper_output_file(process, log)
    #parse the Caliper output
    data = get_data(content, log)

    # Fill values in LIMS
    for out in process.all_outputs():
        if CALIPER_PAT.findall(out.name):
            found_flag = False
            for k, v in data.items():
                if v['Sample'] == CALIPER_PAT.findall(out.name)[0] and v['Well'] == out.location[1]:
                    found_flag = True
                    for item in map:
                        if v[item[1]] != 'NA' and v[item[1]] != '':
                            out.udf[item[0]] = float(re.sub('\[|\]','',v[item[1]]))
                        else:
                            log.append("Sample {} in well {} missing {}.".format(v['Sample'], v['Well'], item[0]))
                        out.udf['Conc. Units'] = 'ng/ul'
            if found_flag:
                out.put()
                set_field(out)
            else:
                log.append('No record of sample {} in well {} in the Caliper WellTable file.'.format(NGISAMPLE_PAT.findall(out.name)[0], out.location[1]))

    print(''.join(log), file=sys.stderr)

def main(lims, pid, epp_logger):

    process = Process(lims, id = pid)
    parse_caliper_results(process)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid', default = '24-594126', dest = 'pid',
                        help='Lims id for current Process')
    parser.add_argument('--log', dest = 'log',
                        help=('File name for standard log file, '
                              'for runtime information and problems.'))

    args = parser.parse_args()

    lims = Lims(BASEURI,USERNAME,PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args.pid, epp_logger)
