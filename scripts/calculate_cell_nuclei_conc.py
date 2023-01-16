#!/usr/bin/env python
from __future__ import print_function

DESC="""EPP script to calculating cell/nuclei concentration
by dividing cell/nuclei count by volume and correcting by % viability.

Author: Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
"""
import os
import sys
import logging
import codecs

from argparse import ArgumentParser
from requests import HTTPError
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process
from scilifelab_epps.epp import EppLogger


def calculate_cell_nuclei_conc(pro):
    log=[]
    artifacts = pro.all_inputs(unique=True)

    for art in artifacts:
        # Fetch data
        sample = art.name.split(' ')[0]
        count = art.udf.get('Count')
        volume = art.udf.get('Volume (ul)')
        percentage_viability = art.udf.get('% Viability')
        value_dict = {'Count': count, 'Volume (ul)': volume, '% Viability': percentage_viability}

        # Calculate when all values are set
        if all([count, volume, percentage_viability]):
            conc = count/volume*percentage_viability/100
            art.udf['Concentration'] = conc
            art.udf['Conc. Units'] = 'count/ul'
            art.udf['Amount (ng)'] = 0
            art.put()
            log.append("Sample {} concentration set to {} count/ul.".format(sample, conc))
        # Throw error message when there is missing value
        else:
            for k, v in value_dict.items():
                if not v:
                    log.append("Sample {} is missing {}.".format(sample, k))

    print(''.join(log), file=sys.stderr)

def main(lims, pid, epp_logger):
    pro = Process(lims,id=pid)
    calculate_cell_nuclei_conc(pro)


if __name__ == "__main__":
    # Initialize parser with standard arguments and description
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid', dest = 'pid',
                        help='Lims id for current Process')
    parser.add_argument('--log', dest = 'log',
                        help=('File name for standard log file, '
                              'for runtime information and problems.'))
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args.pid, epp_logger)
