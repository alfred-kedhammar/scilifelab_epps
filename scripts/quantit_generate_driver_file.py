#!/usr/bin/env python
from __future__ import print_function
DESC = """EPP script for Quant-iT measurements to generate a driver file (.csv)
which include the Sample names and their positions in the working plate.

Reads from:
    --Lims fields--
    "location"      field of input analytes to the process

Writes to:
    --file--
    "Driver File"   shared result file

Logging:
The script outputs a regular log file with regular execution information.

Written by Maya Brandi
"""
import os
import sys
import logging

from argparse import ArgumentParser
from requests import HTTPError
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process
from scilifelab_epps.epp import EppLogger
from scilifelab_epps.epp import set_field
from scilifelab_epps.epp import ReadResultFiles

class QuantitDriverFile():
    def __init__(self, process, drivf):
        self.udfs = dict(list(process.udf.items()))
        self.drivf = drivf

    def make_location_dict(self, io_filtered):
        """Loops through the input-output map and formates the
        well location info into the driver file formate:
        row,col,,sample_name"""
        location_dict = {}
        for input, output in io_filtered:
            well = output['uri'].location[1]
            sample = input['uri'].name
            row, col = well.split(':')
            location_dict[well] = ','.join([row, col,'', sample])
        return location_dict

    def make_file(self, location_dict):
        """Writes the formated well location info into a driver
        file sorted by row and col."""
        keylist = list(location_dict.keys())
        keylist.sort()
        f = open(self.drivf, 'a')
        print('Row,Column,*Target Name,*Sample Name', file=f)
        for key in keylist:
            print(location_dict[key], file=f)
        f.close()

def main(lims, pid, drivf ,epp_logger):
    process = Process(lims,id = pid)
    QiT = QuantitDriverFile(process, drivf)
    io = process.input_output_maps
    io_filtered = [x for x in io if x[1]['output-generation-type']=='PerInput']
    io_filtered = [x for x in io_filtered if x[1]['output-type']=='ResultFile']
    location_dict = QiT.make_location_dict(io_filtered)
    QiT.make_file(location_dict)

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid', default = None , dest = 'pid',
                        help='Lims id for current Process')
    parser.add_argument('--log', dest = 'log',
                        help=('File name for standard log file, '
                              'for runtime information and problems.'))
    parser.add_argument('--drivf', dest = 'drivf',
                        default = 'QuantiT_driver_file_exported_from_LIMS.csv',
                        help=('File name for Driver file to be generated'))

    args = parser.parse_args()
    lims = Lims(BASEURI,USERNAME,PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args.pid, args.drivf, epp_logger)
