#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

DESC = """EPP script to copy 'concentration' and 'concentration unit' for each
sample sample in the 'Qubit Result File' to the 'Concentration' and 'Conc. Units'
fields of the output analytes of the process.

Warnings are generated to the user and stored in regular log file wich allso
contains regular execution information in the folowing cases:

1) missing row names (samples) in file
2) duplicated row names (samples)
3) missing value (concentrations)
4) values found but for some reason are not successfully copied:

Can be executed in the background or triggered by a user pressing a "blue button".

Written by Maya Brandi and Denis Moreno
"""

import os
import sys
import logging
import numpy as np
import codecs

from argparse import ArgumentParser
from requests import HTTPError
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process
from scilifelab_epps.epp import EppLogger
from scilifelab_epps.epp import ReadResultFiles
from scilifelab_epps.epp import set_field

import csv

def get_qbit_file(process):
    content = None
    for outart in process.all_outputs():
        #get the right output artifact
        if outart.type == 'ResultFile' and outart.name == 'Qubit Result File':
            try:
                fid = outart.files[0].id
                content = lims.get_file_contents(id=fid)
            except:
                raise RuntimeError("Cannot access the tecan output file to read the concentrations.")
            break
    return content

def get_data(csv_content, log):
    read=False
    data={}
    if isinstance(csv_content, str):
        text = csv_content
    else:
        text = csv_content.encode("utf-8")
    # Try to determine the format of the csv:
    dialect = csv.Sniffer().sniff(text)
    pf = csv.reader(text.splitlines(), dialect=dialect)
    #New Qubit
    #defaults
    sample_index=2
    conc_index=6
    conc_unit=7
    for row in pf:
        if 'Test Name' in row:
            #this is the header row
            sample_index=row.index('Test Name')
            conc_index=row.index('Original sample conc.')
            unit_index=conc_index+1
            read=True
        elif(read and row[sample_index]):
            #this is every other row
            if row[sample_index] in data:
                #Sample is duplicated, drop the key
                log.append("sample {0} has two rows in the Qubit CSV file. Please check the file manually.".format(row[sample_index]))
                del data[row[sample_index]]

            else:
                #normal procedure
                data[row[sample_index]]={}
                data[row[sample_index]]['concentration']=row[conc_index]
                data[row[sample_index]]['unit']=row[unit_index]
        elif (not read  and 'Sample' in row):
            sample_index=row.index('Sample')
            conc_index=row.index('Sample Concentration')
            unit_index=conc_index+1
            read=True
    return data

def convert_to_ng_ul(conc, unit):
    factor=float(1.0)#I really want a float
    units=unit.split('/')
    if units[0] == 'µg' or units[0] == 'ug':
        factor*=1000
    elif units[0] == 'mg':
        factor*=1000000
    if units[1] == 'mL':
        factor/=1000
    if units[1] == 'nL':
        factor*=1000

    return conc*factor


def get_qbit_csv_data(process):
    #samples missing from the qubit csv file
    missing_samples = 0
    low_conc = 0
    bad_format = 0
    #strings returned to the EPP user
    log = []
    # Get file contents by parsing lims artifacts
    file_content = get_qbit_file(process)
    #parse the qubit file and get the interesting data out
    data = get_data(file_content, log)

    if "Minimum required concentration (ng/ul)" in process.udf:
        min_conc=process.udf['Minimum required concentration (ng/ul)']
    else:
        min_conc=None
        log.append("Set 'Minimum required concentration (ng/ul)' to get qc-flags based on this threshold!")

    for target_file in process.result_files():
        conc=None
        new_conc=None
        file_sample = target_file.input_artifact_list()[0].name
        if file_sample in data:
            try:
                conc=float(data[file_sample]['concentration'])
            except ValueError:
                #concentration is not a float
                target_file.qc_flag = "FAILED"
                if data[file_sample]['concentration'] != 'Out of range':
                    #Out of range is a valid value, the others are not.
                    bad_format+=1

            else:
                new_conc=convert_to_ng_ul(conc, data[file_sample]['unit'])
                if new_conc is not None :
                    target_file.udf['Concentration'] = new_conc
                    target_file.udf['Conc. Units'] = 'ng/ul'
                    if min_conc:
                        if new_conc < min_conc:
                            target_file.qc_flag = "FAILED"
                            low_conc +=1
                        else:
                            target_file.qc_flag = "PASSED"

            #actually set the data
            target_file.put()
            set_field(target_file)
        else:
            missing_samples += 1
    if low_conc:
        log.append('{0}/{1} samples have low concentration.'.format(low_conc, len(process.result_files())))
    if missing_samples:
        log.append('{0}/{1} samples are missing in the Qubit Result File.'.format(missing_samples, len(process.result_files())))
    if bad_format:
        log.append('There are {0} badly formatted samples in Qubit Result File. Please fix these to get proper results.'.format(bad_format))

    print(''.join(log), file=sys.stderr)

def main(lims, pid, epp_logger):

    process = Process(lims,id = pid)
    get_qbit_csv_data(process)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid', default = '24-38458', dest = 'pid',
                        help='Lims id for current Process')
    parser.add_argument('--log', dest = 'log',
                        help=('File name for standard log file, '
                              'for runtime information and problems.'))

    args = parser.parse_args()

    lims = Lims(BASEURI,USERNAME,PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args.pid, epp_logger)
