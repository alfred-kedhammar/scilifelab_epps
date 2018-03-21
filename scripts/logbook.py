#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

DESC = """EPP script for automatically logging the record of instrument use
into the electronic logbooks in Google Doc.
"""

import httplib2
import os
import sys
import logging
import codecs
import datetime

from requests import HTTPError
from genologics.config import BASEURI,USERNAME,PASSWORD
from scilifelab_epps.epp import EppLogger
from genologics.entities import *
from genologics.lims import *
from genologics.descriptors import StringDescriptor,EntityDescriptor

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

import argparse
from argparse import ArgumentParser

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/*.json
SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];
CLIENT_SECRET_FILE = '~/.credentials/client_secret.json'
APPLICATION_NAME = 'GDOC API FOR LIMS EPP'
flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args('--auth_host_name localhost --logging_level INFO'.split())

# A full list of LIMS steps with involved instrument and details for logging
def categorization(process_name):
    record={
        "Adapter ligation and reverse transcription (TruSeq small RNA) 1.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Adapter Ligation and 1st Amplification (SMARTer Pico) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Aliquot Samples for Caliper/Bioanalyzer" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Aliquot Samples for Qubit/Bioanalyzer" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Aliquot Libraries for Hybridization (SS XT)" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "default" : {
                "dest_file" : "Speedvac",
                "details" : []
            }
        },
        "Amplify Adapter-Ligated Library (SS XT) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Amplify Captured Libraries to Add Index Tags (SS XT) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Amplify by PCR and Add Index Tags (TruSeq small RNA) 1.0" : {
            "lims_instrument" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Automated Quant-iT QC (DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "default" : {
                "dest_file" : "Tecan",
                "details" : [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]
            }
        },
        "Automated Quant-iT QC (Library Validation) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "default" : {
                "dest_file" : "Tecan",
                "details" : [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]
            }
        },
        "Automated Quant-iT QC (RNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "default" : {
                "dest_file" : "Tecan",
                "details" : [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]
            }
        },
        "Bioanalyzer Fragmentation QC (TruSeq DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bioanalyzer",
                "details" : [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : ""}]
            }
        },
        "Bioanalyzer QC (Library Validation) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bioanalyzer",
                "details" : [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : ""}]
            }
        },
        "Bioanalyzer QC (DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bioanalyzer",
                "details" : [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : ""}]
            }
        },
        "Bioanalyzer QC (RNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bioanalyzer",
                "details" : [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : "", "udf_Lot no: Ladder" : ""}]
            }
        },
        "CA Purification" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "CaliperGX QC (DNA)" : {
            "lims_instrument" : {
                "dest_file" : "Caliper",
                "details" : [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : ""}]
            }
        },
        "CaliperGX QC (RNA)" : {
            "lims_instrument" : {
                "dest_file" : "Caliper",
                "details" : [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : "", "udf_Lot no: RNA ladder" : ""}]
            }
        },
        "Capture And Wash (SS XT) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Denature, Dilute and Load Sample (MiSeq) 4.0" : {
            "udf_Instrument Used" : {
                "dest_file" : "MiSeq",
                "details" : [{"udf_Flowcell ID" : "", "udf_RGT#s" : ""}]
            }
        },
        "End Repair, A-Tailing and Adapter Ligation (SS XT) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "End repair, size selection, A-tailing and adapter ligation (Lucigen NxSeq DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "End repair, size selection, A-tailing and adapter ligation (TruSeq DNA Nano) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "End repair, size selection, A-tailing and adapter ligation (TruSeq PCR-free DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "End repair, A-tailing and adapter ligation (TruSeq RNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Enrich DNA fragments (Nextera) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Enrich DNA fragments (TruSeq DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "PCR",
                "details" : []
            },
            "udf_Bravo" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Enrich DNA fragments (TruSeq RNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Fragment Analyzer QC (DNA) 4.0" : {
            "default" : {
                "dest_file" : "FragmentAnalyzer",
                "details" : [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]
            }
        },
        "Fragment Analyzer QC (Library Validation) 4.0" : {
            "default" : {
                "dest_file" : "FragmentAnalyzer",
                "details" : [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]
            }
        },
        "Fragment Analyzer QC (RNA) 4.0" : {
            "default" : {
                "dest_file" : "FragmentAnalyzer",
                "details" : [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]
            }
        },
        "Fragment DNA (ThruPlex)" : {
            "lims_instrument" : {
                "dest_file" : "Covaris",
                "details" : [{"udf_Lot no: Covaris tube" : ""}]
            }
        },
        "Fragment DNA (TruSeq DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Covaris",
                "details" : [{"udf_Lot no: Covaris tube" : ""}]
            }
        },
        "Fragmentation & cDNA synthesis (SMARTer Pico) 4.0" : {
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : {
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Hybridize Library  (SS XT) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "PCR",
                "details" : []
            },
            "udf_Instrument Used" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Normalization (HiSeq X) 1.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Normalization (Illumina SBS) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Normalization (MiSeq) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Normalization (NovaSeq) v2.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Pooling (HiSeq X) 1.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Pooling (Illumina SBS) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Pooling (MiSeq) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Pooling (RAD-seq) v1.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Library Pooling (TruSeq Small RNA) 1.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Linear DNA digestion, Circularized DNA shearing and Streptavidin Bead Binding" : {
            "lims_instrument" : {
                "dest_file" : "Covaris",
                "details" : [{"udf_Lot no: Covaris tube" : ""}]
            }
        },
        "mRNA Purification, Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Pre-Pooling (Illumina SBS) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Pre-Pooling (MiSeq) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Pre-Pooling (NovaSeq) v2.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Purification (ThruPlex)" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "qPCR QC (Dilution Validation) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "CFX",
                "details" : [{"udf_Lot no. qPCR reagent kit" : "", "udf_Lot no. Standard" : ""}]
            },
            "udf_Instrument Used" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "qPCR QC (Library Validation) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "CFX",
                "details" : [{"udf_Lot no. qPCR reagent kit" : "", "udf_Lot no. Standard" : ""}]
            },
            "udf_Instrument Used" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Quant-iT QC (DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "CFX",
                "details" : [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]
            }
        },
        "Quant-iT QC (Library Validation) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "CFX",
                "details" : [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]
            }
        },
        "Quant-iT QC (RNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "CFX",
                "details" : [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]
            }
        },
        "Qubit QC (DNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Qubit",
                "details" : [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]
            }
        },
        "Qubit QC (RNA) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Qubit",
                "details" : [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]
            }
        },
        "Qubit QC (Dilution Validation) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Qubit",
                "details" : [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]
            }
        },
        "Qubit QC (Library Validation) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Qubit",
                "details" : [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]
            }
        },
        "RAD-seq Library Indexing v1.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Ribosomal cDNA Depletion and 2nd Amplification (SMARTer Pico) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "RiboZero depletion" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "Setup Workset/Plate" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Size Selection (Pippin)" : {
            "default" : {
                "dest_file" : "Pippin",
                "details" : [{"udf_Type: Gel Cassette" : "", "udf_Lot no: Gel Cassette" : "", "udf_Type: Marker" : "", "udf_Lot no: Marker" : "", "udf_Lot no: Electrophoresis Buffer" : ""}]
            }
        },
        "Size Selection (Robocut)" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "Shear DNA (SS XT) 4.0" : {
            "lims_instrument" : {
                "dest_file" : "Covaris",
                "details" : [{"udf_Lot no: Covaris tube" : ""}]
            },
            "udf_Instrument Used" : {
                "dest_file" : "Bravo",
                "details" : []
            }
        },
        "ThruPlex library amplification" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        },
        "ThruPlex template preparation and synthesis" : {
            "lims_instrument" : {
                "dest_file" : "Bravo",
                "details" : []
            },
            "udf_PCR Machine" : {
                "dest_file" : "PCR",
                "details" : []
            }
        }
    }
    return record[process_name]

# TBD: add the GDoc logbook IDs
def get_logbook(dest_file):
    GDoc_logbook = {
        "Bioanalyzer" : { "File" : "1m2_kGf-vTi-XP8RCnuxZ3yAR1xIw_ySCpGMuRlb2gFs" },
        "Bravo" : { "File" : "1Di5uRlEI7zlQ7DgvQuEYpwDslf8VMrRnhruk5GWNtIo" },
        "Caliper" : { "File" : "1x3w-0s1-xENQORthMSF1GLTezfXQcsaVEiAfJRRjsoc" },
        "CFX" : { "File" : "19LKni8LO-Dzkvs7gkVHLEcTqzqX2zOerT3SOF9GPHNQ" },
        "Covaris" : { "File" : "1wpSzEdiZcRWk1YFo59Pzt4y-AVSb0Fi9AOg2VFrQOVE" },
        "FragmentAnalyzer" : { "File" : "1T4Cy3ywZvl0-kQR-QbtXzu_sErPaYymXeGMf81fqK8k" },
        "MiSeq" : { "File" : "1ThnEbahwm3InlF_tUJ0riyT3RImVKQINfMD4rB6VThU" },
        "PCR" : { "File" : "1YE_M4ywhr5HuQEV2DhO0oVLDPRkThhuAytAEawcdTZM" },
        "Pippin" : { "File" : "1cJd2Wo9GMVq0HjXrVahxF2o_I_LqIipAreWOXeWwObM" },
        "Qubit" : { "File" : "1-sByQA6XVrbli0V24n4CxdxogLUlRlGvykkxOpBG-_U" },
        "Speedvac" : {"File" : "1Dk7qPJeNmzKtHWEdNkZ4yLB0FycjREqqIhNhInZ8G9g"},
        "Tecan" : { "File" : "1DUBEL8DBf0lnXJjIIjowQf2PrftMo9ECXpeNrDodM4s" }
    }
    return GDoc_logbook[dest_file]["File"]

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-limsepp.json')

    store = Storage(credential_path)
    credentials = store.get()

    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

# In coordinates, the values correspond to LineID of the header, columns for date, operator, instrument name and details. The sheet name shoud be "Logbook"
LOGBOOK_COORDINATES = [22, "B", "C", "D", "E"]

def write_record(content,dest_file):

    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheetId = get_logbook(dest_file)

    # Insert empty line
    batch_update_spreadsheet_request_body = {
        "requests": [
            {
              "insertDimension": {
                "range": {
                  "sheetId": 1,
                  "dimension": "ROWS",
                  "startIndex": LOGBOOK_COORDINATES[0],
                  "endIndex": LOGBOOK_COORDINATES[0]+1
                },
                "inheritFromBefore": False,
              }
            },
          ]
    }
    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=batch_update_spreadsheet_request_body)
    response = request.execute()

    # Fill in values
    rangeName = ''.join(["Logbook","!",str(LOGBOOK_COORDINATES[1]),str(LOGBOOK_COORDINATES[0]+1),":",str(LOGBOOK_COORDINATES[4]),str(LOGBOOK_COORDINATES[0]+1)])
    values = [content]
    data = [{'range' : rangeName, 'majorDimension': "ROWS", 'values' : values}]
    body = {'valueInputOption' : "USER_ENTERED", 'data' : data}
    result = service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheetId, body=body).execute()

# Fetch UDF details
def get_details(record_instrument,pro):
    udf_detail=[]
    if record_instrument["details"] != []:
        for item in record_instrument["details"]:
            try:
                udf_detail.append(item[4:]+":"+pro.udf[item[4:]])
            except KeyError, e:
                continue
        if udf_detail == []:
            details = "-"
        else:
            details = ','.join(udf_detail)
    else:
        details = "-"
    return details

# All logics about logging
def main(lims, pid, epp_logger):
    pro=Process(lims, id=pid)
    log=[]
    time=datetime.datetime.now().strftime("%Y-%m-%d")
    log.append(time)
    user="{0} {1}".format(pro.technician.first_name,pro.technician.last_name)
    log.append(user)
    log_tmp=log[:]
    record=categorization(pro.type.name)

    for instrument in record:

        log=log_tmp[:]

        if instrument == "default":
            log.append("-")
            details = get_details(record[instrument],pro)
            log.append(details)
            write_record(log,record[instrument]["dest_file"])

        elif instrument == "lims_instrument":
            instrument_type = pro.instrument.type
            if instrument_type == "Manual":
                continue
            else:
                instrument_name = pro.instrument.name
                log.append(instrument_name)
                details = get_details(record[instrument],pro)
                log.append(details)
                write_record(log,record[instrument]["dest_file"])

        elif instrument[0:3] == "udf":
            try:
                instrument_name = pro.udf[instrument[4:]]
                if pro.udf[instrument[4:]] != '':
                    log.append(instrument_name)
                    details = get_details(record[instrument],pro)
                    log.append(details)
                    write_record(log,record[instrument]["dest_file"])
            except KeyError, e:
                continue

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
