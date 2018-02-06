
from genologics.entities import *
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.descriptors import StringDescriptor,EntityDescriptor

from __future__ import print_function
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/*.json
SCOPES = ['https://www.googleapis.com/auth/spreadsheets'];
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python LIMS Logbook'

lims= Lims(BASEURI, USERNAME, PASSWORD)

# A full list of LIMS steps with involved instrument and details for logging
def categorization(process_name):
    record={
        "Adapter ligation and reverse transcription (TruSeq small RNA) 1.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Adapter Ligation and 1st Amplification (SMARTer Pico) 4.0" : {"dest_file" : ["Bravo","PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Aliquot Samples for Caliper/Bioanalyzer" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Aliquot Samples for Qubit/Bioanalyzer" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Aliquot Libraries for Hybridization (SS XT)" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Amplify Adapter-Ligated Library (SS XT) 4.0" : {"dest_file" : ["Bravo", "PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Amplify Captured Libraries to Add Index Tags (SS XT) 4.0" : {"dest_file" : ["Bravo","PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Amplify by PCR and Add Index Tags (TruSeq small RNA) 1.0" : {"dest_file" : ["PCR"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Automated Quant-iT QC (DNA) 4.0" : {"dest_file" : ["Tecan"], "instrument" : ["default"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Automated Quant-iT QC (Library Validation) 4.0" : {"dest_file" : ["Tecan"], "instrument" : ["default"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Automated Quant-iT QC (RNA) 4.0" : {"dest_file" : ["Tecan"], "instrument" : ["default"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Bioanalyzer Fragmentation QC (TruSeq DNA) 4.0" : {"dest_file" : ["Bioanalyzer"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : ""}]},
        "Bioanalyzer QC (Library Validation) 4.0" : {"dest_file" : ["Bioanalyzer"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : ""}]},
        "Bioanalyzer QC (DNA) 4.0" : {"dest_file" : ["Bioanalyzer"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : ""}]},
        "Bioanalyzer QC (RNA) 4.0" : {"dest_file" : ["Bioanalyzer"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : "", "udf_Lot no: Ladder" : ""}]},
        "CA Purification" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "CaliperGX QC (DNA)" : {"dest_file" : ["Caliper"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : ""}]},
        "CaliperGX QC (RNA)" : {"dest_file" : ["Caliper"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : "", "udf_Lot no: RNA ladder" : ""}]},
        "Capture And Wash (SS XT) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        #"Cluster Generation (Illumina SBS) 4.0" : {"dest_file" : ["cBot"], "instrument" : ["lims_instrument"], "details" : [""]},
        "End Repair, A-Tailing and Adapter Ligation (SS XT) 4.0" : {"dest_file" : ["Bravo", "PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "End repair, size selection, A-tailing and adapter ligation (Lucigen NxSeq DNA) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "End repair, size selection, A-tailing and adapter ligation (TruSeq DNA Nano) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "End repair, size selection, A-tailing and adapter ligation (TruSeq PCR-free DNA) 4.0" : {"dest_file" : ["Bravo", "PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "End repair, A-tailing and adapter ligation (TruSeq RNA) 4.0" : {"dest_file" : ["Bravo", "PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Enrich DNA fragments (Nextera) 4.0" : {"dest_file" : ["PCR"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Enrich DNA fragments (TruSeq DNA) 4.0" : {"dest_file" : ["PCR", "Bravo"], "instrument" : ["lims_instrument", "udf_Bravo"], "details" : ["", ""]},
        "Enrich DNA fragments (TruSeq RNA) 4.0" : {"dest_file" : ["PCR"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Fragment Analyzer QC (DNA) 4.0" : {"dest_file" : ["FragmentAnalyzer"], "instrument" : ["default"], "details": [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]},
        "Fragment Analyzer QC (Library Validation) 4.0" : {"dest_file" : ["FragmentAnalyzer"], "instrument" : ["default"], "details": [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]},
        "Fragment Analyzer QC (RNA) 4.0" : {"dest_file" : ["FragmentAnalyzer", "instrument"] : ["default"], "details": [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]},
        "Fragment DNA (ThruPlex)" : {"dest_file" : ["Covaris"], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "Fragment DNA (TruSeq DNA) 4.0" : {"dest_file" : ["Covaris"], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "Fragmentation & cDNA synthesis (SMARTer Pico) 4.0" : {"dest_file" : ["PCR"], "instrument" : ["udf_PCR Machine"], "details" : [""]},
        "Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : {"dest_file" : ["PCR"], "instrument" : ["udf_PCR Machine"], "details" : [""]},
        "Hybridize Library (SS XT) 4.0" : {"dest_file" : ["PCR", "Bravo"], "instrument" : ["lims_instrument", "udf_Instrument Used"], "details" : ["", ""]},
        "Library Normalization (HiSeq X) 1.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Normalization (Illumina SBS) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Normalization (MiSeq) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (HiSeq X) 1.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (Illumina SBS) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (MiSeq) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (RAD-seq) v1.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (TruSeq Small RNA) 1.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Linear DNA digestion, Circularized DNA shearing and Streptavidin Bead Binding" : {"dest_file" : ["Covaris"], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "mRNA Purification, Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Pre-Pooling (Illumina SBS) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Pre-Pooling (MiSeq) 4.0" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Purification (ThruPlex)" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "qPCR QC (Dilution Validation) 4.0" : {"dest_file" : ["CFX", "Bravo"], "instrument" : ["lims_instrument", "udf_Instrument Used"], "details": [{"udf_Lot no. qPCR reagent kit" : "", "udf_Lot no. Standard" : ""}, ""]},
        "qPCR QC (Library Validation) 4.0" : {"dest_file" : ["CFX", "Bravo"], "instrument" : ["lims_instrument", "udf_Instrument Used"], "details": [{"udf_Lot no. qPCR reagent kit" : "", "udf_Lot no. Standard" : ""}, ""]},
        "Quant-iT QC (DNA) 4.0" : {"dest_file" : ["CFX"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Quant-iT QC (Library Validation) 4.0" : {"dest_file" : ["CFX"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Quant-iT QC (RNA) 4.0" : {"dest_file" : ["CFX"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Qubit QC (DNA) 4.0" : {"dest_file" : ["Qubit"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "Qubit QC (RNA) 4.0" : {"dest_file" : ["Qubit"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "Qubit QC (Dilution Validation) 4.0" : {"dest_file" : ["Qubit"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "Qubit QC (Library Validation) 4.0" : {"dest_file" : ["Qubit"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "RAD-seq Library Indexing v1.0" : {"dest_file" : ["Bravo","PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Ribosomal cDNA Depletion and 2nd Amplification (SMARTer Pico) 4.0" : {"dest_file" : ["Bravo","PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "RiboZero depletion" : {"dest_file" : ["Bravo","PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Setup Workset/Plate" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Size Selection (Pippin)" : {"dest_file" : ["Pippin"], "instrument" : ["default"], "details": [{"udf_Type: Gel Cassette" : "", "udf_Lot no: Gel Cassette" : "", "udf_Type: Marker" : "", "udf_Lot no: Marker" : "", "udf_Lot no: Electrophoresis Buffer" : ""}]},
        "Size Selection (Robocut)" : {"dest_file" : ["Bravo"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Shear DNA (SS XT) 4.0" : {"dest_file" : ["Covaris"], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "ThruPlex library amplification" : {"dest_file" : ["Bravo","PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "ThruPlex template preparation and synthesis" : {"dest_file" : ["Bravo","PCR"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]}
    }
    return record[process_name]

def get_logbook(dest_file):
    GDoc_logbook = {
        "Bioanalyzer" : { "File" : "" },
        "Brave" : { "File" : "" },
        "Caliper" : { "File" : "" },
        "CFX" : { "File" : "" },
        "Covaris" : { "File" : "" },
        "FragmentAnalyzer" : { "File" : "" },
        "PCR" : { "File" : "" },
        "Pippin" : { "File" : "" },
        "Qubit" : { "File" : "1nbj9E7jWRwa-XZdPNrVx64_bul5CwedjK0VDIyPkCc0" },
        "Tecan" : { "File" : "" },
    }
    return GDoc_logbook[dest_file]["File"]

# TBD: fetching instrument information
def get_instrument(pid):
    return instrument_Name

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
                                   'sheets.googleapis.com-python-quickstart.json')

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
LOGBOOK_COORDINATES = [18, "B", "C", "D", "E"]

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

# All logics about logging
def main():
    pro=Process(lims, id=args.pid)
    log=[]
    time=pro.date_run
    log.append(time)
    user="{0} {1}".format(pro.technician.first_name,pro.technician.last_name)
    log.append(user)
    record=categorization(pro.type.name)

    for instrument in record["instrument"]:

        instrument_number=record["instrument"].index(instrument)
        udf_detail=[]

        if instrument == "default":
            log.append("")
            if record["details"][instrument_number] is not '':
                for item in record["details"][instrument_number]:
                    udf_detail.append(pro.udf[item[3:]])
                log.append(','.join(udf_detail))
                write_record(log,record["dest_file"][instrument_number])
            else:
                log.append("")
                write_record(log,record["dest_file"][instrument_number])

        elif instrument == "lims_instrument":
            instrument_name = get_instrument(args.pid)
            if instrument_name == "Manual operation":
                break
            else:
                log.append(instrument_name)
                if record["details"][instrument_number] is not '':
                    for item in record["details"][instrument_number]:
                        udf_detail.append(pro.udf[item[3:]])
                    log.append(','.join(udf_detail))
                    write_record(log,record["dest_file"][instrument_number])
                else:
                    log.append("")
                    write_record(log,record["dest_file"][instrument_number])

        elif instrument[0:3] == "udf":
            if pro.udf[instrument[3:]] is not '':
                instrument_name = pro.udf[instrument[3:]]
                log.append(instrument_name)
                if record["details"][instrument_number] is not '':
                    for item in record["details"][instrument_number]:
                        udf_detail.append(pro.udf[item[3:]])
                    log.append(','.join(udf_detail))
                    write_record(log,record["dest_file"][instrument_number])
                else:
                    log.append("")
                    write_record(log,record["dest_file"][instrument_number])
            else:
                break
