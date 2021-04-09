#!/usr/bin/env python

import os
import sys
import glob

from argparse import ArgumentParser
from datetime import datetime
from genologics.lims import Lims
from genologics.entities import Process
from genologics.config import BASEURI, USERNAME, PASSWORD
from flowcell_parser.classes import RunParser, RunParametersParser


DESC = """EPP for parsing run paramters for Illumina MiSeq and NextSeq runs"""


def fetch_fc(process):
    fc_id = ''
    if process.parent_processes()[0].type.name == 'Load to Flowcell (NextSeq v1.0)':
        if process.parent_processes()[0].udf['Experiment Name']:
            fc_id = process.parent_processes()[0].udf['Experiment Name'].upper()
        else:
            sys.stderr.write("Experiment Name is empty in the associated Load to Flowcell (NextSeq v1.0) step.")
            sys.exit(2)
    elif process.parent_processes()[0].type.name == 'Denature, Dilute and Load Sample (MiSeq) 4.0':
        if process.parent_processes()[0].udf['Flowcell ID']:
            fc_id = process.parent_processes()[0].udf['Flowcell ID'].upper()
        else:
            sys.stderr.write("Flowcell ID is empty in the associated Denature, Dilute and Load Sample (MiSeq) 4.0 step.")
            sys.exit(2)
    else:
        sys.stderr.write("No associated parent step can be found.")
        sys.exit(2)
    return fc_id


def fetch_rundir(fc_id, run_type):
    run_dir = ''
    if run_type == 'nextseq':
        data_dir = 'NextSeq_data'
    elif run_type == 'miseq':
        data_dir = 'miseq_data'
    run_dir_path = os.path.join(os.sep,"srv","mfs",data_dir,"*{}".format(fc_id))
    if len(glob.glob(run_dir_path)) == 1:
        run_dir = glob.glob(run_dir_path)[0]
    elif len(glob.glob(run_dir_path)) == 0:
        sys.stderr.write("No run dir can be found for FC {}".format(fc_id))
        sys.exit(2)
    else:
        sys.stderr.write("Multiple run dirs found for FC {}".format(fc_id))
        sys.exit(2)
    return run_dir


def parse_run(run_dir):
    runParserObj = RunParser(run_dir)
    if os.path.exists("{}/RunParameters.xml".format(run_dir)):
        RunParametersParserObj = RunParametersParser("{}/RunParameters.xml".format(run_dir))
    elif os.path.exists("{}/runParameters.xml".format(run_dir)):
        RunParametersParserObj = RunParametersParser("{}/runParameters.xml".format(run_dir))
    else:
        sys.stderr.write("No RunParameters.xml found for FC {}".format(fc_id))
        sys.exit(2)
    return runParserObj, RunParametersParserObj


def attach_xml(process, run_dir):
    for outart in process.all_outputs():
        if outart.type == 'ResultFile' and outart.name == 'Run Info':
            try:
                lims.upload_new_file(outart, "{}/RunInfo.xml".format(run_dir))
            except:
                try:
                    lims.upload_new_file(outart, "{}/runInfo.xml".format(run_dir))
                except:
                    sys.stderr.write("No RunInfo.xml found")
                    sys.exit(2)
        elif outart.type == 'ResultFile' and outart.name == 'Run Parameters':
            try:
                lims.upload_new_file(outart, "{}/RunParameters.xml".format(run_dir))
            except:
                try:
                    lims.upload_new_file(outart, "{}/runParameters.xml".format(run_dir))
                except:
                    sys.stderr.write("No RunParameters.xml found")
                    sys.exit(2)


def lims_for_nextseq(process, run_dir):
    # Parse run
    runParserObj, RunParametersParserObj = parse_run(run_dir)
    # Attach RunInfo.xml and RunParamters.xml
    attach_xml(process, run_dir)
    # Write info in LIMS UDFs
    process.udf['Finish Date'] = datetime.strptime(RunParametersParserObj.data['RunParameters']['RunEndTime'][:10],"%Y-%m-%d").date() if 'RunEndTime' in RunParametersParserObj.data['RunParameters'].keys() and RunParametersParserObj.data['RunParameters']['RunEndTime'] != '' else datetime.now().date()
    process.udf['Run Type'] = "NextSeq 2000 {}".format(RunParametersParserObj.data['RunParameters']['FlowCellMode'].split(' ')[2])
    process.udf['Chemistry'] = "NextSeq 2000 {}".format(RunParametersParserObj.data['RunParameters']['FlowCellMode'].split(' ')[2])
    planned_cycles = sum(list(map(int, RunParametersParserObj.data['RunParameters']['PlannedCycles'].values())))
    completed_cycles = sum(list(map(int, RunParametersParserObj.data['RunParameters']['CompletedCycles'].values())))
    process.udf['Status'] = "Cycle {} of {}".format(completed_cycles, planned_cycles)
    process.udf['Flow Cell ID'] = RunParametersParserObj.data['RunParameters']['FlowCellSerialNumber']
    process.udf['Experiment Name'] = RunParametersParserObj.data['RunParameters']['FlowCellSerialNumber']
    process.udf['Read 1 Cycles'] = int(RunParametersParserObj.data['RunParameters']['PlannedCycles']['Read1'])
    process.udf['Index 1 Read Cycles'] = int(RunParametersParserObj.data['RunParameters']['PlannedCycles']['Index1'])
    process.udf['Index 2 Read Cycles'] = int(RunParametersParserObj.data['RunParameters']['PlannedCycles']['Index2'])
    process.udf['Read 2 Cycles'] = int(RunParametersParserObj.data['RunParameters']['PlannedCycles']['Read2'])
    process.udf['Run ID'] = runParserObj.runinfo.data['Id']
    process.udf['Reagent Cartridge ID'] = RunParametersParserObj.data['RunParameters']['CartridgeSerialNumber']
    process.put()


def lims_for_miseq(process, run_dir):
    # Parse run
    runParserObj, RunParametersParserObj = parse_run(run_dir)
    # Write info in LIMS UDFs
    process.udf['Finish Date'] = datetime.now().date()
    if RunParametersParserObj.data['RunParameters']['Setup']['SupportMultipleSurfacesInUI'] == 'true' and unParametersParserObj.data['RunParameters']['Setup']['NumTilesPerSwath'] == '19':
        process.udf['Run Type'] = 'Version3'
    elif RunParametersParserObj.data['RunParameters']['Setup']['SupportMultipleSurfacesInUI'] == 'true' and unParametersParserObj.data['RunParameters']['Setup']['NumTilesPerSwath'] == '14':
        process.udf['Run Type'] = 'Version2'
    elif RunParametersParserObj.data['RunParameters']['Setup']['SupportMultipleSurfacesInUI'] == 'false' and unParametersParserObj.data['RunParameters']['Setup']['NumTilesPerSwath'] == '2':
        process.udf['Run Type'] = 'Version2Nano'
    else:
        process.udf['Run Type'] = 'null'
    total_cycles = sum(list(map(int, [read['NumCycles'] for read in RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead']])))
    process.udf['Status'] = "Cycle {} of {}".format(total_cycles, total_cycles)
    process.udf['Flow Cell ID'] = RunParametersParserObj.data['RunParameters']['FlowcellRFIDTag']['SerialNumber']
    process.udf['Flow Cell Version'] = RunParametersParserObj.data['RunParameters']['FlowcellRFIDTag']['PartNumber']
    process.udf['Experiment Name'] = process.all_inputs()[0].name

    non_index_read_idx = [read['Number'] for read in RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead'] if read['IsIndexedRead'] == 'N']
    index_read_idx = [read['Number'] for read in RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead'] if read['IsIndexedRead'] == 'Y']
    if len(non_index_read_idx) == 2:
        process.udf['Read 1 Cycles'] = int(list(filter(lambda read: read['Number'] == str(min(list(map(int,non_index_read_idx)))), RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead']))[0]['NumCycles'])
        process.udf['Read 2 Cycles'] = int(list(filter(lambda read: read['Number'] == str(max(list(map(int,non_index_read_idx)))), RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead']))[0]['NumCycles'])
    elif len(non_index_read_idx) == 1:
        process.udf['Read 1 Cycles'] = int(list(filter(lambda read: read['Number'] == str(min(list(map(int,non_index_read_idx)))), RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead']))[0]['NumCycles'])

    if len(index_read_idx) == 2:
        process.udf['Index 1 Read Cycles'] = int(list(filter(lambda read: read['Number'] == str(min(list(map(int,index_read_idx)))), RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead']))[0]['NumCycles'])
        process.udf['Index 2 Read Cycles'] = int(list(filter(lambda read: read['Number'] == str(max(list(map(int,index_read_idx)))), RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead']))[0]['NumCycles'])
    elif len(index_read_idx) == 1:
        process.udf['Index 1 Read Cycles'] = int(list(filter(lambda read: read['Number'] == str(min(list(map(int,index_read_idx)))), RunParametersParserObj.data['RunParameters']['Reads']['RunInfoRead']))[0]['NumCycles'])

    process.udf['Run ID'] = RunParametersParserObj.data['RunParameters']['RunID']
    process.udf['Output Folder'] = RunParametersParserObj.data['RunParameters']['OutputFolder'].replace(RunParametersParserObj.data['RunParameters']['RunID'], '')
    process.udf['Reagent Cartridge ID'] = RunParametersParserObj.data['RunParameters']['ReagentKitRFIDTag']['SerialNumber']
    process.udf['Reagent Cartridge Part #'] = RunParametersParserObj.data['RunParameters']['ReagentKitRFIDTag']['PartNumber']
    process.udf['PR2 Bottle ID'] = RunParametersParserObj.data['RunParameters']['PR2BottleRFIDTag']['SerialNumber']
    process.udf['Chemistry'] = RunParametersParserObj.data['RunParameters']['Chemistry']
    process.udf['Workflow'] = RunParametersParserObj.data['RunParameters']['Workflow']['Analysis']


def main(lims, args):
    log = []
    process = Process(lims, id=args.pid)

    if process.type.name == 'Illumina Sequencing (NextSeq) v1.0':
        run_type =  'nextseq'
    elif process.type.name == 'MiSeq Run (MiSeq) 4.0':
        run_type = 'miseq'

    # Fetch FC ID
    fc_id = fetch_fc(process)

    # Fetch run dir
    run_dir = fetch_rundir(fc_id, run_type)

    # Fill info in LIMS
    if run_type == 'nextseq':
        lims_for_nextseq(process, run_dir)
    elif run_type == 'miseq':
        lims_for_miseq(process, run_dir)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
