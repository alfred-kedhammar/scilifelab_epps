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


DESC = """EPP for parsing run paramters for NextSeq"""


def fetch_fc(process):
    fc_id = ''
    if process.parent_processes()[0].type.name == 'Load to Flowcell (NextSeq v1.0)':
        if process.parent_processes()[0].udf['Experiment Name']:
            fc_id = process.parent_processes()[0].udf['Experiment Name']
        else:
            sys.stderr.write("Experiment Name is empty in the associated Load to Flowcell (NextSeq v1.0) step.")
            sys.exit(2)
    else:
        sys.stderr.write("No associated Load to Flowcell (NextSeq v1.0) step can be found.")
        sys.exit(2)
    return fc_id


def fetch_rundir(fc_id):
    run_dir = ''
    run_dir_path = os.path.join(os.sep,"srv","mfs","NextSeq_data","*{}".format(fc_id))
    if len(glob.glob(run_dir_path)) == 1:
        run_dir = glob.glob(run_dir_path)[0]
    elif len(glob.glob(run_dir_path)) == 0:
        sys.stderr.write("No run dir can be found for FC {}".format(fc_id))
        sys.exit(2)
    else:
        sys.stderr.write("Multiple run dirs found for FC {}".format(fc_id))
        sys.exit(2)
    return run_dir


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


def main(lims, args):
    log = []
    process = Process(lims, id=args.pid)

    # Fetch FC ID
    fc_id = fetch_fc(process)

    # Fetch run dir
    run_dir = fetch_rundir(fc_id)

    # Attach RunInfo.xml and RunParamters.xml
    attach_xml(process, run_dir)

    # Parse run
    runParserObj, RunParametersParserObj = parse_run(run_dir)

    # Write info in LIMS UDFs
    process.udf['Finish Date'] = datetime.strptime(RunParametersParserObj.data['RunParameters']['RunEndTime'][:10],"%Y-%m-%d").date()
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


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
