
from genologics.entities import *
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.descriptors import StringDescriptor,EntityDescriptor

import xml.etree.ElementTree as ET

from argparse import ArgumentParser

lims= Lims(BASEURI, USERNAME, PASSWORD)

def categorization(process_name):
    record={
        "Qubit QC (RNA) 4.0" : {"dest_file" : "Qubit_LIMS_logbook.csv", "details": {"instrument" : "", "udf_Assay" : "", "udf_Lot no: Qubit kit" : "", "zComments" : "" }}
    }
    return record[process_name]

def get_instrument(pid):
    return instrument_Name

def get_record():
    pro=Process(lims, id=args.pid)
    log=[]
    time=pro.date_run
    log.append(time)
    user="{0} {1}".format(pro.technician.first_name,pro.technician.last_name)
    log.append(user)
    record=categorization(pro.type.name)

    for key in sorted(record['details'].keys()):
        if key == "instrument":
            instrument = get_instrument(args.pid)
            log.append(instrument)
        elif key[0:3] == "udf":
            log.append(pro.udf[key[3:]])
        elif key == "zComments":
            if 'Comments' in pro.udf and pro.udf['Comments'] is not '':
                log.append(pro.udf['Comments'])

    return log

def write_record(log):
