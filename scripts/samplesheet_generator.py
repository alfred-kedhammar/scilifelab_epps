#!/usr/bin/env python
DESC="""EPP used to create csv files for the bravo robot"""

from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.entities import Process
from genologics.config import BASEURI,USERNAME,PASSWORD
from scilifelab_epps.epp import attach_file, EppLogger
import logging
import os
import sys
import re

def gen_X_header(pro):
    header="[Header]\nInvestigator Name,{}\nDate,{}\n".format(pro.technician.name, pro.date_run)
    if "Experiment Name" in pro.udf:
        header= header + "Experiment Name,{}\n".format(pro.udf["Experiment Name"])
    return header
def gen_X_reads_info(pro):
    reads=[]
    if "Read 1 Cycles" in pro.udf:
        reads.append(str(pro.udf["Read 1 Cycles"]))
        if "Read 2 Cycles" in pro.udf:
            reads.append(str(pro.udf["Read 2 Cycles"]))
        return "[Reads]\n{}\n".format("\n".join(reads))

    else:
        return None

def gen_X_lane_data(pro):
    data=[]
    single_end=True
    for inp in pro.all_inputs():
        for sample in inp.samples:
            sp_obj={}
            sp_obj['lane']=inp.location[1].split(':')[0]
            sp_obj['sid']="Sample_{}".format(sample.name)
            sp_obj['sn']=sample.name
            sp_obj['pj']=sample.project.name
            sp_obj['fc']=inp.location[0].name
            sp_obj['sw']=inp.location[1]
            idxs=find_barcode(sample, pro)
            sp_obj['idx1']=idxs[0]
            try:
                sp_obj['idx2']=idxs[1]
                single_end=False
            except KeyError:
                sp_obj['idx2']=''

            data.append(sp_obj)

    header_ar=["Lane", "SampleID", "SampleName", "SamplePlate", "SampleWell", "index", "Project", "Description"]
    if not single_end:
        header_ar.insert(6, "index2")

    header="[Data]\n{}\n".format(",".join(header_ar))
    str_data=""
    for line in sorted(data, key=lambda x:x['lane']):
        l_data=[line['lane'], line['sid'], line['sn'], line['fc'], line['sw'], line['idx1'], line['pj'], '']
        if not single_end:
            l_data.insert(6, line['idx2'])

        str_data = str_data + ",".join(l_data) + "\n"
        

    return "{}{}".format(header, str_data)

        
def find_barcode(sample, process):
    #print "trying to find {} barcode in {}".format(sample.name, process.name)
    idx_pat=re.compile("([ATCG]+)-?([ATCG]*)")
    for art in process.all_inputs():
        if sample in art.samples:
            if len(art.samples)==1 and art.reagent_labels:
                idxs=idx_pat.findall(art.reagent_labels[0])[0]
                return idxs
            else:
                if art == sample.artifact:
                    return None
                else:
                    return find_barcode(sample, art.parent_process)



    

def main(lims, args):
    content=None
    process = Process(lims, id=args.pid)
    if process.type.name == 'Cluster Generation (HiSeq X) 1.0':
        header=gen_X_header(process)
        reads=gen_X_reads_info(process)
        data=gen_X_lane_data(process)
        content="{}{}{}".format(header, reads, data)

        if not args.test:
            for out in process.all_outputs():
                if out.name =="bcl2fastq Sample Sheet":
                    ss_rfid=out.id
                elif out.type=="Analyte":
                    fc_name=out.location[0].name

            with open("{}_{}.csv".format(ss_rfid, fc_name), "w") as f:
                f.write(content)






if __name__=="__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    parser.add_argument('--test', action="store_true",
                        help='do not upload the samplesheet')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args)
