#!/usr/bin/env python
DESC="""EPP script to copy the "Comments" field to the projects running notes on process termination

Denis Moreno, Science for Life Laboratory, Stockholm, Sweden
"""
from argparse import ArgumentParser
from genologics.entities import *
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from scilifelab_epps.epp import attach_file, EppLogger

import datetime
import logging
import os
import sys
import json


def categorization(process_name):
    decision={
    "Aggregate QC (DNA) 4.0" : "Workset",
    "Aggregate QC (Library Validation) 4.0" : "Workset",
    "Aggregate QC (RNA) 4.0" : "Workset",
    "Aliquot Libraries for Hybridization (SS XT)" : "",
    "Aliquot Libraries for Pooling (Small RNA)" : "",
    "Aliquot Samples for Caliper/Bioanalyzer" : "Workset",
    "Aliquot Samples for Qubit/Bioanalyzer" : "Workset",
    "Amplification and Purification" : "Workset",
    "Amplify Adapter-Ligated Library (SS XT) 4.0" : "",
    "Amplify Captured Libraries to Add Index Tags (SS XT) 4.0" : "",
    "Amplify by PCR and Add Index Tags (TruSeq small RNA) 1.0" : "Workset",
    "Applications Generic Process" : "Workset",
    "Applications Indexing" : "Workset",
    "Applications Pre-Pooling" : "",
    "Automated Quant-iT QC (DNA) 4.0" : "Workset",
    "Automated Quant-iT QC (Library Validation) 4.0" : "Workset",
    "Bcl Conversion & Demultiplexing (Illumina SBS) 4.0" : "Bioinformatics",
    "Bioanalyzer Fragmentation QC (TruSeq DNA) 4.0" : "Workset",
    "Bioanalyzer QC (DNA) 4.0" : "Workset",
    "Bioanalyzer QC (Library Validation) 4.0" : "Workset",
    "Bioanalyzer QC (RNA) 4.0" : "Workset",
    "CA Purification" : "Workset",
    "CaliperGX QC (DNA)" : "Workset",
    "CaliperGX QC (RNA)" : "Workset",
    "Capture And Wash (SS XT) 4.0" : "Workset",
    "Chromatin capture, digestion, end ligation and crosslink reversal (HiC) 1.0" : "",
    "Circularization" : "Workset",
    "Cluster Generation (HiSeq X) 1.0" : "Flowcell",
    "Cluster Generation (Illumina SBS) 4.0" : "Flowcell",
    "Customer Gel QC" : "Workset",
    "Denature, Dilute and Load Sample (MiSeq) 4.0" : "Flowcell",
    "End Repair, A-Tailing and Adapter Ligation (SS XT) 4.0" : "Workset",
    "End repair, A-tailing and adapter ligation (Nextera) 4.0" : "Workset",
    "End repair, A-tailing and adapter ligation (TruSeq RNA) 4.0" : "Workset",
    "End repair, adapter ligation, ligation capture and Index PCR (HiC)" : "",
    "End repair, size selection, A-tailing and adapter ligation (TruSeq PCR-free DNA) 4.0" : "Workset",
    "Enrich DNA fragments (TruSeq RNA) 4.0" : "Workset",
    "Fragment DNA (ThruPlex)" : "Workset",
    "Fragment DNA (TruSeq DNA) 4.0" : "Workset",
    "Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : "Workset",
    "Generic QC" : "",
    "HT-End repair, A-tailing and adapter ligation (TruSeq RNA) 4.0" : "Workset",
    "HiC Intermediate QC" : "",
    "Hybridize Library  (SS XT) 4.0" : "",
    "Illumina Sequencing (HiSeq X) 1.0" : "Flowcell",
    "Illumina Sequencing (Illumina SBS) 4.0" : "Flowcell",
    "Illumina Sequencing (NextSeq) v1.0" : "Flowcell",
    "Library Normalization (Illumina SBS) 4.0" : "",
    "Library Normalization (MiSeq) 4.0" : "",
    "Library Normalization (NextSeq) v1.0" : "",
    "Library Normalization (NovaSeq) v2.0" : "",
    "Library Pooling (Finished Libraries) 4.0" : "",
    "Library Pooling (Illumina SBS) 4.0" : "",
    "Library Pooling (MiSeq) 4.0" : "",
    "Library Pooling (NextSeq) v1.0" : "",
    "Library Pooling (TruSeq Small RNA) 1.0" : "",
    "Ligate 3' adapters (TruSeq small RNA) 1.0" : "Workset",
    "Ligate 5' adapters (TruSeq small RNA) 1.0" : "Workset",
    "Linear DNA digestion, Circularized DNA shearing and Streptavidin Bead Binding" : "Workset",
    "Load to Flowcell (NextSeq v1.0)" : "" ,
    "mRNA Purification, Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : "Workset",
    "MinElute Purification" : "",
    "MiSeq Run (MiSeq) 4.0" : "Flowcell",
    "NeoPrep Library QC v1.0" : "Workset",
    "Pre-Pooling (Illumina SBS) 4.0" : "",
    "Pre-Pooling (MiSeq) 4.0" : "",
    "Pre-Pooling (NextSeq) v1.0" : "",
    "Pre-Pooling (NovaSeq) v2.0" : "",
    "Project Summary 1.3" : "",
    "Purification (ThruPlex)" : "Workset",
    "qPCR QC (Dilution Validation) 4.0" : "",
    "qPCR QC (Library Validation) 4.0" : "",
    "Quant-iT QC (DNA) 4.0" : "Workset",
    "Quant-iT QC (Library Validation) 4.0" : "Workset",
    "Quant-iT QC (RNA) 4.0" : "Workset",
    "Qubit QC (DNA) 4.0" : "Workset",
    "Qubit QC (Dilution Validation) 4.0" : "Workset",
    "Qubit QC (Library Validation) 4.0" : "Workset",
    "Qubit QC (RNA) 4.0" : "Workset",
    "RAD-seq Library Indexing v1.0" : "",
    "Reverse Transcribe (TruSeq small RNA) 1.0" : "Workset",
    "RiboZero depletion" : "Workset",
    "Sample Crosslinking" : "",
    "Sample Placement (Size Selection)" : "",
    "Selection, cDNA Synthesis and Library Construction" : "Workset",
    "Setup Workset/Plate" : "Workset",
    "Shear DNA (SS XT) 4.0" : "Workset",
    "Size Selection (Caliper XT) 1.0" : "Workset",
    "Size Selection (Pippin)" : "Workset",
    "Size Selection (Robocut)" : "Workset",
    "Sort HiSeq Samples (HiSeq) 4.0" : "",
    "Sort HiSeq X Samples (HiSeq X) 1.0" : "",
    "Sort MiSeq Samples (MiSeq) 4.0" : "",
    "Sort NextSeq Samples (NextSeq) v1.0" : "",
    "Sort NovaSeq Samples (NovaSeq) v2.0" : "",
    "Tagmentation, Strand displacement and AMPure purification" : "Workset",
    "Tissue Extraction" : "",
    "Tissue QC" : "",
    "ThruPlex library amplification" : "Workset",
    "ThruPlex template preparation and synthesis" : "Workset",
    "Volume Measurement QC" : "Workset" }

    return decision[process_name]



def main(lims, args):


    comment=False

    noteobj={}
    pro=Process(lims, id=args.pid)
    if 'Comments' in pro.udf and pro.udf['Comments'] is not '':
        key=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        noteobj[key]={}
        note="Comment from {0} ({1}) : \n{2}".format(pro.type.name, '[LIMS](https://genologics.scilifelab.se/clarity/work-details/{0})'.format(pro.id.split('-')[1]), pro.udf['Comments'].encode('utf-8'))
        noteobj[key]['note']=note
        noteobj[key]['user']="{0} {1}".format(pro.technician.first_name,pro.technician.last_name)
        noteobj[key]['email']=pro.technician.email
        noteobj[key]['category']=categorization(pro.type.name)

        #find the correct projects.
        samples=set()
        projects=set()
        for inp in pro.all_inputs():
            #bitwise or to add inp.samples to samplesas a set
            samples |= set(inp.samples)
        for sam in samples:
            if sam.project:
                projects.add(sam.project)

        for proj in projects:
            if 'Running Notes' in proj.udf:
                existing_notes=json.loads(proj.udf['Running Notes'])
                for key in noteobj:
                    existing_notes[key]=noteobj[key]
                proj.udf['Running Notes']=json.dumps(existing_notes)
            else:
                proj.udf['Running Notes']=json.dumps(noteobj)
            proj.put()

if __name__=="__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    main(lims, args)
