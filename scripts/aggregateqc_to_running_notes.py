#!/usr/bin/env python
# -*- coding: utf-8 -*-
DESC="""EPP script to summarize aggregate QC results to the projects running notes

Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
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

from write_notes_to_couchdb import write_note_to_couch


# Prepare a table with all sample details
def prepare_sample_table(artifacts):
    sample_table = {}
    key = 1;
    for art in artifacts:
        name = art.name
        project = art.samples[0].project.id
        container = art.container.name
        qc_flag = art.qc_flag
        measurements = {}
        for i in art.udf.items():
            measurements.update({i[0] : i[1]})
        sample_table.update({key : {'name' : name,
                                    'project' : project,
                                    'container' : container,
                                    'qc_flag' : qc_flag,
                                    'measurements' : measurements
                                    }
                            })
        key += 1
    return sample_table


# Prepare the summary text
def make_summary(process, sample_table):
    summary = {}
    # Prepare project list
    projects = set()
    for k, v in sample_table.items():
        projects.add(v['project'])
    # Prepare summary for each project
    for proj in projects:
        comments = ''
        qc_flag_by_container = []
        for k, v in sample_table.items():
            if v['project'] == proj:
                qc_flag_by_container.append((v['container'], v['qc_flag']))
        total_sample_number = len(qc_flag_by_container)
        passed_sample_number = len([i for i in qc_flag_by_container if i[1] == 'PASSED'])
        comments += '**Overall QC summary:** {}/{} samples passed QC'.format(passed_sample_number, total_sample_number)
        containers = list(set(i[0] for i in qc_flag_by_container))
        if len(containers) == 1:
            comments += ' in container **{}**. \n'.format(containers[0])
        else:
            comments += '\n\n'
            for container in containers:
                total_sample_number_by_container = len([i for i in qc_flag_by_container if i[0]==container])
                passed_sample_number_by_container = len([i for i in qc_flag_by_container if i[0]==container and i[1]=='PASSED'])
                comments += 'Container **{}**: {}/{} samples passed QC. \n'.format(container, passed_sample_number_by_container, total_sample_number_by_container)
        noteobj = {}
        key = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        noteobj[key] = {}
        note = "Summary from {0} ({1}) : \n{2}".format(process.type.name, '[LIMS]({0}/clarity/work-details/{1})'.format(BASEURI, process.id.split('-')[1]), comments)
        noteobj[key]['note'] = note
        noteobj[key]['user'] = "{0} {1}".format(process.technician.first_name,process.technician.last_name)
        noteobj[key]['email'] = process.technician.email
        noteobj[key]['category'] = 'Lab'
        summary.update({proj: noteobj})
    return summary


def main(lims, args):

    pro = Process(lims, id=args.pid)
    artifacts = pro.all_inputs(unique=True)
    sample_table = prepare_sample_table(artifacts)
    summary = make_summary(pro, sample_table)

    # Write summary to couch
    for proj, noteobj in summary.items():
        for k, v in noteobj.items():
            write_note_to_couch(proj, k, v, lims.get_uri())


if __name__=="__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    main(lims, args)
