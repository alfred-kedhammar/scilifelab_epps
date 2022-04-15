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
from data.QC_criteria import QC_criteria


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


def prepare_QC_details(lims, process, proj, sample_table):
    QC_details = ''
    QC_metrics = {}
    library = False
    # Retrieve the QC metrics from the QC critera file
    project = Project(lims,id=proj)
    library_construction_method = project.udf.get('Library construction method')
    if library_construction_method in QC_criteria.keys():
        if process.type.name in ['Aggregate QC (RNA) 4.0', 'Aggregate QC (DNA) 4.0']:
            library = False
            library_prep_option = project.udf.get('Library prep option') if project.udf.get('Library prep option') else 'default'
            if library_prep_option in QC_criteria[library_construction_method].keys():
                QC_metrics = QC_criteria[library_construction_method][library_prep_option]
        elif process.type.name in ['Aggregate QC (Library Validation) 4.0'] and library_construction_method=='Finished library (by user)':
            library = True
            sequencing_platform = project.udf.get('Sequencing platform')
            if sequencing_platform in QC_criteria[library_construction_method].keys():
                flowcell = project.udf.get('Flowcell')
                flowcell_type = flowcell.split('-')[0] if flowcell else 'default'
                if flowcell_type in QC_criteria[library_construction_method][sequencing_platform].keys():
                    flowcell_option = project.udf.get('Flowcell option') if project.udf.get('Flowcell option') else 'default'
                    if flowcell_option in QC_criteria[library_construction_method][sequencing_platform][flowcell_type].keys():
                        QC_metrics = QC_criteria[library_construction_method][sequencing_platform][flowcell_type][flowcell_option]
    # Decide QC status on individual metrix
    filtered_sample_table = {k: v for k, v in sample_table.items() if v['project'] == proj}
    if QC_metrics:
        # Check concentration units
        conc_units = set()
        for k, v in filtered_sample_table.items():
            conc_units.add(v['measurements']['Conc. Units'])
        if library:
            if any(i != 'nM' for i in list(conc_units)):
                sys.exit("Wrong concentration unit detected!")
        else:
            if any(i not in ['ng/ul', 'ng/uL'] for i in list(conc_units)):
                sys.exit("Wrong concentration unit detected!")
        # Start working on QC metrics
        for k, v in QC_metrics.items():
            low_theshold = 0
            high_theshold = 0
            lower_than_theshold_counter = 0
            higher_than_theshold_counter = 0
            for k1, v1 in filtered_sample_table.items():
                if k in v1['measurements'].keys():
                    value = v1['measurements'].get(k)
                    if isinstance(v, tuple):
                        low_theshold = v[0]
                        high_theshold = v[1]
                        if value and value < low_theshold:
                            lower_than_theshold_counter += 1
                        elif value and value > high_theshold:
                            higher_than_theshold_counter += 1
                    else:
                        low_theshold = v
                        if value and value < low_theshold:
                            lower_than_theshold_counter += 1
            conc_unit = list(conc_units)[0] if k=='Concentration' else ''
            if lower_than_theshold_counter != 0:
                QC_details += '**{}**: {} samples lower than theshold {}{}.\n'.format(k, lower_than_theshold_counter, low_theshold, conc_unit)
            if higher_than_theshold_counter != 0:
                QC_details += '**{}**: {} samples higher than theshold {}{}.\n'.format(k, higher_than_theshold_counter, high_theshold, conc_unit)
    return QC_details


# Prepare the summary text
def make_summary(lims, process, sample_table):
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
        containers = list(set(i[0] for i in qc_flag_by_container))
        comments += '**Overall QC summary: {}/{} samples passed QC in container {}**.\n'.format(passed_sample_number, total_sample_number, ','.join(containers))
        QC_details_all_samples = prepare_QC_details(lims, process, proj, sample_table)
        if QC_details_all_samples != '':
            comments += '\n**QC details for all samples: **\n'
            comments += QC_details_all_samples
        if len(containers) > 1:
            comments += '\n\n'
            for container in containers:
                total_sample_number_by_container = len([i for i in qc_flag_by_container if i[0]==container])
                passed_sample_number_by_container = len([i for i in qc_flag_by_container if i[0]==container and i[1]=='PASSED'])
                comments += '\nContainer **{}**: {}/{} samples passed QC.\n'.format(container, passed_sample_number_by_container, total_sample_number_by_container)
                container_sample_table = {k: v for k, v in sample_table.items() if v['container'] == container}
                QC_details_per_container = prepare_QC_details(lims, process, proj, container_sample_table)
                if QC_details != '':
                    comments += '\nQC details for container **{}**: \n'.format(container)
                    comments += QC_details_per_container
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
    summary = make_summary(lims, pro, sample_table)

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
