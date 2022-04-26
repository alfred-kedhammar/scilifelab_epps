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

QC_criteria_json = '/home/glsai/repos/scilifelab_epps/data/QC_criteria.json'

with open(QC_criteria_json,'r') as file:
    QC_criteria = json.loads(file.read())

# Prepare a table with all sample details
def prepare_sample_table(artifacts):
    sample_table = {}
    key = 1;
    for art in artifacts:
        try:
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
        # ignore control samples
        except AttributeError:
            pass
    return sample_table


def verify_sample_table(sample_table, library=False):
    error_message = []
    measurements_keys = set()
    optional_keys = {'Amount (fmol)', 'Dilution Fold', 'Failure Reason', 'Max Size (bp)', 'Min Size (bp)', 'Rerun', 'Size (bp)'}
    for k, v in sample_table.items():
        # Prepare a set of all existing measurement keys
        measurements_keys |= set(v['measurements'].keys())
        # Check samples with unknown QC flag
        if v['qc_flag'] not in ['PASSED', 'FAILED']:
            error_message.append('Sample {} is missing QC flag!'.format(v['name']))
    # No measurement is filled in
    if len(measurements_keys) == 0:
        error_message.append('No measurement is available!')
    else:
        # Remove the UDFs that should skip checking
        if measurements_keys & optional_keys:
            measurements_keys -= optional_keys
    # Check value for each measurement
    for measurement in measurements_keys:
        for k, v in sample_table.items():
            # Check missing value
            if measurement not in v['measurements'].keys():
                error_message.append('Sample {} is missing {}!'.format(v['name'], measurement))
            else:
                # Verify concentration unit
                if measurement == 'Conc. Units':
                    if (library and v['measurements'][measurement] != 'nM') or (not library and v['measurements'][measurement] not in ['ng/ul', 'ng/uL']):
                        error_message.append('Sample {} has a wrong concentration unit!'.format(v['name']))
    return error_message


def prepare_QC_details(project, sample_table, library=False):
    QC_details = ''
    QC_metrics = {}
    # Retrieve the QC metrics from the QC critera file
    library_construction_method = project.udf.get('Library construction method')
    if library_construction_method in QC_criteria.keys():
        if not library:
            library_prep_option = project.udf.get('Library prep option') if project.udf.get('Library prep option') else 'default'
            if library_prep_option in QC_criteria[library_construction_method].keys():
                QC_metrics = QC_criteria[library_construction_method][library_prep_option]
        elif library and library_construction_method=='Finished library (by user)':
            sequencing_platform = project.udf.get('Sequencing platform')
            if sequencing_platform in QC_criteria[library_construction_method].keys():
                flowcell = project.udf.get('Flowcell')
                flowcell_type = flowcell.split('-')[0] if flowcell else 'default'
                if flowcell_type in QC_criteria[library_construction_method][sequencing_platform].keys():
                    flowcell_option = project.udf.get('Flowcell option') if project.udf.get('Flowcell option') else 'default'
                    if flowcell_option in QC_criteria[library_construction_method][sequencing_platform][flowcell_type].keys():
                        QC_metrics = QC_criteria[library_construction_method][sequencing_platform][flowcell_type][flowcell_option]
    # Decide QC status on individual metrix
    filtered_sample_table = {k: v for k, v in sample_table.items() if v['project'] == project.id}
    if QC_metrics:
        # Start working on QC metrics
        conc_units = set()
        for k, v in QC_metrics.items():
            low_threshold = 0
            high_threshold = 0
            lower_than_threshold_counter = 0
            higher_than_threshold_counter = 0
            for k1, v1 in filtered_sample_table.items():
                conc_units.add(v1['measurements']['Conc. Units'])
                if k in v1['measurements'].keys():
                    value = v1['measurements'].get(k)
                    if isinstance(v, list):
                        low_threshold = v[0]
                        high_threshold = v[1]
                        if value < low_threshold:
                            lower_than_threshold_counter += 1
                        elif value > high_threshold:
                            higher_than_threshold_counter += 1
                    else:
                        low_threshold = v
                        if value < low_threshold:
                            lower_than_threshold_counter += 1
            conc_unit = list(conc_units)[0] if k=='Concentration' else ''
            if lower_than_threshold_counter != 0:
                QC_details += '**{}**: {} samples lower than threshold {}{}.\n'.format(k, lower_than_threshold_counter, low_threshold, conc_unit)
            if higher_than_threshold_counter != 0:
                QC_details += '**{}**: {} samples higher than threshold {}{}.\n'.format(k, higher_than_threshold_counter, high_threshold, conc_unit)
    return QC_details


# Prepare the summary text
def make_summary(lims, process, sample_table, library):
    summary = {}
    # Prepare project list
    projects = set()
    for k, v in sample_table.items():
        projects.add(v['project'])
    # Prepare summary for each project
    for proj in projects:
        comments = ''
        qc_flag_by_container = []
        project = Project(lims,id=proj)
        for k, v in sample_table.items():
            if v['project'] == proj:
                qc_flag_by_container.append((v['container'], v['qc_flag']))
        total_sample_number = len(qc_flag_by_container)
        passed_sample_number = len([i for i in qc_flag_by_container if i[1] == 'PASSED'])
        containers = list(set(i[0] for i in qc_flag_by_container))
        comments += '**Overall QC summary: {}/{} samples passed QC in container {}**.\n'.format(passed_sample_number, total_sample_number, ','.join(sorted(containers)))
        QC_details_all_samples = prepare_QC_details(project, sample_table, library)
        if QC_details_all_samples != '':
            comments += '\n**QC details for all samples: **\n'
            comments += QC_details_all_samples
        if len(containers) > 1:
            comments += '\n\n'
            for container in sorted(containers):
                total_sample_number_by_container = len([i for i in qc_flag_by_container if i[0]==container])
                passed_sample_number_by_container = len([i for i in qc_flag_by_container if i[0]==container and i[1]=='PASSED'])
                comments += '\nContainer **{}**: {}/{} samples passed QC.\n'.format(container, passed_sample_number_by_container, total_sample_number_by_container)
                container_sample_table = {k: v for k, v in sample_table.items() if v['container'] == container}
                QC_details_per_container = prepare_QC_details(project, container_sample_table, library)
                if QC_details_per_container != '':
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
    library = True if pro.type.name in ['Aggregate QC (Library Validation) 4.0'] else False

    artifacts = pro.all_inputs(unique=True)
    sample_table = prepare_sample_table(artifacts)
    error_message = verify_sample_table(sample_table, library)
    if error_message:
        sys.exit(' '.join(error_message))
    summary = make_summary(lims, pro, sample_table, library)

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
