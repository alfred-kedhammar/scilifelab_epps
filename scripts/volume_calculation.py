#!/usr/bin/env python

import os
import sys
import yaml
import psycopg2
from argparse import ArgumentParser
from datetime import datetime
from genologics.lims import Lims
from genologics.entities import Process
from genologics.config import BASEURI, USERNAME, PASSWORD

DESC = """EPP for calculating volume for the OmniC protocol"""

factors = {'ng/ul': 1, 'ug/ul': 0.001, 'mg/ul': 0.000001, 'ng/ml': 1000, 'ug/ml': 1, 'mg/ml': 0.001}

with open("/opt/gls/clarity/users/glsai/config/genosqlrc.yaml", "r") as f:
    config = yaml.safe_load(f)


def verify_inputs(process, value_list):
    message = []
    for inp in process.all_inputs():
        for val in value_list:
            if not inp.udf.get(val):
                message.append("ERROR: Unknown {} for sample {}.".format(val, inp.name))
            elif val == 'Conc. Units' and inp.udf[val].lower() not in ['ng/ul', 'ug/ul', 'mg/ul', 'ng/ml', 'ug/ml', 'mg/ml']:
                message.append("ERROR: Unsupported {} for sample {}.".format(val, inp.name))
    return message


def calculate_volume_limsapi(process):

    message = verify_inputs(process, ['Concentration', 'Conc. Units', 'Amount (ng)'])
    if message:
        sys.stderr.write('; '.join(message)+ '\n')
        sys.exit(2)

    for art_tuple in process.input_output_maps:
        input = art_tuple[0]['uri']
        output = art_tuple[1]['uri']
        if input.type == 'Analyte' and output.type == 'Analyte':
            if output.udf.get('Amount taken (ng)'):
                if input.udf['Amount (ng)'] >= output.udf['Amount taken (ng)']:
                    factor = factors[input.udf['Conc. Units'].lower()]
                    output.udf['Volume to take (uL)'] = output.udf['Amount taken (ng)']/input.udf['Concentration']*factor
                    output.put()
                else:
                    sys.stderr.write("Insufficient Amount taken (ng) defined for sample {}.".format(output.name) + '\n')
                    sys.exit(2)
            else:
                sys.stderr.write("Amount taken (ng) not defined for sample {}.".format(output.name) + '\n')
                sys.exit(2)


def calculate_volume_postgres(process):

    connection = psycopg2.connect(user=config['username'], host=config['url'],database=config['db'], password=config['password'])
    cursor = connection.cursor()

    query = ('select pro.processid, art.name, aus.numeric0, aus.text0 '
                'from process pro '
                'inner join processtype pt on pt.typeid=pro.typeid '
                'inner join processiotracker pit on pit.processid=pro.processid '
                'inner join artifact art on art.artifactid=pit.inputartifactid '
                'inner join outputmapping opm on opm.trackerid=pit.trackerid '
                'inner join artifact art2 on art2.artifactid=opm.outputartifactid '
                'inner join artifactudfstorage aus on aus.artifactid=art2.artifactid '
                'inner join processoutputtype pot on pot.processtypeid = pt.typeid AND pot.typeid = art2.processoutputtypeid '
                'where pt.displayname=\'Intermediate QC\' '
                'and art2.name=\'Qubit Measurement\' '
                'and pot.displayname = \'ResultFile\' '
                'and aus.numeric0 is not NULL '
                'and aus.text0 is not NULL '
                'and art.luid=\'{}\';')

    for art_tuple in process.input_output_maps:
        input = art_tuple[0]['uri']
        output = art_tuple[1]['uri']
        if input.type == 'Analyte' and output.type == 'Analyte':
            cursor.execute(query.format(input.id))
            query_output = cursor.fetchall()
            if len(query_output) == 1:
                sample_id = query_output[0][1]
                conc = query_output[0][2]
                conc_unit = query_output[0][3]
            # When there are more than 1 query results found, use the latest values
            elif len(query_output) > 1:
                sample_id = max(query_output, key=lambda tup:tup[0])[1]
                conc = max(query_output, key=lambda tup:tup[0])[2]
                conc_unit = max(query_output, key=lambda tup:tup[0])[3]
            # No concentration could be found
            else:
                sys.stderr.write("No measurement found for sample {}.".format(output.name) + '\n')
                sys.exit(2)
            # Calculation
            if output.udf.get('Amount taken (ng)'):
                if conc and conc_unit.lower() in factors.keys():
                    factor = factors[conc_unit.lower()]
                    output.udf['Volume to take (uL)'] = output.udf['Amount taken (ng)']/conc*factor
                    output.put()
                else:
                    sys.stderr.write("Invalid conc or conc unit for sample {}.".format(output.name) + '\n')
                    sys.exit(2)
            else:
                sys.stderr.write("Amount taken (ng) not defined for sample {}.".format(output.name) + '\n')
                sys.exit(2)


def main(lims, pid):
    process = Process(lims, id = pid)

    art_workflows = set()
    for inp in process.all_inputs():
        for stage in inp.workflow_stages_and_statuses:
            if stage[1] == 'IN_PROGRESS':
                art_workflows.add(stage[0].workflow.name)

    if process.type.name == 'Sample Setup':
        calculate_volume_limsapi(process)
    elif process.type.name == 'Setup Workset/Plate' and any('OmniC' in i for i in art_workflows):
        calculate_volume_postgres(process)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args.pid)
