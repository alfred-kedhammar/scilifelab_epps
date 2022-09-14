#!/usr/bin/env python

import os
import sys

from argparse import ArgumentParser
from datetime import datetime
from genologics.lims import Lims
from genologics.entities import Process
from genologics.config import BASEURI, USERNAME, PASSWORD

DESC = """EPP for calculating volume"""

factors = {'ng/ul': 1, 'ug/ul': 0.001, 'mg/ul': 0.000001, 'ng/ml': 1000, 'ug/ml': 1, 'mg/ml': 0.001}


def verify_inputs(process, value_list):
    message = []
    for inp in process.all_inputs():
        for val in value_list:
            if not inp.udf.get(val):
                message.append("ERROR: Unknown {} for sample {}.".format(val, inp.name))
            elif val == 'Conc. Units' and inp.udf[val] not in ['ng/ul', 'ug/ul', 'mg/ul', 'ng/ml', 'ug/ml', 'mg/ml']:
                message.append("ERROR: Unsupported {} for sample {}.".format(val, inp.name))
    return message


def main(lims, pid):
    process = Process(lims, id = pid)

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
                    factor = factors[input.udf['Conc. Units']]
                    output.udf['Volume to take (uL)'] = output.udf['Amount taken (ng)']/input.udf['Concentration']*factor
                    output.put()
                else:
                    sys.stderr.write("Insufficient Amount taken (ng) defined for sample {}.".format(output.name) + '\n')
                    sys.exit(2)
            else:
                sys.stderr.write("Amount taken (ng) not defined for sample {}.".format(output.name) + '\n')
                sys.exit(2)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args.pid)
