#!/usr/bin/env python
from __future__ import print_function
DESC="""EPP script to calculate amount in ng from concentration and volume
udf:s in Clarity LIMS. The script checks that the 'Volume (ul)' and
'Concentration' udf:s are defined and that the udf. 'Conc. Units'
 have the correct value, otherwise that artifact is skipped,
left unchanged, by the script.

Johannes Alneberg, Science for Life Laboratory, Stockholm, Sweden
"""
from argparse import ArgumentParser

from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD

from genologics.entities import Process
from scilifelab_epps.epp import EppLogger

from epp_utils import formula, udf_tools

import logging
import sys

def apply_calculations(artifacts, udf1, op, udf2, unit_amount_map, process):
    """For each result file of the process: if its corresponding inart has the udf
    'Dilution Fold', the result_udf: 'Amount (xx)' is calculated as

    'Amount (xx)' =  'Concentration'*'Volume (ul)'*'Dilution Fold'

    otherwise its calculated as

    'Amount (xx)' =  'Concentration'*'Volume (ul)'"""

    for artifact in artifacts:

        result_udf = unit_amount_map[artifact.udf['Conc. Units']]

        try:
            artifact.udf[result_udf]
        except KeyError:
            artifact.udf[result_udf] = 0

        try:
            inart = process.input_per_sample(artifact.samples[0].name)[0]
            dil_fold = inart.udf['Dilution Fold']
        except:
            dil_fold = None

        # Special calculation formula for total lysate
        if process.udf.get('Total Lysate Calculation', ''):
            udf2_value = 250
        else:
            udf2_value = artifact.udf[udf2]

        logging.info(("Updating: Artifact id: {0}, "
                     "result_udf: {1}, udf1: {2}, "
                     "operator: {3}, udf2: {4}").format(artifact.id,
                                                        artifact.udf.get(result_udf,0),
                                                        artifact.udf[udf1],
                                                        op,
                                                        udf2_value))
        prod = eval('{0}{1}{2}'.format(artifact.udf[udf1],op,udf2_value))
        if dil_fold:
            prod = eval('{0}{1}{2}'.format(prod, op, dil_fold))
        if artifact.udf['Conc. Units'] == 'pM':
            prod = eval('{0}{1}{2}'.format(prod, op, 1/1000))
        artifact.udf[result_udf] = prod

        artifact.put()

        logging.info("Updated {0} to {1}.".format(result_udf, artifact.udf[result_udf]))
        calculate_fmol_AND_ng(artifact, result_udf)


def calculate_fmol_AND_ng(art, result_udf):
    """Use ng <--> fmol conversion to populate both 'Amount (ng)' and 'Amount (fmol)' if possible."""

    size_udf = "Size (bp)"

    if udf_tools.is_filled(art, result_udf) and udf_tools.is_filled(art, size_udf):
        result_amount = art.udf[result_udf]
        size = art.udf[size_udf]

        if result_udf == "Amount (ng)":
            supplemented_udf = "Amount (fmol)"
            supplemented_amount = formula.ng_to_fmol(result_amount, size)
        elif result_udf == "Amount (fmol)":
            supplemented_udf = "Amount (ng)"
            supplemented_amount = formula.fmol_to_ng(result_amount, size)

        if udf_tools.put(art, supplemented_udf, supplemented_amount, on_fail=None):
            logging.info(
                f"Artifact {art.id} ({result_udf}: {result_amount}) was supplemented --> ({supplemented_udf}: {supplemented_amount})"
            )


def check_udf_is_defined(artifacts, udf):
    """ Filter and Warn if udf is not defined for any of artifacts. """
    filtered_artifacts = []
    incorrect_artifacts = []
    for artifact in artifacts:
        if (udf in artifact.udf):
            filtered_artifacts.append(artifact)
        else:
            logging.warning(("Found artifact for sample {0} with {1} "
                             "undefined/blank, skipping").format(artifact.samples[0].name, udf))
            incorrect_artifacts.append(artifact)
    return filtered_artifacts, incorrect_artifacts


def check_udf_has_value(artifacts, udf, value):
    """ Filter artifacts on undefined udf or if udf has wrong value. """
    filtered_artifacts = []
    incorrect_artifacts = []
    for artifact in artifacts:
        if udf in artifact.udf and (artifact.udf[udf] in value.keys()):
            filtered_artifacts.append(artifact)
        elif udf in artifact.udf:
            incorrect_artifacts.append(artifact)
            logging.warning(("Filtered out artifact for sample: {0}"
                          ", due to wrong {1}").format(artifact.samples[0].name, udf))
        else:
            incorrect_artifacts.append(artifact)
            logging.warning(("Filtered out artifact for sample: {0}"
                          ", due to undefined/blank {1}").format(artifact.samples[0].name, udf))

    return filtered_artifacts, incorrect_artifacts

def main(lims,args,epp_logger):
    p = Process(lims,id = args.pid)

    udf_factor1 = 'Concentration'
    udf_factor2 = 'Volume (ul)'
    udf_check = 'Conc. Units'
    unit_amount_map = {'ng/ul' : 'Amount (ng)',
                       'ng/uL' : 'Amount (ng)',
                       'nM'    : 'Amount (fmol)',
                       'pM'    : 'Amount (fmol)'}


    if args.aggregate:
        artifacts = p.all_inputs(unique=True)
    else:
        all_artifacts = p.all_outputs(unique=True)
        artifacts = [a for a in all_artifacts if a.output_type == "ResultFile"]

    correct_artifacts, wrong_factor1 = check_udf_is_defined(artifacts, udf_factor1)
    correct_artifacts, wrong_factor2 = check_udf_is_defined(correct_artifacts, udf_factor2)

    correct_artifacts, wrong_value = check_udf_has_value(correct_artifacts, udf_check, unit_amount_map)

    if correct_artifacts:
        apply_calculations(
            correct_artifacts, udf_factor1, "*", udf_factor2, unit_amount_map, p
        )

    d = {'ca': len(correct_artifacts),
         'ia': len(wrong_factor1)+ len(wrong_factor2) + len(wrong_value)}

    abstract = ("Updated {ca} artifact(s), skipped {ia} artifact(s) with "
                "wrong and/or blank values for some udfs.").format(**d)

    print(abstract, file=sys.stderr) # stderr will be logged and printed in GUI


if __name__ == "__main__":
    # Initialize parser with standard arguments and description
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    parser.add_argument('--log',
                        help='Log file for runtime info and errors.')
    parser.add_argument('--aggregate', action='store_true',
                        help=('Use this tag if current Process is an '
                              'aggregate QC step'))
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    with EppLogger(args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args, epp_logger)
