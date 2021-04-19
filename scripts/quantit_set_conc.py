#!/usr/bin/env python
DESC = """EPP script for Quant-iT measurements to calculate concentrations and
load input artifact-udfs and output file-udfs of the process with concentration
values and fluorescence intensity.

Reads from:
    --Lims fields--
    "Assay type"                process udf
    "Standard volume"           process udf
    "Standard dilution"         process udf
    "Linearity of standards"    process udf
    "Sample volume"             process udf

    --files--
    "Standards File (.txt)"     "shared result file" uploaded by user.
    "Quant-iT Result File 1"    "shared result file" uploaded by user.
    "Quant-iT Result File 2"    "shared result file" uploaded by user. (optional)

Writes to:
    --Lims fields--
    "Fluorescence intensity 1"  udf of input analytes to the process
    "Fluorescence intensity 2"  udf of input analytes to the  process
    "Concentration"             udf of process artifacts (result file)
    "Conc. Units"               udf of process artifacts (result file)

Logging:
    The script outputs a regular log file with regular execution information.

Performance:
    1)  Makes a linear regression analysis on the End RFU standards read from
    "Standards File (.txt)" --> slope, intersect and R2 (Pearson correlation
    coefficient)

    2)  If R2 is less than "Linearity of standards", standards are not ok and
    the user is warned "Problem with standards! Redo measurement!".

    3)  If R2 is greater than "Linearity of standards", concentration values are
    calculated based on the linear regression parameters, copied to the
    "Concentration"-udf on the output artifacts (one result file per sample).
    The "Conc. Units"-udf on the same artifacts are set to "ng/ul".

    4)  "End RFU" values from "Quant-iT Result File 1" are copied to the udf
    "Fluorescence intensity 1" of the input analyte to the process, and
    "End RFU" values from "Quant-iT Result File 2" are copied to the udf
    "Fluorescence intensity 2".

Written by Maya Brandi
"""
from __future__ import print_function
import os
import sys
import logging
import numpy as np

from argparse import ArgumentParser
from requests import HTTPError
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import Process
from scilifelab_epps.epp import EppLogger
from scilifelab_epps.epp import set_field
from scilifelab_epps.epp import ReadResultFiles

class QuantitConc():
    def __init__(self, process, file_handler):
        self.file_handler = file_handler
        self.udfs = dict(list(process.udf.items()))
        self.abstract = []
        self.missing_udfs = []
        self.missing_samps =[]
        self.no_samps = 0
        self.standards = {}
        self.model = []
        self.result_files = {}

    def prepare_result_files_dict(self):
        """"Quant-iT Result File 1" and "Quant-iT Result File 2" (the second is
        optional) uploaded by user, are formated and stored in a dict"""
        result_files_dict = {}
        file_names = {'Quant-iT Result File 1':'Fluorescence intensity 1',
                    'Quant-iT Result File 2':'Fluorescence intensity 2'}
        for f_name, udf_name in file_names.items():
            if f_name in self.file_handler.shared_files:
                result_file = self.file_handler.shared_files[f_name]
                result_files_dict[udf_name] = self.file_handler.format_file(
                    result_file, name = f_name, root_key_col = 1, header_row = 19)
        self.result_files = result_files_dict

    def _make_standards_dict(self):
        """End RFU standards are read from 'Standards File (.txt)' and stored
        in a dict"""
        standards_file = self.file_handler.shared_files['Standards File (.txt)']
        standards_file_formated = self.file_handler.format_file(standards_file,
                name = 'Standards File (.txt)', root_key_col = 1, header_row = 19)
        standards_dict = {}
        for k,v in standards_file_formated.items():
            cond1 = set(['Sample','End RFU']).issubset(v)
            cond2 = v['Sample'].split()[0]=='Standard'
            if cond1 and cond2:
                standard = int(v['Sample'].split()[1])
                standards_dict[standard] = float(v['End RFU'])
        return standards_dict

    def _nuclear_acid_amount_in_standards(self):
        """Nuclear acid amount in standards is calculated as
        "Supposed concentrations of standards" * "Standard volume" / "Standard
        dilution" Supposed concentrations of standards are different between
        Quant-iT BR assays and Quant-iT HS assays."""
        requiered_udfs = set(['Standard volume','Assay type','Standard dilution'])
        supp_conc_stds = {'RNA BR':[0,5,10,20,40,60,80,100],
                          'DNA BR':[0,5,10,20,40,60,80,100],
                          'RNA':[0,0.5,1,2,4,6,8,10],
                          'DNA HS':[0,0.5,1,2,4,6,8,10]}
        if requiered_udfs.issubset(list(self.udfs.keys())):
            nuclear_acid_amount = np.ones(8)
            for standard in range(8):
                supp_conc_standard = supp_conc_stds[self.udfs['Assay type']][standard]
                stand_conc = supp_conc_standard * self.udfs['Standard volume']
                stand_dil = self.udfs['Standard dilution']
                nuclear_acid_amount[standard] = np.true_divide(stand_conc, stand_dil)
            return nuclear_acid_amount
        elif not requiered_udfs.issubset(self.missing_udfs):
            self.missing_udfs += requiered_udfs
        return None

    def _linear_regression(self, X,Y):
        """Perform linear regression with intersect forced to origin. And
        calculates Pearson correlation coefficient R2. """
        X = np.array(X)
        X_force_zero = X[:,np.newaxis]
        slope = np.linalg.lstsq(X_force_zero, Y)[0]

        A = np.array([ X, np.ones(len(X))])
        resid = np.linalg.lstsq(A.T,Y)[1]
        R2 = 1 - resid / (Y.size * Y.var())
        return float(R2), float(slope)

    def fit_model(self):
        """Performing linear regression on standards.
        X = Relative fluorescence intensities of 8 standards
        Y = Nuclear acid amount in standards
        Y = slope*X + intersect; R2=Pearson correlation coefficient."""
        self.standards = self._make_standards_dict()
        amount_in_standards = self._nuclear_acid_amount_in_standards()
        if amount_in_standards is not None:
            relative_standards = np.ones(8)
            for k,v in self.standards.items():
                relative_standards[k-1] = v - self.standards[1]
            R2, slope = self._linear_regression(relative_standards,
                            amount_in_standards)
            self.model = [R2, slope]
        else:
            self.model = None

    def get_and_set_fluor_int(self, target_file):
        """Copies "End RFU" values from "Quant-iT Result File 1" and
        "Quant-iT Result File 2" (if provided) to udfs "Fluorescence intensity 1"
        and "Fluorescence intensity 2. Calculates and returns Relative
        fluorescence intensity standards:
        rel_fluor_int = The End RFU of standards - Background fluorescence
        intensity"""
        sample = target_file.input_artifact_list()[0].name
        fluor_int = []
        target_udfs = target_file.udf
        # For the moment we dont know ofa way to delete udfs. Should be solved.
        #if dict(target_udfs.items()).has_key('Fluorescence intensity 1'):
        #    del target_udfs['Fluorescence intensity 1']
        #if dict(target_udfs.items()).has_key('Fluorescence intensity 2'):
        #    del target_udfs['Fluorescence intensity 2']
        target_file.udf = target_udfs
        for udf_name ,formated_file in self.result_files.items():
            if sample in list(formated_file.keys()):
                fluor_int.append(float(formated_file[sample]['End RFU']))
                target_file.udf[udf_name] = float(formated_file[sample]['End RFU'])
            else:
                self.missing_samps.append(sample)
        set_field(target_file)
        rel_fluor_int = np.mean(fluor_int) - self.standards[1]
        return rel_fluor_int

    def calc_and_set_conc(self, target_file, rel_fluor_int):
        """Concentrations are calculated based on the linear regression
        parametersand copied to the "Concentration"-udf of the target_file. The
        "Conc. Units"-udf is set to "ng/ul"""
        requiered_udfs = set(['Sample volume','Standard dilution','WS volume'])
        if requiered_udfs.issubset(list(self.udfs.keys())) and self.model:
            conc = np.true_divide((self.model[1] * rel_fluor_int * (
                self.udfs['WS volume'] + self.udfs['Sample volume'])),
                self.udfs['Sample volume']*(self.udfs['WS volume'] +
                self.udfs['Standard volume']))
            target_file.udf['Concentration'] = conc
            target_file.udf['Conc. Units'] = 'ng/ul'
            set_field(target_file)
            self.no_samps +=1
        elif not requiered_udfs.issubset(self.missing_udfs):
            self.missing_udfs += requiered_udfs


def main(lims, pid, epp_logger):
    process = Process(lims,id = pid)
    target_files = dict((r.samples[0].name, r) for r in process.result_files())
    file_handler = ReadResultFiles(process)
    QiT = QuantitConc(process, file_handler)
    QiT.fit_model()
    QiT.prepare_result_files_dict()
    if QiT.model and 'Linearity of standards' in list(QiT.udfs.keys()):
        R2 = QiT.model[0]
        if R2 >= QiT.udfs['Linearity of standards']:
            QiT.abstract.insert(0,"R2 = {0}. Standards OK.".format(R2))
            if QiT.result_files:
                for sample, target_file in target_files.items():
                    rel_fluor_int = QiT.get_and_set_fluor_int(target_file)
                    QiT.calc_and_set_conc(target_file, rel_fluor_int)
                QiT.abstract.append("Concentrations uploaded for {0} "
                                                "samples.".format(QiT.no_samps))
            else:
                QiT.abstract.append("Upload input file(s) for samples.")
        else:
            QiT.abstract.insert(0, "R2 = {0}. Problem with standards! Redo "
                                                    "measurement!".format(R2))
    else:
        QiT.missing_udfs.append('Linearity of standards')
    if QiT.missing_samps:
        QiT.abstract.append("The following samples are missing in Quant-iT "
            "result File 1 or 2: {0}.".format(', '.join(QiT.missing_samps)))
    if QiT.missing_udfs:
        QiT.abstract.append("Are all of the following udfs set? : {0}".format(
                                                   ', '.join(QiT.missing_udfs)))
    print(' '.join(QiT.abstract), file=sys.stderr)

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid', default = None , dest = 'pid',
                        help='Lims id for current Process')
    parser.add_argument('--log', dest = 'log',
                        help=('File name for standard log file, '
                              'for runtime information and problems.'))

    args = parser.parse_args()
    lims = Lims(BASEURI,USERNAME,PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args.pid, epp_logger)
