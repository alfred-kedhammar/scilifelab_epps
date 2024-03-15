#!/usr/bin/env python

DESC = """EPP script for Quant-iT mesurements to set QC flaggs and
intensity check based on concentrations, Fluorescence intensity.

Performance:
    1) compares udfs "Fluorescence intensity 1" and "Fluorescence intensity 2"
    with the Saturation threshold of fluorescence intensity. If either of these
    two udfs >= Saturation threshold of fluorescence intensity, assign
    "Saturated" to the udf "Intensity check" and assign "Fail" to the sample.
    Otherwise assign "OK" to the analyte "Intensity check".

    2) Copies the %CV values to the sample udf "%CV".
    For a sample with duplicate measurements, "%CV" is calculated as:
        %CV = (SD of "Fluorescence intensity 1" and "Fluorescence intensity 2")/
        (Mean of "Fluorescence intensity 1" and ""Fluorescence intensity 2)

    3) If "%CV" >= "Allowed %CV of duplicates", assigning "Fail" to the QC flagg.

    4) For a sample with only one measurement, if it passes in step 2, a "Pass"
    should be assigned to the QC flag. For a sample with duplicate measurements,
    if it passes both step 2 and step 4, a "Pass" is assigned to the QC flag.

Reads from:
    --Lims fields--
    "Saturation threshold of fluorescence intensity"    process udf
    "Allowed %CV of duplicates"                         process udf
    "Fluorescence intensity 1"  udf of input analytes to the process
    "Fluorescence intensity 2"  udf of input analytes to the process

    --files--
    "Standards File (.txt)"     "shared result file" uploaded by user.
    "Quant-iT Result File 1"    "shared result file" uploaded by user.
    "Quant-iT Result File 2"    "shared result file" uploaded by user. (optional)

Writes to:
    --Lims fields--
    "Intensity check"           udf of process artifacts (result file)
    "%CV"                       udf of process artifacts (result file)
    "QC"                        qc-flag of process artifacts (result file)

Logging:
    The script outputs a regular log file with regular execution information.

Written by Maya Brandi
"""
import sys
from argparse import ArgumentParser

import numpy as np
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import EppLogger, set_field


class QuantitQC:
    def __init__(self, process):
        self.result_files = process.result_files()
        self.udfs = dict(list(process.udf.items()))
        self.required_udfs = {
            "Allowed %CV of duplicates",
            "Saturation threshold of fluorescence intensity",
            "Minimum required concentration (ng/ul)",
        }
        self.abstract = []
        self.missing_udfs = []
        self.hig_CV_fract = 0
        self.saturated = 0
        self.low_conc = 0
        self.flour_int_missing = 0
        self.conc_missing = 0
        self.no_failed = 0

    def saturation_QC(self, result_file, udfs):
        treshold = self.udfs["Saturation threshold of fluorescence intensity"]
        allowed_dupl = self.udfs["Allowed %CV of duplicates"]
        fint_key1 = "Fluorescence intensity 2"
        fint_key2 = "Fluorescence intensity 1"
        fint_2 = udfs[fint_key2] if fint_key2 in udfs else None
        fint_1 = udfs[fint_key1] if fint_key1 in udfs else None

        if fint_1 or fint_2:
            qc_flag = "PASSED"
            if (fint_1 is not None and fint_1 >= treshold) or (
                fint_2 is not None and fint_2 >= treshold
            ):
                result_file.udf["Intensity check"] = "Saturated"
                qc_flag = "FAILED"
                self.saturated += 1
            else:
                result_file.udf["Intensity check"] = "OK"
                if fint_1 and fint_2:
                    std = np.std([fint_1, fint_2])
                    mean = np.mean([fint_1, fint_2])
                    procent_CV = np.true_divide(std, mean)
                    result_file.udf["%CV"] = procent_CV
                    if procent_CV >= allowed_dupl:
                        qc_flag = "FAILED"
                        self.hig_CV_fract += 1
            return qc_flag
        else:
            self.flour_int_missing += 1
            return None

    def concentration_QC(self, result_file, result_file_udfs):
        min_conc = self.udfs["Minimum required concentration (ng/ul)"]
        if "Concentration" in result_file_udfs:
            if result_file_udfs["Concentration"] < min_conc:
                self.low_conc += 1
                return "FAILED"
            else:
                return "PASSED"
        else:
            self.conc_missing += 1
            return None

    def assign_QC_flag(self):
        if self.required_udfs.issubset(list(self.udfs.keys())):
            for result_file in self.result_files:
                result_file_udfs = dict(list(result_file.udf.items()))
                QC_conc = self.concentration_QC(result_file, result_file_udfs)
                QC_sat = self.saturation_QC(result_file, result_file_udfs)
                if QC_conc and QC_sat:
                    QC = QC_conc if QC_conc == QC_sat else "FAILED"
                    self.no_failed += 1 if QC == "FAILED" else 0
                    result_file.qc_flag = QC
                    set_field(result_file)
        else:
            self.missing_udfs = ", ".join(list(self.required_udfs))


def main(lims, pid, epp_logger):
    process = Process(lims, id=pid)
    QiT = QuantitQC(process)
    QiT.assign_QC_flag()
    if QiT.flour_int_missing:
        QiT.abstract.append(
            f"Fluorescence intensity is missing for {QiT.flour_int_missing} " "samples."
        )
    if QiT.missing_udfs:
        QiT.abstract.append(
            "Could not set QC flags. Some of the following "
            f"required udfs seems to be missing: {QiT.missing_udfs}."
        )
    else:
        QiT.abstract.append(
            f"{QiT.no_failed} out of {len(process.result_files())} samples failed " "QC."
        )
    if QiT.saturated:
        QiT.abstract.append(
            f"{QiT.saturated} samples had saturated fluorescence " "intensity."
        )
    if QiT.hig_CV_fract:
        QiT.abstract.append(f"{QiT.hig_CV_fract} samples had high %CV.")
    if QiT.low_conc:
        QiT.abstract.append(f"{QiT.low_conc} samples had low concentration.")
    if QiT.conc_missing:
        QiT.abstract.append(
            f"Concentration is missing for {QiT.conc_missing} " "sample(s)."
        )
    QiT.abstract = list(set(QiT.abstract))
    print(" ".join(QiT.abstract), file=sys.stderr)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument(
        "--pid", default=None, dest="pid", help="Lims id for current Process"
    )
    parser.add_argument(
        "--log",
        dest="log",
        help=(
            "File name for standard log file, " "for runtime information and problems."
        ),
    )

    args = parser.parse_args()
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args.pid, epp_logger)
