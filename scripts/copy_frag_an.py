#!/usr/bin/env python

DESC = """EPP script to copy 'Conc. (ng/ul)', 'RQN' and 'xxS/xxS' for each
sample sample in the 'Quality Table File' to the 'Concentration', 'RIN' and '28s/18s ratio'
fields of the output analytes of the process. In addition, the Conc. Units will be filled in as "ng/ul"

Warnings are generated to the user and stored in regular log file wich allso
contains regular execution information in the folowing cases:

1) missing row names (samples) in file
2) duplicated row names (samples)
3) missing value (concentrations)
4) values found but for some reason are not successfully copied:

Can be executed in the background or triggered by a user pressing a "blue button".

Written by Denis Moreno and Chuan Wang
"""

import csv
import re
import sys
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import EppLogger, set_field


def get_result_file(process, log):
    content = dict()
    for outart in process.all_outputs():
        # get the right output artifact
        if outart.type == "ResultFile" and outart.name == "Quality Table File":
            try:
                fid = outart.files[0].id
                content["Quality_Table_File"] = lims.get_file_contents(id=fid)
            except:
                log.append("No Quality Table File found.")
        if outart.type == "ResultFile" and outart.name == "Smear Analysis Result File":
            try:
                fid = outart.files[0].id
                content["Smear_Analysis_Result_File"] = lims.get_file_contents(id=fid)
            except:
                log.append("No Smear Analysis Result File found.")
    # give error when there is no input file
    if len(content) == 0:
        raise RuntimeError("Cannot access any output file.")
    return content


def get_data(csv_content, log):
    read = False
    data = {}
    # read quality table file
    if csv_content.get("Quality_Table_File"):
        if isinstance(csv_content["Quality_Table_File"], str):
            text = csv_content["Quality_Table_File"]
        else:
            text = csv_content["Quality_Table_File"].encode("utf-8")
        # Try to determine the format of the csv:
        dialect = csv.Sniffer().sniff(text)
        pf = csv.reader(text.splitlines(), dialect=dialect)
        # defaults
        sample_index = 1
        conc_index = 2
        rin_index = 3
        ratio_index = 4
        ratio_header_pat = re.compile("[0-9]*S/[0-9]*S")
        sample_list = []
        for row in pf:
            if "Sample ID" in row:
                # this is the header row
                for header in row:
                    if ratio_header_pat.findall(header):
                        ratio_header = ratio_header_pat.findall(header)[0]
                sample_index = row.index("Sample ID")
                conc_index = row.index("Conc. (ng/ul)")
                rin_index = row.index("RQN")
                ratio_index = row.index(ratio_header)
                read = True
            elif read and row[sample_index]:
                if row[sample_index] not in sample_list:
                    sample_list.append(row[sample_index])
                    data[row[sample_index]] = {}
                    data[row[sample_index]]["concentration"] = row[conc_index]
                    data[row[sample_index]]["rin"] = row[rin_index]
                    data[row[sample_index]]["ratio"] = row[ratio_index]
                else:
                    # Multiple sample entris for one sample, drop the key
                    log.append(
                        "sample {} has multiple entries in the Quality Table File. Please check the file manually.".format(
                            row[sample_index]
                        )
                    )
                    try:
                        del data[row[sample_index]]
                    except KeyError:
                        continue
    # reset to read smear analysis result file
    read = False
    if csv_content.get("Smear_Analysis_Result_File"):
        if isinstance(csv_content["Smear_Analysis_Result_File"], str):
            text = csv_content["Smear_Analysis_Result_File"]
        else:
            text = csv_content["Smear_Analysis_Result_File"].encode("utf-8")
        # Try to determine the format of the csv:
        dialect = csv.Sniffer().sniff(text)
        pf = csv.reader(text.splitlines(), dialect=dialect)
        # defaults
        sample_index = 1
        range_index = 2
        dv200_index = 4
        sample_list = []
        for row in pf:
            if "Sample ID" in row:
                # this is the header row
                sample_index = row.index("Sample ID")
                range_index = row.index("Range")
                dv200_index = row.index("% Total")
                read = True
            elif read and row[sample_index]:
                if row[sample_index] not in sample_list:
                    sample_list.append(row[sample_index])
                    # case of a new sample
                    if row[sample_index] not in data:
                        data[row[sample_index]] = {}
                    data[row[sample_index]]["range"] = row[range_index]
                    data[row[sample_index]]["dv200"] = row[dv200_index]
                # Multiple sample entris for one sample, clear the existing values
                else:
                    log.append(
                        "sample {} has multiple entries in the Smear Analysis Result File. Please check the file manually.".format(
                            row[sample_index]
                        )
                    )
                    try:
                        del data[row[sample_index]]
                    except KeyError:
                        continue
    return data


def get_frag_an_csv_data(process):
    # samples missing from the csv file
    missing_samples = 0
    # strings returned to the EPP user
    log = []
    # Get file contents by parsing lims artifacts
    file_content = get_result_file(process, log)
    # parse the file and get the interesting data out
    data = get_data(file_content, log)

    for target_file in process.result_files():
        key_dict = {
            "concentration": "Concentration",
            "rin": "RIN",
            "ratio": "28s/18s ratio",
            "range": "Range",
            "dv200": "DV200",
        }
        file_sample = target_file.samples[0].name
        if file_sample in data:
            for k, v in key_dict.items():
                if data[file_sample].get(k):
                    value = None
                    try:
                        if data[file_sample][k].upper() != "NAN":
                            if k == "range":
                                value = str(data[file_sample][k])
                            else:
                                value = float(data[file_sample][k])
                            target_file.udf[v] = value
                            if k == "concentration":
                                target_file.udf["Conc. Units"] = "ng/ul"
                        else:
                            log.append(
                                "NaN {} value for Sample {}.".format(k, file_sample)
                            )
                    except ValueError:
                        log.append(
                            "Bad {} value format for Sample {}.".format(k, file_sample)
                        )
            # actually set the data
            target_file.put()
            set_field(target_file)
        else:
            missing_samples += 1
    if missing_samples:
        log.append(
            "{}/{} samples are missing in the Result File.".format(
                missing_samples, len(process.result_files())
            )
        )
    print("".join(log), file=sys.stderr)


def main(lims, pid, epp_logger):
    process = Process(lims, id=pid)
    get_frag_an_csv_data(process)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument(
        "--pid", default="24-38458", dest="pid", help="Lims id for current Process"
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
