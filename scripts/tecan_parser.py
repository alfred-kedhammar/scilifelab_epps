import sys
import re
import fileinput
import csv
import os

from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI,USERNAME,PASSWORD
from genologics.entities import *
from scilifelab_epps.epp import attach_file, EppLogger

DESC="""EPP used to parse csv files from the Tecan plate reader"""

# Display error message if any measurement has a standard deviation and CV above
# these numbers:
SD_LIMIT = 1000
CV_LIMIT = 20

def parse(iterable):
    # Pattern for matching "Layout" ID:
    p = re.compile(r"(S[MT])1_(\d{1,2})")
    # Read in the csv file:
    for line in iterable.splitlines():
        if "#" in line:
            vals = line.rstrip().replace("#","").replace(" ","").split(",")
            vals.append("#")
        else:
            vals = line.rstrip().replace(" ","").split(",")
            vals.append("")
        # Filter out all rows with sample and standard entries and
        # Use "1/3" or "1/2" replicate:
        m = p.match(vals[1])
        if m and vals[2] in ("1/3", "1/2"):
            # Substitute replicate with index:
            vals[2] = m.group(2)
            # Assign a type and source well:
            if m.group(1) == "SM":
                vals[1] = "Sample"
                vals[0] = index_to_well(m.group(2))
            else:
                vals[1] = "Standard"
                vals[0] = ""
            # Skip some columns:
            del vals[9]
            del vals[6]
            del vals[3]

            yield vals

def convert(file_in, file_out):
    # Make a list of it to be able to sort:
    file_in = file_in.read()
    data = list(parse(file_in))
    # Sort the results on type then index:
    data.sort(key=lambda x: (x[1], int(x[2])))
    # Create the header:
    header = [
        "Well",
        "Type",
        "Index",
        "Raw mean",
        "Raw standard dev",
        "Conc mean (ng/uL)",
        "Conc CV%",
        "Mark"
    ]
    # Write data:
    writer = csv.writer(file_out)
    writer.writerow(header)
    writer.writerows(data)

    return data

# Convert an index to a plate coordinate, e.g. 8 => H1
def index_to_well(index):
    i = int(index)
    row = chr(64 + ((i-1) % 8 + 1))
    col = int(((i-1) - (i-1) % 8) / 8 + 1)
    return "{0}{1}".format(row, col)

def dictionarize(datalist):
    data_to_upload={}
    for row in datalist:
        data_to_upload[row[0]]={}
        data_to_upload[row[0]]['conc']=row[5]
        data_to_upload[row[0]]['cv']=row[6]
        data_to_upload[row[0]]['raw_sd']=row[4]


    return data_to_upload

def main(args, lims):
    err_out=""
    pro=Process(lims, id=args.pid)
    for output in pro.all_outputs():
        if output.name == "Tecan output file":
            try:
                fid=output.files[0].id
            except:
                raise RuntimeError("Cannot access the tecan output file to read the concentrations.")
        elif output.name=='EPP log file':
            out_id=output.id

    file_contents=lims.get_file_contents(id=fid)
    with open('{0}_tecan.out'.format(out_id), 'w') as outf:
        data=convert(file_contents, outf)

    data_dict=dictionarize(data)

    for iom in pro.input_output_maps:
        outp = iom[1]["uri"]
        if outp.output_type == "ResultFile" and len(outp.samples) == 1:
            status = "PASSED"
            pos = outp.location[1].replace(":", "")
            sample_data = data_dict[pos]
            outp.udf["Conc. Units"] = "ng/ul"
            try:
                outp.udf["Concentration"] = float(sample_data["conc"])
                outp.udf["%CV"] = float(sample_data["cv"])
                outp.udf["Raw standard dev"] = float(sample_data["raw_sd"])
            except ValueError:
                status = "FAILED"
                # Set the error string once to whatever condition that occurs first:
                if not err_out:
                    err_out = "Data is missing for one or several samples."
            else:
                if float(sample_data["raw_sd"]) > SD_LIMIT and float(sample_data["cv"]) > CV_LIMIT:
                    status = "FAILED"
                    if not err_out:
                        err_out="One or several samples has a raw SD above {:d} and concentration CV above {:d}. Check the output file for details.".format(SD_LIMIT, CV_LIMIT)

            outp.qc_flag = status
            outp.put()

    if err_out:
        sys.stderr.write(err_out)
        sys.exit(2)

if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument('--pid',
                        help='Lims id for current Process')
    parser.add_argument('--log',
                        help='log file name')
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    main(args, lims)
