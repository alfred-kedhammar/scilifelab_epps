from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
import xml.etree.ElementTree as ET

DESC = """This script parses the Agilent BioAnalyzer XML report. 

It is written to replace the current Illumina-supplied script consisting of compiled 
Java which as of 2023-08-25 does not serve it's purpose. 
"""


def well_name2num(well_name: str) -> int:

    letter2num = {}
    for i, l in zip(range(0,8), "ABCDEFGH"):
        letter2num[l] = i

    row_letter, col_num = well_name.split(":")
    well_num = letter2num[row_letter] + int(col_num)
    return well_num


def main(lims, args):
    currentStep = Process(lims, id=args.pid)
    log = []

    # Set up an XML tree
    tree = ET.fromstring(get_ba_output_file(currentStep, log))
    samples_node = tree.find('.//Samples')

    # Grab the output measurements, i.e. output artifacts with a defined location
    measurements = [art for art in currentStep.all_outputs() if art.location[1]]

    for measurement in measurements:
        well_num = well_name2num(measurement.location[1])

        # Isolate the XML sample nest w. the same well as the measurement
        matching_wells = [e for e in samples_node if int(e.find('WellNumber').text.strip()) == well_num]
        assert len(matching_wells) == 1
        sample_node = matching_wells[0]

        # Get the xml measurements


    
    # Useful
    results_node = tree.find('.//RegionsMolecularResults')


    sample_names = [s.text.strip() for s in samples_node.findall("./Sample/Name")]

    for sample_name in sample_names:

    



def get_ba_output_file(currentStep, log):
    content = None
    for outart in currentStep.all_outputs():
        # Try fetching the BA result file from the uploaded file in LIMS
        if outart.type == 'ResultFile' and outart.name == 'Bioanalyzer XML Result File (required)':
            try:
                fid = outart.files[0].id
                content = lims.get_file_contents(id=fid)
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
            except:
                log.append('No BioAnalyzer .xml file found')
            break
    return content


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    try:
        main(lims, args)
    except BaseException as e:
        sys.stderr.write(str(e))
        sys.exit(2)