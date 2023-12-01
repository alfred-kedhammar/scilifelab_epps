import os
import sys
import xml.etree.ElementTree as ET
from argparse import ArgumentParser
from datetime import datetime as dt

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from epp_utils import udf_tools
from scilifelab_epps.epp import get_well_number

DESC = """This script parses the Agilent BioAnalyzer XML report. 

It is written to replace the current Illumina-supplied script consisting of compiled 
Java which does not as of 2023-08-25 populate the measurement UDFs of interest.
"""


def main(lims, args):
    currentStep = Process(lims, id=args.pid)
    log = []
    errors = False

    # Set up an XML tree from the BioAnalyser output file
    tree = ET.fromstring(get_ba_output_file(currentStep, log))
    samples_node = tree.find(".//Samples")

    # Grab the output measurements, i.e. output artifacts with a defined location
    measurements = [art for art in currentStep.all_outputs() if art.location[1]]

    # Define which LIMS UDFs should be populated with which XML metric
    results_to_grab = {
        # {LIMS UDF: (XML name, type)}
        "Min Size (bp)": ("StartBasePair", int),
        "Max Size (bp)": ("EndBasePair", int),
        "Concentration": ("RegionConcentration", float),
        "Size (bp)": ("AverageSize", int),
        "Ratio (%)": ("PercentTotal", float),
    }

    # Iterate over output measurements and gather the results
    for measurement in measurements:
        # Find the corresponding well number
        try:
            well_num = get_well_number(measurement)
        except:
            log.append(
                f"ERROR: Could not determine the well number of {measurement.name}, skipping."
            )
            errors = True
            continue

        # Isolate the XML sample nest w. the same well as the measurement
        matching_wells = [
            e
            for e in samples_node
            if int(e.find("WellNumber").text.strip()) == well_num
        ]
        try:
            assert len(matching_wells) == 1
            sample_node = matching_wells[0]
        except:
            log.append(
                f"ERROR: The measurement {measurement.name} was not found in the .xml file, skipping."
            )
            errors = True
            continue

        # Get the xml smear metrics
        try:
            results_node = sample_node.find(".//RegionsMolecularResults")
        except:
            log.append(
                f"ERROR: No smear region was found for {measurement.name} in the .xml file, skipping."
            )
            errors = True
            continue

        # Grab the target results from the xml smear metrics
        for udf_name in results_to_grab:
            xml_nest, return_type = results_to_grab[udf_name]

            result = results_node.find(f".//{xml_nest}").text.strip()
            if return_type == int:
                result = int(round(float(result), 0))
            elif return_type == float:
                result = float(result)

            try:
                # For concentrations (given in pg/ul), convert to ng/ul
                if udf_name == "Concentration":
                    result = result / 1000
                    udf_tools.put(measurement, "Conc. Units", "ng/ul")

                udf_tools.put(measurement, udf_name, result)

            except AssertionError:
                log.append(
                    f"ERROR: Could not assign UDF {udf_name} of measurement {measurement.name}, skipping."
                )
                errors = True
                continue

        log.append(f"Successfully pulled metrics for measurment {measurement.name}.")

    # Write log
    timestamp = dt.now().strftime("%y%m%d_%H%M%S")
    log_filename = (
        "_".join(["parse_bioanalyzer_xml_log", currentStep.id, timestamp]) + ".txt"
    )
    with open(log_filename, "w") as logContext:
        logContext.write("\n".join(log))

    # Upload log
    for out in currentStep.all_outputs():
        if out.name == "Bioanalyzer XML Parsing Log File":
            for f in out.files:
                lims.request_session.delete(f.uri)
            lims.upload_new_file(out, log_filename)

            # Clean up
            os.remove(log_filename)

            if errors:
                sys.stderr.write("Some samples were skipped, please check the Log file")
                sys.exit()


def get_ba_output_file(currentStep, log):
    content = None
    for outart in currentStep.all_outputs():
        # Try fetching the BA result file from the uploaded file in LIMS
        if (
            outart.type == "ResultFile"
            and outart.name == "Bioanalyzer XML Result File (required)"
        ):
            try:
                fid = outart.files[0].id
                content = lims.get_file_contents(id=fid)
                if isinstance(content, bytes):
                    content = content.decode("utf-8")
            except:
                log.append("No BioAnalyzer .xml file found")
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
