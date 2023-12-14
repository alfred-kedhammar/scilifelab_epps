from __future__ import division
from argparse import ArgumentParser
from genologics.lims import Lims
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.entities import Process
import xml.etree.ElementTree as ET
from epp_utils import udf_tools
from scilifelab_epps.epp import get_well_number
import sys
from datetime import datetime as dt
import os

DESC = """This script parses the Agilent BioAnalyzer XML report. 

It is written to replace the current Illumina-supplied script consisting of compiled 
Java which does not as of 2023-08-25 populate the measurement UDFs of interest.
"""


def main(lims, args):
    currentStep = Process(lims, id=args.pid)
    log = []
    errors = False
    count_per = "row"  # BioAnalyzer XML numbers wells row-wise

    # Set up an XML tree from the BioAnalyser output file
    xml_tree = ET.fromstring(get_ba_output_file(currentStep, log))
    xml_samples = xml_tree.find(".//Samples")
    log.append(f"{len(xml_samples)} samples found in .xml file.")

    # Grab the output measurements, i.e. output artifacts with a defined location
    lims_arts = [art for art in currentStep.all_outputs() if art.location[1]]
    log.append(f"{len(lims_arts)} LIMS measurements to be processed.")

    # Define which LIMS UDFs should be populated with which XML metric
    udf_to_xml = {
        # {LIMS UDF: (XML name, type)}
        "Min Size (bp)": ("StartBasePair", int),
        "Max Size (bp)": ("EndBasePair", int),
        "Concentration": ("RegionConcentration", float),
        "Size (bp)": ("AverageSize", int),
        "Ratio (%)": ("PercentTotal", float),
    }

    log.append(
        "\nFor each sample, populate the following UDFs with the following .xml nests"
    )
    for i, j in udf_to_xml.items():
        log.append(f"{i} --> {j}")

    # Iterate over output measurements and gather the results
    for lims_art in lims_arts:
        log.append(f"\nProcessing measurement '{lims_art.name}'...")

        # Find the corresponding well number
        try:
            lims_well_num = get_well_number(lims_art, count_per)
            log.append(
                f"Well '{lims_art.location[1]}' corresponds to {count_per}-wise well number {lims_well_num}"
            )
        except:
            log.append(
                f"ERROR: Could not determine the well number of {lims_art.name}, skipping."
            )
            errors = True
            continue

        # Isolate the XML sample nest w. the same well as the measurement
        xml_matching_samples = [
            sample_node
            for sample_node in xml_samples
            if int(sample_node.find("WellNumber").text.strip()) == lims_well_num
        ]

        if len(xml_matching_samples) == 1:
            xml_sample = xml_matching_samples[0]
            xml_sample_name = xml_sample.find(".//Name").text.strip()
            log.append(
                f"Found .xml sample '{xml_sample_name}' matching {count_per}-wise well number {lims_well_num}."
            )

        elif len(xml_matching_samples) < 1:
            log.append(
                f"ERROR: Found no samples in the .xml at well number {lims_well_num}, skipping."
            )
            errors = True
            continue
        elif len(xml_matching_samples) > 1:
            log.append(
                f"ERROR: Found multiple samples in the .xml at well number {lims_well_num}, skipping."
            )
            errors = True
            continue
        else:
            raise AssertionError

        # Get the xml smear metrics
        try:
            xml_results = xml_sample.find(".//RegionsMolecularResults")
            assert xml_results
            log.append("Fetched sample results section from .xml.")
        except:
            log.append("ERROR: No smear region was found, skipping.")
            errors = True
            continue

        # Grab the target results from the xml smear metrics
        for udf_name in udf_to_xml:
            xml_query, return_type = udf_to_xml[udf_name]

            result = xml_results.find(f".//{xml_query}").text.strip()
            if return_type == int:
                result = int(round(float(result), 0))
            elif return_type == float:
                result = float(result)

            try:
                # For concentrations (given in pg/ul), convert to ng/ul
                if udf_name == "Concentration":
                    result = result / 1000
                    udf_tools.put(lims_art, "Conc. Units", "ng/ul")

                udf_tools.put(lims_art, udf_name, result)

                log.append(f"{udf_name} --> {result}")

            except AssertionError:
                log.append(
                    f"ERROR: Could not assign UDF {udf_name} of measurement {lims_art.name}, skipping."
                )
                errors = True
                continue

        log.append("Successfully pulled metrics.")

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
