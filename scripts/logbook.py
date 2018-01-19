
from genologics.entities import *
from genologics.lims import *
from genologics.config import BASEURI, USERNAME, PASSWORD
from genologics.descriptors import StringDescriptor,EntityDescriptor

# import xml.etree.ElementTree as ET
# from argparse import ArgumentParser

lims= Lims(BASEURI, USERNAME, PASSWORD)

# A full list of LIMS steps with involved instrument and details for logging
def categorization(process_name):
    record={
        "Adapter ligation and reverse transcription (TruSeq small RNA) 1.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Adapter Ligation and 1st Amplification (SMARTer Pico) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv","PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Aliquot Samples for Caliper/Bioanalyzer" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Aliquot Samples for Qubit/Bioanalyzer" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Aliquot Libraries for Hybridization (SS XT)" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Amplify Adapter-Ligated Library (SS XT) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv", "PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Amplify Captured Libraries to Add Index Tags (SS XT) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv","PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Amplify by PCR and Add Index Tags (TruSeq small RNA) 1.0" : {"dest_file" : ["PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Automated Quant-iT QC (DNA) 4.0" : {"dest_file" : ["Tecan_LIMS_logbook.csv"], "instrument" : ["default"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Automated Quant-iT QC (Library Validation) 4.0" : {"dest_file" : ["Tecan_LIMS_logbook.csv"], "instrument" : ["default"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Automated Quant-iT QC (RNA) 4.0" : {"dest_file" : ["Tecan_LIMS_logbook.csv"], "instrument" : ["default"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Bioanalyzer Fragmentation QC (TruSeq DNA) 4.0" : {"dest_file" : ["Bioanalyzer_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : ""}]},
        "Bioanalyzer QC (Library Validation) 4.0" : {"dest_file" : ["Bioanalyzer_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : ""}]},
        "Bioanalyzer QC (DNA) 4.0" : {"dest_file" : ["Bioanalyzer_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : ""}]},
        "Bioanalyzer QC (RNA) 4.0" : {"dest_file" : ["Bioanalyzer_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent kit" : "", "udf_Lot no: Ladder" : ""}]},
        "CA Purification" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "CaliperGX QC (DNA)" : {"dest_file" : ["Caliper_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : ""}]},
        "CaliperGX QC (RNA)" : {"dest_file" : ["Caliper_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Lot no: Chip" : "", "udf_Lot no: Reagent Kit" : "", "udf_Lot no: RNA ladder" : ""}]},
        "Capture And Wash (SS XT) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Cluster Generation (Illumina SBS) 4.0" : {"dest_file" : ["cBot_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "End Repair, A-Tailing and Adapter Ligation (SS XT) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv", "PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "End repair, size selection, A-tailing and adapter ligation (Lucigen NxSeq DNA) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "End repair, size selection, A-tailing and adapter ligation (TruSeq DNA Nano) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "End repair, size selection, A-tailing and adapter ligation (TruSeq PCR-free DNA) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv", "PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "End repair, A-tailing and adapter ligation (TruSeq RNA) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv", "PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Enrich DNA fragments (Nextera) 4.0" : {"dest_file" : ["PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Enrich DNA fragments (TruSeq DNA) 4.0" : {"dest_file" : ["PCR_LIMS_logbook.csv", "Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_Bravo"], "details" : ["", ""]},
        "Enrich DNA fragments (TruSeq RNA) 4.0" : {"dest_file" : ["PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Fragment Analyzer QC (DNA) 4.0" : {"dest_file" : ["FragmentAnalyzer_LIMS_logbook.csv"], "instrument" : ["default"], "details": [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]},
        "Fragment Analyzer QC (Library Validation) 4.0" : {"dest_file" : ["FragmentAnalyzer_LIMS_logbook.csv"], "instrument" : ["default"], "details": [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]},
        "Fragment Analyzer QC (RNA) 4.0" : {"dest_file" : ["FragmentAnalyzer_LIMS_logbook.csv", "instrument"] : ["default"], "details": [{"udf_Lot no: Fragment Analyzer Reagents" : ""}]},
        "Fragment DNA (ThruPlex)" : {"dest_file" : [["CovarisS2_LIMS_logbook.csv","CovarisE220_LIMS_logbook.csv"]], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "Fragment DNA (TruSeq DNA) 4.0" : {"dest_file" : [["CovarisS2_LIMS_logbook.csv","CovarisE220_LIMS_logbook.csv"]], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "Fragmentation & cDNA synthesis (SMARTer Pico) 4.0" : {"dest_file" : ["PCR_LIMS_logbook.csv"], "instrument" : ["udf_PCR Machine"], "details" : [""]},
        "Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : {"dest_file" : ["PCR_LIMS_logbook.csv"], "instrument" : ["udf_PCR Machine"], "details" : [""]},
        "Hybridize Library (SS XT) 4.0" : {"dest_file" : ["PCR_LIMS_logbook.csv", "Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_Instrument Used"], "details" : ["", ""]},
        "Library Normalization (HiSeq X) 1.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Normalization (Illumina SBS) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Normalization (MiSeq) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (HiSeq X) 1.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (Illumina SBS) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (MiSeq) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (RAD-seq) v1.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Library Pooling (TruSeq Small RNA) 1.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Linear DNA digestion, Circularized DNA shearing and Streptavidin Bead Binding" : {"dest_file" : [["CovarisS2_LIMS_logbook.csv","CovarisE220_LIMS_logbook.csv"]], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "mRNA Purification, Fragmentation & cDNA synthesis (TruSeq RNA) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Pre-Pooling (Illumina SBS) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Pre-Pooling (MiSeq) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Purification (ThruPlex)" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "qPCR QC (Dilution Validation) 4.0" : {"dest_file" : ["CFX_LIMS_logbook.csv", "Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_Instrument Used"], "details": [{"udf_Lot no. qPCR reagent kit" : "", "udf_Lot no. Standard" : ""}, ""]},
        "qPCR QC (Library Validation) 4.0" : {"dest_file" : ["CFX_LIMS_logbook.csv", "Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_Instrument Used"], "details": [{"udf_Lot no. qPCR reagent kit" : "", "udf_Lot no. Standard" : ""}, ""]},
        "Quant-iT QC (DNA) 4.0" : {"dest_file" : ["CFX_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Quant-iT QC (Library Validation) 4.0" : {"dest_file" : ["CFX_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Quant-iT QC (RNA) 4.0" : {"dest_file" : ["CFX_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay type" : "", "udf_Lot no: Quant-iT reagent kit" : ""}]},
        "Qubit QC (DNA) 4.0" : {"dest_file" : ["Qubit_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "Qubit QC (RNA) 4.0" : {"dest_file" : ["Qubit_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "Qubit QC (Dilution Validation) 4.0" : {"dest_file" : ["Qubit_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "Qubit QC (Library Validation) 4.0" : {"dest_file" : ["Qubit_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details": [{"udf_Assay" : "", "udf_Lot no: Qubit kit" : ""}]},
        "RAD-seq Library Indexing v1.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv","PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Ribosomal cDNA Depletion and 2nd Amplification (SMARTer Pico) 4.0" : {"dest_file" : ["Bravo_LIMS_logbook.csv","PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "RiboZero depletion" : {"dest_file" : ["Bravo_LIMS_logbook.csv","PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "Setup Workset/Plate" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Size Selection (Pippin)" : {"dest_file" : ["Pippin_LIMS_logbook.csv"], "instrument" : ["default"], "details": [{"udf_Type: Gel Cassette" : "", "udf_Lot no: Gel Cassette" : "", "udf_Type: Marker" : "", "udf_Lot no: Marker" : "", "udf_Lot no: Electrophoresis Buffer" : ""}]},
        "Size Selection (Robocut)" : {"dest_file" : ["Bravo_LIMS_logbook.csv"], "instrument" : ["lims_instrument"], "details" : [""]},
        "Shear DNA (SS XT) 4.0" : {"dest_file" : [["CovarisS2_LIMS_logbook.csv","CovarisE220_LIMS_logbook.csv"]], "instrument" : ["lims_instrument"], "details" : ["udf_Lot no: Covaris tube"]},
        "ThruPlex library amplification" : {"dest_file" : ["Bravo_LIMS_logbook.csv","PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]},
        "ThruPlex template preparation and synthesis" : {"dest_file" : ["Bravo_LIMS_logbook.csv","PCR_LIMS_logbook.csv"], "instrument" : ["lims_instrument", "udf_PCR Machine"], "details" : ["", ""]}
    }
    return record[process_name]

# TBD: fetching instrument information
def get_instrument(pid):
    return instrument_Name

# TBD: writing log in GDoc electronic logbook
def write_record(content,logbook):

# All logics about logging
def main():
    pro=Process(lims, id=args.pid)
    log=[]
    time=pro.date_run
    log.append(time)
    user="{0} {1}".format(pro.technician.first_name,pro.technician.last_name)
    log.append(user)
    record=categorization(pro.type.name)

    for instrument in record["instrument"]:

        instrument_number=record["instrument"].index(instrument)

        if instrument == "default":
            if record["details"][instrument_number] is not '':
                for item in record["details"][instrument_number]:
                    log.append(pro.udf[item[3:]])
                write_record(log,record["dest_file"][instrument_number])

        elif instrument == "lims_instrument":
            instrument_name = get_instrument(args.pid)
            if instrument_name == "Manual operation":
                break
            elif instrument_name == "CovarisS2":
                if record["details"][instrument_number] is not '':
                    for item in record["details"][instrument_number]:
                        log.append(pro.udf[item[3:]])
                    write_record(log,record["dest_file"][instrument_number][0])
            elif instrument_name == "CovarisE220":
                if record["details"][instrument_number] is not '':
                    for item in record["details"][instrument_number]:
                        log.append(pro.udf[item[3:]])
                    write_record(log,record["dest_file"][instrument_number][1])
            else:
                log.append(instrument_name)
                for item in record["details"][instrument_number]:
                    log.append(pro.udf[item[3:]])
                write_record(log,record["dest_file"][instrument_number])

        elif instrument[0:3] == "udf":
            if pro.udf[instrument[3:]] is not '':
                instrument_name = pro.udf[instrument[3:]]
                log.append(instrument_name)
                for item in record["details"][instrument_number]:
                    log.append(pro.udf[item[3:]])
                write_record(log,record["dest_file"][instrument_number])
            else:
                break
