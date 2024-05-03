#!/usr/bin/env python

import json
import os
import re
import sys
from argparse import ArgumentParser

import psycopg2
import yaml
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process, Project
from genologics.lims import Lims

from data.Chromium_10X_indexes import Chromium_10X_indexes
from scilifelab_epps.epp import attach_file

SMARTSEQ3_indexes_json = (
    "/opt/gls/clarity/users/glsai/repos/scilifelab_epps/data/SMARTSEQ3_indexes.json"
)

with open(SMARTSEQ3_indexes_json) as file:
    SMARTSEQ3_indexes = json.loads(file.read())

with open("/opt/gls/clarity/users/glsai/config/genosqlrc.yaml") as f:
    config = yaml.safe_load(f)

DESC = """EPP used to check index distance in library pool
Author: Chuan Wang, Science for Life Laboratory, Stockholm, Sweden
"""

# Pre-compile regexes in global scope:
IDX_PAT = re.compile("([ATCG]{4,}N*)-?([ATCG]*)")
TENX_SINGLE_PAT = re.compile("SI-(?:GA|NA)-[A-H][1-9][0-2]?")
TENX_DUAL_PAT = re.compile("SI-(?:TT|NT|NN|TN|TS)-[A-H][1-9][0-2]?")
SMARTSEQ_PAT = re.compile("SMARTSEQ[1-9]?-[1-9][0-9]?[A-P]")
NGISAMPLE_PAT = re.compile("P[0-9]+_[0-9]+")


def verify_indexes(data):
    message = []
    pools = {x["pool"] for x in data}
    for p in sorted(pools):
        subset = [i for i in data if i["pool"] == p]
        subset = sorted(subset, key=lambda d: d["sn"])
        if len(subset) == 1:
            continue
        idx_length = set()
        for i, sample_a in enumerate(subset[:-1]):
            idx_a = sample_a.get("idx1", "") + "-" + sample_a.get("idx2", "")
            idx_length.add(len(idx_a))
            if sample_a.get("idx1", "") == "" and sample_a.get("idx2", "") == "":
                message.append(
                    "INDEX WARNING: Sample {} in pool {} has no index".format(
                        sample_a.get("sn", ""), p
                    )
                )
            j = i + 1
            for sample_b in subset[j:]:
                idx_b = sample_b.get("idx1", "") + "-" + sample_b.get("idx2", "")
                if idx_a == idx_b:
                    message.append(
                        "INDEX WARNING: Same index {} for samples {} and {} in pool {}".format(
                            idx_a, sample_a.get("sn", ""), sample_b.get("sn", ""), p
                        )
                    )
        sample_last = subset[-1]
        idx_last = sample_last.get("idx1", "") + "-" + sample_last.get("idx2", "")
        idx_length.add(len(idx_last))
        if sample_last.get("idx1", "") == "" and sample_last.get("idx2", "") == "":
            message.append(
                "INDEX WARNING: Sample {} in pool {} has no index".format(
                    sample_last.get("sn", ""), p
                )
            )
        if len(idx_length) > 1:
            message.append(f"INDEX WARNING: Multiple index lengths noticed in pool {p}")
    return message


def verify_orientation(data):
    message = []
    connection = psycopg2.connect(
        user=config["username"],
        host=config["url"],
        database=config["db"],
        password=config["password"],
    )
    cursor = connection.cursor()
    query = (
        "select reagenttype.name from reagenttype " "where reagenttype.name like '{}%';"
    )
    # We only search against part of the index sets that exist in LIMS
    index_sets_10nt = [
        "AmpliconUD_UDP_",
        "IDT_10nt_UD_",
        "NexteraUD_UDP_",
        "QIAseq_UX_UDI_",
        "v2_IDT_10nt_UD_",
        "v3_Illumina_10nt_UD_",
        "xGen_Normalase_10nt_UDI_",
        "%_SI-NN-",
        "%_SI-NT-",
        "%_SI-TN-",
        "%_SI-TS-",
        "%_SI-TT-",
    ]
    index_sets_8nt = [
        "NexteraCD",
        "NexteraXT_",
        "Nextera16S_",
        "Nextera FS Dual",
        "SMARTerDNA_",
        "SMARTerV2_",
        "SMARTer_RNA_UD_",
        "Swift_SNAP_",
        "TAKARA_8nt_UDI_",
        "TruSeqUDv2-UDI_",
        "v2_Illumina_TruSeq_8nt_UD_",
        "v2_NexteraXT_",
        "xGen_8nt_UDI_",
    ]
    pools = {x["pool"] for x in data}
    for p in sorted(pools):
        subset = [i for i in data if i["pool"] == p]
        subset = sorted(subset, key=lambda d: d["sn"])
        if NGISAMPLE_PAT.findall(subset[0].get("sn", "")):
            project_id = subset[0]["sn"].split("_")[0]
            project_info = Project(lims, id=project_id)
            seq_platform = project_info.udf.get("Sequencing platform")
        else:
            # The error message is skipped here since the verify_samplename function will check the names of all samples
            seq_platform = ""
        idx1_len = list(set([len(i["idx1"]) for i in subset if i["idx1"]]))
        idx2_len = list(set([len(i["idx2"]) for i in subset if i["idx2"]]))
        if len(idx1_len) == len(idx2_len) == 1 and idx1_len[0] == idx2_len[0] == 8:
            search_index_sets = index_sets_8nt
        elif len(idx1_len) == len(idx2_len) == 1 and idx1_len[0] == idx2_len[0] == 10:
            search_index_sets = index_sets_10nt
        else:
            message.append(
                f"Unable to check index orientations due to index length for pool {p}"
            )
            continue
        # Search through the index sets for the first and last samples in the pool to save time
        flag_idx_search = False
        for idx_set in search_index_sets:
            cursor.execute(query.format(idx_set))
            query_output = cursor.fetchall()
            flag_first_sample = ""
            flag_last_sample = ""
            for out in query_output:
                index1 = IDX_PAT.findall(out[0])[0][0]
                index2 = IDX_PAT.findall(out[0])[0][1]
                # Convert index 2 to RC for MiSeq projects
                if seq_platform:
                    if "MISEQ" in seq_platform.upper():
                        index2 = rc(index2)
                # Check the first sample
                if subset[0]["idx1"] == index1 and subset[0]["idx2"] == index2:
                    flag_first_sample = "CORRECT"
                elif subset[0]["idx1"] == rc(index1) and subset[0]["idx2"] == index2:
                    flag_first_sample = "Index1_RC"
                elif subset[0]["idx1"] == index1 and subset[0]["idx2"] == rc(index2):
                    flag_first_sample = "Index2_RC"
                elif subset[0]["idx1"] == rc(index1) and subset[0]["idx2"] == rc(
                    index2
                ):
                    flag_first_sample = "Index1_and_Index2_RC"
                # Check the last sample
                if subset[-1]["idx1"] == index1 and subset[-1]["idx2"] == index2:
                    flag_last_sample = "CORRECT"
                elif subset[-1]["idx1"] == rc(index1) and subset[-1]["idx2"] == index2:
                    flag_last_sample = "Index1_RC"
                elif subset[-1]["idx1"] == index1 and subset[-1]["idx2"] == rc(index2):
                    flag_last_sample = "Index2_RC"
                elif subset[-1]["idx1"] == rc(index1) and subset[-1]["idx2"] == rc(
                    index2
                ):
                    flag_last_sample = "Index1_and_Index2_RC"
            # Make a conclusion
            if flag_first_sample == flag_last_sample == "CORRECT":
                flag_idx_search = True
                break
            elif flag_first_sample == flag_last_sample == "Index1_RC":
                message.append(
                    f"Seems that Index 1 needs to be converted to RC for pool {p}"
                )
                flag_idx_search = True
                break
            elif flag_first_sample == flag_last_sample == "Index2_RC":
                message.append(
                    f"Seems that Index 2 needs to be converted to RC for pool {p}"
                )
                flag_idx_search = True
                break
            elif flag_first_sample == flag_last_sample == "Index1_and_Index2_RC":
                message.append(
                    f"Seems that both Index 1 and Index 2 need to be converted to RC for pool {p}"
                )
                flag_idx_search = True
                break
            elif flag_first_sample != flag_last_sample:
                message.append(f"Inconsistent Index pattern detected for pool {p}")
                flag_idx_search = True
                break
        if not flag_idx_search:
            message.append(
                f"Unable to find matched index set to check orientation for pool {p}"
            )

    # Close connections
    if connection:
        cursor.close()
        connection.close()

    return message


def rc(sequence):
    compl = {"A": "T", "C": "G", "G": "C", "T": "A"}
    rc_sequence = "".join(
        reversed([compl.get(b, b) for b in sequence.replace(",", "").upper()])
    )
    return rc_sequence


def verify_placement(data):
    message = []
    pools = {x["pool"] for x in data}
    for p in sorted(pools):
        subset = [i for i in data if i["pool"] == p]
        subset = sorted(subset, key=lambda d: d["sn"])
        for sample in subset:
            if sample.get("step_container_name", "") != sample.get(
                "submitted_container_name", ""
            ):
                message.append(
                    "PLACEMENT WARNING: Sample {} in pool {} is placed in container {} which is different than the submitted container {}".format(
                        sample.get("sn", ""),
                        p,
                        sample.get("step_container_name", ""),
                        sample.get("submitted_container_name", ""),
                    )
                )
            if sample.get("step_pool_well", "") != sample.get(
                "submitted_pool_well", ""
            ):
                message.append(
                    "PLACEMENT WARNING: Sample {} in pool {} is placed in well {} which is different than the submitted well {}".format(
                        sample.get("sn", ""),
                        p,
                        sample.get("step_pool_well", ""),
                        sample.get("submitted_pool_well", ""),
                    )
                )
    return message


def verify_samplename(data):
    message = []
    pools = {x["pool"] for x in data}
    for p in sorted(pools):
        subset = [i for i in data if i["pool"] == p]
        subset = sorted(subset, key=lambda d: d["sn"])
        for sample in subset:
            if not NGISAMPLE_PAT.findall(sample.get("sn", "")):
                message.append(
                    "SAMPLE NAME WARNING: Bad sample name format {}".format(
                        sample.get("sn", "")
                    )
                )
            else:
                if sample.get("sn", "").split("_")[0] != sample.get("proj_id", ""):
                    message.append(
                        "SAMPLE NAME WARNING: Sample name {} does not match project ID {}".format(
                            sample.get("sn", ""), sample.get("proj_id", "")
                        )
                    )
    return message


def check_index_distance(data):
    message = []
    pools = {x["pool"] for x in data}
    for p in sorted(pools):
        subset = [i for i in data if i["pool"] == p]
        if len(subset) == 1:
            continue
        for i, sample_a in enumerate(subset[:-1]):
            if sample_a.get("idx1", "") == "" and sample_a.get("idx2", "") == "":
                message.append(
                    "NO INDEX ERROR: Sample {} in pool {} has no index".format(
                        sample_a.get("sn", ""), p
                    )
                )
            j = i + 1
            for sample_b in subset[j:]:
                d = 0
                if sample_a.get("idx1", "") and sample_b.get("idx1", ""):
                    d += my_distance(sample_a["idx1"], sample_b["idx1"])
                if sample_a.get("idx2", "") and sample_b.get("idx2", ""):
                    d += my_distance(sample_a["idx2"], sample_b["idx2"])
                if d == 0:
                    idx_a = sample_a.get("idx1", "") + "-" + sample_a.get("idx2", "")
                    idx_b = sample_b.get("idx1", "") + "-" + sample_b.get("idx2", "")
                    message.append(
                        "INDEX COLLISION ERROR: {} for sample {} and {} for sample {} in pool {}".format(
                            idx_a,
                            sample_a.get("sn", ""),
                            idx_b,
                            sample_b.get("sn", ""),
                            p,
                        )
                    )
                if d == 1:
                    idx_a = sample_a.get("idx1", "") + "-" + sample_a.get("idx2", "")
                    idx_b = sample_b.get("idx1", "") + "-" + sample_b.get("idx2", "")
                    message.append(
                        "SIMILAR INDEX WARNING: {} for sample {} and {} for sample {} in pool {}".format(
                            idx_a,
                            sample_a.get("sn", ""),
                            idx_b,
                            sample_b.get("sn", ""),
                            p,
                        )
                    )
        sample_last = subset[-1]
        if sample_last.get("idx1", "") == "" and sample_last.get("idx2", "") == "":
            message.append(
                "NO INDEX ERROR: Sample {} in pool {} has no index".format(
                    sample_last.get("sn", ""), p
                )
            )
    return message


def my_distance(idx_a, idx_b):
    diffs = 0
    short = min((idx_a, idx_b), key=len)
    lon = idx_a if short == idx_b else idx_b
    for i, c in enumerate(short):
        if c != lon[i]:
            diffs += 1
    return diffs


def prepare_index_table(process):
    data = []
    message = []
    for out in process.all_outputs():
        if out.type == "Analyte":
            pool_name = out.name
            step_container_name = out.container.name
            step_pool_well = out.location[1]
            for sample in out.samples:
                try:
                    proj_id = sample.project.id
                except AttributeError:
                    proj_id = "P0000"
                submitted_container_name = ""
                submitted_pool_well = ""
                if process.type.name == "Library Pooling (Finished Libraries) 4.0":
                    submitted_container_name = sample.artifact.container.name.split(
                        "-"
                    )[0]
                    try:
                        submitted_pool_well_row = re.findall(
                            "[a-zA-Z]+", sample.artifact.container.name.split("-")[2]
                        )[0]
                        submitted_pool_well_col = re.findall(
                            "[0-9]+", sample.artifact.container.name.split("-")[2]
                        )[0]
                        submitted_pool_well = (
                            submitted_pool_well_row + ":" + submitted_pool_well_col
                        )
                    except IndexError:
                        submitted_pool_well = sample.artifact.container.name.split("-")[
                            2
                        ]
                sample_idxs = set()
                find_barcode(sample_idxs, sample, process)
                if sample_idxs:
                    for idxs in sample_idxs:
                        sp_obj = {}
                        sp_obj["step_container_name"] = step_container_name
                        sp_obj["step_pool_well"] = step_pool_well
                        sp_obj["submitted_container_name"] = submitted_container_name
                        sp_obj["submitted_pool_well"] = submitted_pool_well
                        if idxs[0] == "NoIndex":
                            sp_obj["pool"] = pool_name
                            sp_obj["proj_id"] = proj_id
                            sp_obj["sn"] = sample.name.replace(",", "")
                            sp_obj["idx1"] = ""
                            sp_obj["idx2"] = ""
                            data.append(sp_obj)
                        elif TENX_DUAL_PAT.findall(idxs[0]):
                            sp_obj["pool"] = pool_name
                            sp_obj["proj_id"] = proj_id
                            sp_obj["sn"] = sample.name.replace(",", "")
                            sp_obj["idx1"] = Chromium_10X_indexes[
                                TENX_DUAL_PAT.findall(idxs[0])[0]
                            ][0].replace(",", "")
                            sp_obj["idx2"] = Chromium_10X_indexes[
                                TENX_DUAL_PAT.findall(idxs[0])[0]
                            ][1].replace(",", "")
                            data.append(sp_obj)
                        elif TENX_SINGLE_PAT.findall(idxs[0]):
                            for tenXidx in Chromium_10X_indexes[
                                TENX_SINGLE_PAT.findall(idxs[0])[0]
                            ]:
                                sp_obj_sub = {}
                                sp_obj_sub["pool"] = pool_name
                                sp_obj_sub["proj_id"] = proj_id
                                sp_obj_sub["sn"] = sample.name.replace(",", "")
                                sp_obj_sub["idx1"] = tenXidx.replace(",", "")
                                sp_obj_sub["idx2"] = ""
                                data.append(sp_obj_sub)
                        elif SMARTSEQ_PAT.findall(idxs[0]):
                            for i7_idx in SMARTSEQ3_indexes[idxs[0]][0]:
                                for i5_idx in SMARTSEQ3_indexes[idxs[0]][1]:
                                    sp_obj_sub = {}
                                    sp_obj_sub["pool"] = pool_name
                                    sp_obj_sub["proj_id"] = proj_id
                                    sp_obj_sub["sn"] = sample.name.replace(",", "")
                                    sp_obj_sub["idx1"] = i7_idx
                                    sp_obj_sub["idx2"] = i5_idx
                                    data.append(sp_obj_sub)
                        else:
                            sp_obj["pool"] = pool_name
                            sp_obj["proj_id"] = proj_id
                            sp_obj["sn"] = sample.name.replace(",", "")
                            sp_obj["idx1"] = idxs[0].replace(",", "") if idxs[0] else ""
                            sp_obj["idx2"] = idxs[1].replace(",", "") if idxs[1] else ""
                            data.append(sp_obj)
                else:
                    sp_obj = {}
                    sp_obj["step_container_name"] = step_container_name
                    sp_obj["step_pool_well"] = step_pool_well
                    sp_obj["submitted_container_name"] = submitted_container_name
                    sp_obj["submitted_pool_well"] = submitted_pool_well
                    sp_obj["pool"] = pool_name
                    sp_obj["proj_id"] = proj_id
                    sp_obj["sn"] = sample.name.replace(",", "")
                    sp_obj["idx1"] = ""
                    sp_obj["idx2"] = ""
                    data.append(sp_obj)
    return data, message


def find_barcode(sample_idxs, sample, process):
    # print "trying to find {} barcode in {}".format(sample.name, process.name)
    for art in process.all_inputs():
        if sample in art.samples:
            if len(art.samples) == 1 and art.reagent_labels:
                if process.type.name == "Library Pooling (Finished Libraries) 4.0":
                    if len(art.reagent_labels) > 1:
                        sys.stderr.write(
                            f"INDEX FORMAT ERROR: Sample {sample.name} has a bad format or unknown index category\n"
                        )
                        sys.exit(2)
                    else:
                        reagent_label_name = art.reagent_labels[0].upper()
                        if reagent_label_name and reagent_label_name != "NOINDEX":
                            if (
                                IDX_PAT.findall(reagent_label_name)
                                and len(IDX_PAT.findall(reagent_label_name)) > 1
                            ) or (
                                not (
                                    IDX_PAT.findall(reagent_label_name)
                                    or TENX_SINGLE_PAT.findall(reagent_label_name)
                                    or TENX_DUAL_PAT.findall(reagent_label_name)
                                    or SMARTSEQ_PAT.findall(reagent_label_name)
                                )
                            ):
                                sys.stderr.write(
                                    f"INDEX FORMAT ERROR: Sample {sample.name} has a bad format or unknown index category\n"
                                )
                                sys.exit(2)
                reagent_label_name = art.reagent_labels[0].upper().replace(" ", "")
                idxs = (
                    TENX_SINGLE_PAT.findall(reagent_label_name)
                    or TENX_DUAL_PAT.findall(reagent_label_name)
                    or SMARTSEQ_PAT.findall(reagent_label_name)
                )
                if idxs:
                    # Put in tuple with empty string as second index to
                    # match expected type:
                    sample_idxs.add((idxs[0], ""))
                else:
                    try:
                        idxs = IDX_PAT.findall(reagent_label_name)[0]
                        sample_idxs.add(idxs)
                    except IndexError:
                        try:
                            # we only have the reagent label name.
                            rt = lims.get_reagent_types(name=reagent_label_name)[0]
                            idxs = IDX_PAT.findall(rt.sequence)[0]
                            sample_idxs.add(idxs)
                        except:
                            sample_idxs.add(("NoIndex", ""))
            else:
                if art == sample.artifact or not art.parent_process:
                    pass
                else:
                    find_barcode(sample_idxs, sample, art.parent_process)


def main(lims, pid, auto):
    process = Process(lims, id=pid)
    tech_username = process.technician.username
    data, message = prepare_index_table(process)
    if process.type.name == "Library Pooling (Finished Libraries) 4.0":
        message += verify_placement(data)
        message += verify_indexes(data)
        message += verify_samplename(data)
        message += verify_orientation(data)
    else:
        message = check_index_distance(data)
    warning_start = "**Warnings from Verify Indexes and Placement EPP: **\n"
    warning_end = "== End of Verify Indexes and Placement EPP warnings =="
    if message:
        if auto:
            print("; ".join(message), file=sys.stderr)
        else:
            sys.stderr.write("; ".join(message))
        if process.type.name == "Library Pooling (Finished Libraries) 4.0":
            if not process.udf.get("Comments"):
                process.udf["Comments"] = (
                    warning_start
                    + "\n".join(message)
                    + f"\n@{tech_username}\n"
                    + warning_end
                )
            else:
                start_index = process.udf["Comments"].find(warning_start)
                end_index = process.udf["Comments"].rfind(warning_end)
                # No existing warning message
                if start_index == -1:
                    process.udf["Comments"] += "\n\n"
                    process.udf["Comments"] += warning_start
                    process.udf["Comments"] += "\n".join(message)
                    process.udf["Comments"] += f"\n@{tech_username}\n"
                    process.udf["Comments"] += warning_end
                # Update warning message
                else:
                    process.udf["Comments"] = (
                        process.udf["Comments"][:start_index]
                        + process.udf["Comments"][end_index + len(warning_end) + 1 :]
                    )
                    if not process.udf.get("Comments"):
                        process.udf["Comments"] = (
                            warning_start
                            + "\n".join(message)
                            + f"\n@{tech_username}\n"
                            + warning_end
                        )
                    else:
                        process.udf["Comments"] += warning_start
                        process.udf["Comments"] += "\n".join(message)
                        process.udf["Comments"] += f"\n@{tech_username}\n"
                        process.udf["Comments"] += warning_end
            process.put()
    else:
        print("No issue detected with indexes or placement", file=sys.stderr)
        message.append("No issue detected with indexes or placement")
        # Clear previous warning messages if the error has been corrected
        if process.udf.get("Comments"):
            start_index = process.udf["Comments"].find(warning_start)
            end_index = process.udf["Comments"].rfind(warning_end)
            process.udf["Comments"] = (
                process.udf["Comments"][:start_index]
                + process.udf["Comments"][end_index + len(warning_end) + 1 :]
            )
            process.put()

    with open("index_checker.log", "w") as logContext:
        logContext.write("\n".join(message))
    for out in process.all_outputs():
        if (
            out.name == "Check Index Distance Log"
            or out.name == "Verify Index and Placement Log"
        ):
            attach_file(os.path.join(os.getcwd(), "index_checker.log"), out)
    # Throw red warning message when it is not automatically run
    if not auto:
        sys.exit(2)


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    parser.add_argument(
        "--log",
        dest="log",
        help=(
            "File name for standard log file, " "for runtime information and problems."
        ),
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help=("Used when the script is running automatically in LIMS."),
    )
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()
    main(lims, args.pid, args.auto)
