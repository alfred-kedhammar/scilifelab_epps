#!/usr/bin/env python

import json
import logging
import os
import re
import shutil
from argparse import ArgumentParser, Namespace
from datetime import datetime as dt

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims
from Levenshtein import hamming as distance

from data.Chromium_10X_indexes import Chromium_10X_indexes
from scilifelab_epps.epp import upload_file
from scilifelab_epps.wrapper import epp_decorator
from scripts.generate_minknow_samplesheet import get_pool_sample_label_mapping

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")

# Pre-compile regexes in global scope:
IDX_PAT = re.compile("([ATCG]{4,}N*)-?([ATCG]*)")
TENX_SINGLE_PAT = re.compile("SI-(?:GA|NA)-[A-H][1-9][0-2]?")
TENX_DUAL_PAT = re.compile("SI-(?:TT|NT|NN|TN|TS)-[A-H][1-9][0-2]?")
SMARTSEQ_PAT = re.compile("SMARTSEQ[1-9]?-[1-9][0-9]?[A-P]")

# Set up Element PhiX control sets, keys are options in LIMS dropdown UDF
PHIX_SETS = {
    "PhiX Control Library, Adept": {
        "nickname": "PhiX_Adept",
        "indices": [
            ("ATGTCGCTAG", "CTAGCTCGTA"),
            ("CACAGATCGT", "ACGAGAGTCT"),
            ("GCACATAGTC", "GACTACTAGC"),
            ("TGTGTCGACA", "TGTCTGACAG"),
        ],
    },
    "Cloudbreak PhiX Control Library, Elevate": {
        "nickname": "PhiX_Elevate",
        "indices": [
            ("ACGTGTAGC", "GCTAGTGCA"),
            ("CACATGCTG", "AGACACTGT"),
            ("GTACACGAT", "CTCGTACAG"),
            ("TGTGCATCA", "TAGTCGATC"),
        ],
    },
    "Cloudbreak Freestyle PhiX Control, Third Party": {
        "nickname": "PhiX_Third",
        "indices": [
            ("ATGTCGCTAG", "CTAGCTCGTA"),
            ("CACAGATCGT", "ACGAGAGTCT"),
            ("GCACATAGTC", "GACTACTAGC"),
            ("TGTGTCGACA", "TGTCTGACAG"),
        ],
    },
}

# Load SS3 indexes
SMARTSEQ3_indexes_json = (
    "/opt/gls/clarity/users/glsai/repos/scilifelab_epps/data/SMARTSEQ3_indexes.json"
)
with open(SMARTSEQ3_indexes_json) as file:
    SMARTSEQ3_indexes = json.loads(file.read())


def get_flowcell_id(process: Process) -> str:
    flowcell_ids = [
        op.container.name for op in process.all_outputs() if op.type == "Analyte"
    ]

    assert len(set(flowcell_ids)) == 1, "Expected one flowcell ID."
    flowcell_id = flowcell_ids[0]

    if "-" in flowcell_id:
        logging.warning(
            f"Container name {flowcell_id} contains a dash, did you forget to set the name of the LIMS container to the flowcell ID?"
        )

    return flowcell_id


def get_runValues_section(process: Process, file_name: str) -> str:
    """Generate the [RUNVALUES] section of the AVITI run manifest and return it as a string."""

    read_recipe = "-".join(
        [
            str(process.udf.get("Read 1 Cycles", 0)),
            str(process.udf.get("Index Read 1", 0)),
            str(process.udf.get("Index Read 2", 0)),
            str(process.udf.get("Read 2 Cycles", 0)),
        ]
    )

    runValues_section = "\n".join(
        [
            "[RUNVALUES]",
            "KeyName, Value",
            f"lims_step_name, {sanitize(process.type.name)}",
            f"file_name, {sanitize(file_name)}",
            f"read_recipe, {read_recipe}",
        ]
    )

    return runValues_section


def get_settings_section() -> str:
    """Generate the [SETTINGS] section of the AVITI run manifest and return it as a string."""
    settings_section = "\n".join(
        [
            "[SETTINGS]",
            "SettingName, Value",
        ]
    )

    return settings_section


def idxs_from_label(label: str) -> list[str | tuple[str, str]]:
    """From a LIMS reagent label, return list whose elements are
    single indices or tuples of dual index pairs.
    """

    # Initialize result
    idxs = []

    # Expand 10X single indexes
    if TENX_SINGLE_PAT.findall(label):
        for tenXidx in Chromium_10X_indexes[TENX_SINGLE_PAT.findall(label)[0]]:
            idxs.append(tenXidx)
    # Case of 10X dual indexes
    elif TENX_DUAL_PAT.findall(label):
        i7_idx = Chromium_10X_indexes[TENX_DUAL_PAT.findall(label)[0][0]]
        i5_idx = Chromium_10X_indexes[TENX_DUAL_PAT.findall(label)[0][1]]
        idxs.append((i7_idx, revcomp(i5_idx)))
    # Case of SS3 indexes
    elif SMARTSEQ_PAT.findall(label):
        for i7_idx in SMARTSEQ3_indexes[label][0]:
            for i5_idx in SMARTSEQ3_indexes[label][1]:
                idxs.append((i7_idx, revcomp(i5_idx)))
    # NoIndex cases
    elif label.replace(",", "").upper() == "NOINDEX" or (
        label.replace(",", "").upper() == ""
    ):
        raise AssertionError("NoIndex cases not allowed.")
    # Ordinary indexes
    elif IDX_PAT.findall(label):
        idx_match = IDX_PAT.findall(label)[0]
        if "-" in idx_match:
            idx1, idx2 = idx_match.split("-")
            idxs.append((idx1, idx2))
        else:
            idxs.append(idx_match)
    else:
        raise AssertionError(f"Could not parse index from '{label}'.")

    return idxs


def get_samples_section(process: Process) -> str:
    """Generate the [SAMPLES] section of the AVITI run manifest and return it as a string."""

    # Assert output analytes loaded on flowcell
    arts_out = [op for op in process.all_outputs() if op.type == "Analyte"]
    assert (
        len(arts_out) == 1 or len(arts_out) == 2
    ), "Expected one or two output analytes."
    lanes = [art_out.location[1].split(":")[0] for art_out in arts_out]
    assert set(lanes) == {"1"} or set(lanes) == {
        "1",
        "2",
    }, "Expected a single-lane or dual-lane flowcell."

    # Iterate over pools
    all_rows = []
    for art_out, lane in zip(arts_out, lanes):
        lane_rows = []
        assert (
            "AVITI Flow Cell" in art_out.container.type.name
        ), f"Unsupported container type {art_out.container.type.name}."
        assert (
            len(art_out.samples) > 1 and len(art_out.reagent_labels) > 1
        ), "Not a pool."
        assert len(art_out.samples) == len(
            art_out.reagent_labels
        ), "Unequal number of samples and reagent labels."

        sample2label: dict[str, str] = get_pool_sample_label_mapping(art_out)
        assert len(set(art_out.reagent_labels)) == len(
            art_out.reagent_labels
        ), "Detected non-unique reagent labels."

        samples = art_out.samples
        # Iterate over samples
        for sample in samples:
            # Project name and sequencing setup
            if sample.project:
                project = sample.project.name.replace(".", "__").replace(",", "")
                seq_setup = sample.project.udf.get("Sequencing setup", "0-0")
            else:
                project = "Control"
                seq_setup = "0-0"

            # Add row(s), depending on index type
            lims_label = sample2label[sample.name]
            for idx in idxs_from_label(lims_label):
                row = {}
                row["SampleName"] = sample.name
                if isinstance(idx, tuple):
                    row["Index1"], row["Index2"] = idx
                else:
                    row["Index1"] = idx
                    row["Index2"] = ""
                row["Lane"] = lane
                row["Project"] = project
                row["Recipe"] = seq_setup

                lane_rows.append(row)

        # Add PhiX controls if added:
        phix_loaded: bool = art_out.udf["% phiX"] != 0
        phix_set_name = art_out.udf.get("Element PhiX Set", None)

        if phix_loaded:
            assert (
                phix_set_name is not None
            ), "PhiX controls loaded but no kit specified."

            phix_set = PHIX_SETS(phix_set_name)

            for phix_idx_pair in phix_set["indices"]:
                row = {}
                row["SampleName"] = phix_set["nickname"]
                row["Index1"] = phix_idx_pair[0]
                row["Index2"] = phix_idx_pair[1]
                row["Lane"] = lane
                row["Project"] = phix_set["nickname"]
                row["Recipe"] = "0-0"
                lane_rows.append(row)
        else:
            assert phix_set is None, "PhiX controls specified but not loaded."

        # Check for index collision within lane, across samples and PhiX
        check_distances(lane_rows)
        all_rows.extend(lane_rows)

    df = pd.DataFrame(all_rows)

    samples_section = f"[SAMPLES]\n{df.to_csv(index=None, header=True)}"

    return samples_section


def check_distances(rows: list[dict], dist_warning_threshold=3) -> None:
    for i in range(len(rows)):
        row = rows[i]

        for row_comp in rows[i + 1 :]:
            check_pair_distance(
                row, row_comp, dist_warning_threshold=dist_warning_threshold
            )


def check_pair_distance(
    row, row_comp, check_flips: bool = False, dist_warning_threshold: int = 3
):
    """Distance check between two index pairs.

    row                     dict   manifest row of sample A
    row_comp                dict   manifest row of sample B
    check_flips             bool   check all reverse-complement combinations
    dist_warning_threshold  int    trigger warning for distances at or below this value

    """

    if check_flips:
        flips = []
        for a1, _a1 in zip(
            [row["Index1"], revcomp(row["Index1"])], ["Index1", "Index1_rc"]
        ):
            for a2, _a2 in zip(
                [row["Index2"], revcomp(row["Index2"])], ["Index2", "Index2_rc"]
            ):
                for b1, _b1 in zip(
                    [row_comp["Index1"], revcomp(row_comp["Index1"])],
                    ["Index1", "Index1_rc"],
                ):
                    for b2, _b2 in zip(
                        [row_comp["Index2"], revcomp(row_comp["Index2"])],
                        ["Index2", "Index2_rc"],
                    ):
                        flips.append(
                            (
                                distance(a1, b1) + distance(a2, b2),
                                f"{a1}-{a2} {b1}-{b2}",
                                f"{_a1}-{_a2} {_b1}-{_b2}",
                            )
                        )
        dist, compared_seqs, flip_conf = min(flips, key=lambda x: x[0])

    else:
        dist = distance(
            row["Index1"] + row["Index2"], row_comp["Index1"] + row_comp["Index2"]
        )
        compared_seqs = (
            f"{row['Index1']}-{row['Index2']} {row_comp['Index1']}-{row_comp['Index2']}"
        )

    if dist <= dist_warning_threshold:
        # Build a warning message for the pair
        warning_lines = [
            f"Hamming distance {dist} between {row['SampleName']} and {row_comp['SampleName']}"
        ]
        # If the distance is derived from a flip, show the original and the flipped conformation
        if check_flips:
            warning_lines.append(
                f"Given: {row['Index1']}-{row['Index2']} <-> {row_comp['Index1']}-{row_comp['Index2']}"
            )
            warning_lines.append(f"Distance: {dist} when flipped to {flip_conf}")
        # If the index lengths are equal, add a simple visual representation
        if len(row["Index1"]) + len(row["Index2"]) == len(row_comp["Index1"]) + len(
            row_comp["Index2"]
        ):
            warning_lines.append(show_match(*compared_seqs.split()))

        warning = "\n".join(warning_lines)
        logging.warning(warning)

        # For identical collisions, kill the process
        if dist == 0:
            raise AssertionError("Identical indices detected.")


def revcomp(seq: str) -> str:
    """Reverse-complement a DNA string."""
    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def show_match(seq1: str, seq2: str) -> str:
    """Visualize base-by-base match between sequences of equal length."""

    assert len(seq1) == len(seq2)

    m = ""
    for seq1_base, seq2_base in zip(seq1, seq2):
        if seq1_base == seq2_base:
            m += "|"
        else:
            m += "X"

    lines = "\n".join([seq1, m, seq2])
    return lines


def sanitize(s: str) -> str:
    """Wrap a string in quotes if it contains commas."""
    if "," in s:
        return f'"{s}"'
    else:
        return s


@epp_decorator(script_path=__file__, timestamp=TIMESTAMP)
def main(args: Namespace):
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    process = Process(lims, id=args.pid)

    # Name manifest file
    flowcell_id = get_flowcell_id(process)
    file_name = f"AVITI_run_manifest_{flowcell_id}_{process.id}_{TIMESTAMP}_{process.technician.name.replace(' ','')}.csv"

    # Build manifest
    logging.info("Starting to build run manifest.")

    runValues_section = get_runValues_section(process, file_name)
    settings_section = get_settings_section()
    samples_section = get_samples_section(process)

    manifest = "\n\n".join([runValues_section, settings_section, samples_section])

    # Write manifest
    with open(file_name, "w") as f:
        f.write(manifest)

    # Upload manifest
    logging.info("Uploading run manifest to LIMS...")
    upload_file(
        file_name,
        args.file,
        process,
        lims,
    )

    logging.info("Moving run manifest to ngi-nas-ns...")
    try:
        shutil.copyfile(
            file_name,
            f"/srv/ngi-nas-ns/samplesheets/Aviti/{dt.now().year}/{file_name}",
        )
        os.remove(file_name)
    except:
        logging.error("Failed to move run manifest to ngi-nas-ns.", exc_info=True)
    else:
        logging.info("Run manifest moved to ngi-nas-ns.")


if __name__ == "__main__":
    # Parse args
    parser = ArgumentParser()
    parser.add_argument(
        "--pid",
        required=True,
        type=str,
        help="Lims ID for current Process.",
    )
    parser.add_argument(
        "--log",
        required=True,
        type=str,
        help="Which file slot to use for the script log.",
    )
    parser.add_argument(
        "--file",
        required=True,
        type=str,
        help="Which file slot to use for the run manifest.",
    )
    args = parser.parse_args()

    main(args)
