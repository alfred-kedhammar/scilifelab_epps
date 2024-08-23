#!/usr/bin/env python

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

from scilifelab_epps.epp import upload_file
from scilifelab_epps.wrapper import epp_decorator
from scripts.generate_minknow_samplesheet import get_pool_sample_label_mapping

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")
LABEL_SEQ_SUBSTRING = re.compile(r"[ACGT]{4,}(-[ACGT]{4,})?")


def get_runValues_section(process: Process, file_name: str) -> str:
    """Generate the [RUNVALUES] section of the AVITI run manifest and return it as a string."""

    # TODO master step fields for read recipe?

    runValues_section = "\n".join(
        [
            "[RUNVALUES]",
            "KeyName, Value",
            f"lims_step_name, {safe_string(process.type.name)}",
            f"lims_step_id, {process.id}",
            f"lims_step_operator, {process.technician.name}",
            f"file_name, {safe_string(file_name)}",
            f"file_timestamp, {TIMESTAMP}",
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


def get_samples_section(process: Process) -> str:
    """Generate the [SAMPLES] section of the AVITI run manifest and return it as a string."""

    # Get the analytes placed into the flowcell
    arts_out = [op for op in process.all_outputs() if op.type == "Analyte"]

    # Check whether lanes are individually addressable
    lanes_used = set([art_out.location[1].split(":")[1] for art_out in arts_out])
    ungrouped_lanes = True if len(lanes_used) == 2 else False
    logging.info(f"Individually addressable lanes: {ungrouped_lanes}")

    # Iterate over pools
    all_rows = []
    for art_out in arts_out:
        logging.info(f"Iterating over pool '{art_out.id}'...")

        lane_rows = []
        assert (
            art_out.container.type.name == "AVITI Flow Cell"
        ), "Unsupported container type."
        assert (
            len(art_out.samples) > 1 and len(art_out.reagent_labels) > 1
        ), "Not a pool."
        assert len(art_out.samples) == len(
            art_out.reagent_labels
        ), "Unequal number of samples and reagent labels."

        lane: str = art_out.location[1].split(":")[1]
        sample2label: dict[str, str] = get_pool_sample_label_mapping(art_out)
        samples = art_out.samples
        labels = art_out.reagent_labels

        assert len(set(labels)) == len(labels), "Detected non-unique reagent labels."

        # Iterate over samples
        for sample in samples:
            lims_label = sample2label[sample.name]

            # Parse sample index
            label_seq_match = re.search(LABEL_SEQ_SUBSTRING, lims_label)
            assert (
                label_seq_match is not None
            ), f"Could not parse label sequence from {lims_label}"
            label_seq = label_seq_match.group(0)

            if "-" in label_seq:
                index1, index2 = label_seq.split("-")
            else:
                index1 = label_seq
                index2 = ""

            row = {}
            row["SampleName"] = sample.name
            row["Index1"] = index1
            row["Index2"] = index2
            if ungrouped_lanes:
                row["Lane"] = lane

            lane_rows.append(row)

        # Add PhiX controls
        # TODO read from master step field
        for phix_idx_pair in [
            ("ACGTGTAGC", "GCTAGTGCA"),
            ("CACATGCTG", "AGACACTGT"),
            ("GTACACGAT", "CTCGTACAG"),
            ("TGTGCATCA", "TAGTCGATC"),
        ]:
            row = {}
            row["SampleName"] = "PhiX"
            row["Index1"] = phix_idx_pair[0]
            row["Index2"] = phix_idx_pair[1]
            if ungrouped_lanes:
                row["Lane"] = lane
            lane_rows.append(row)

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


def safe_string(s: str) -> str:
    """Wrap a string in quotes if it contains commas."""
    if "," in s:
        return f'"{s}"'
    else:
        return s


@epp_decorator(script_path=__file__, timestamp=TIMESTAMP)
def main(args: Namespace):
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    process = Process(lims, id=args.pid)

    file_name = f"AVITI_run_manifest_{process.id}_{TIMESTAMP}_{process.technician.name.replace(' ','')}.csv"

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
            f"/srv/ngi-nas-ns/samplesheets/AVITI/{dt.now().year}/{file_name}",
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
