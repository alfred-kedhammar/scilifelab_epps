#!/usr/bin/env python

import logging
from argparse import ArgumentParser, Namespace
from datetime import datetime as dt

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims
from Levenshtein import hamming as distance

from scilifelab_epps.wrapper import epp_decorator
from scripts.generate_minknow_samplesheet import get_pool_sample_label_mapping

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")


def get_samples_section(process: Process) -> str:
    """Generate the [Samples] section of the AVITI run manifest and return it as a string."""

    # Get the analytes placed into the flowcell
    arts_out = [op for op in process.all_outputs() if op.type == "Analyte"]

    # Assert that both flowcell lanes are filled
    assert set([art_out.location[1].split(":")[1] for art_out in arts_out]) == set(
        ["1", "2"]
    ), "Expected two populated lanes."

    # Iterate over pools
    all_rows = []
    for art_out in arts_out:
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

            # TODO add code here to parse reagent labels that do not only consist of sequences and dashes

            if "-" in lims_label:
                index1, index2 = lims_label.split("-")
            else:
                index1 = lims_label
                index2 = ""

            row = {}
            row["SampleName"] = sample.name
            row["Index1"] = index1
            row["Index2"] = index2
            row["Lane"] = lane

            lane_rows.append(row)

        # Add PhiX controls
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
            row["Lane"] = lane
            lane_rows.append(row)

        # Check for index collision within lane, across samples and PhiX
        check_distances(lane_rows)
        all_rows.extend(lane_rows)

    df = pd.DataFrame(all_rows)

    samples_section = f"[Samples]\n{df.to_csv(index=None, header=True)}"

    return samples_section


def revcomp(seq: str) -> str:
    """Reverse-complement a DNA string."""
    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


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
        # If the index lengths are equal, add a simple small visual representation
        if len(row["Index1"]) + len(row["Index2"]) == len(row_comp["Index1"]) + len(
            row_comp["Index2"]
        ):
            warning_lines.append(visualize_hamming(*compared_seqs.split()))

        warning = "\n".join(warning_lines)
        logging.warning(warning)

        # For identical collisions, kill the process
        if dist == 0:
            raise AssertionError("Identical indices detected.")


def visualize_hamming(seq1: str, seq2: str) -> str:
    """Visualize Hamming alignment"""

    assert len(seq1) == len(seq2)

    m = ""
    for seq1_base, seq2_base in zip(seq1, seq2):
        if seq1_base == seq2_base:
            m += "|"
        else:
            m += "X"

    lines = "\n".join([seq1, m, seq2])
    return lines


def check_distances(rows: list[dict]) -> None:
    for i in range(len(rows)):
        row = rows[i]

        for row_comp in rows[i + 1 :]:
            check_pair_distance(row, row_comp, dist_warning_threshold=4)


@epp_decorator(script_path=__file__, timestamp=TIMESTAMP)
def main(args: Namespace):
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    process = Process(lims, id=args.pid)

    logging.info("Starting to build run manifest.")

    samples_section = get_samples_section(process)


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
