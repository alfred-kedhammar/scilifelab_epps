#!/usr/bin/env python

import logging
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, field
from datetime import datetime as dt

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims
from Levenshtein import hamming as distance

from scilifelab_epps.wrapper import epp_decorator
from scripts.generate_minknow_samplesheet import get_pool_sample_label_mapping

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")


@dataclass
class Row:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def write(self, f):
        for attr in self.__dict__.values():
            if isinstance(attr, str) and "," in attr:
                f.write(f'"{attr}", ')
            else:
                f.write(f"{attr}, ")
        f.write("\n")


@dataclass
class Section:
    rows: list[Row] = field(default_factory=list)

    def add(self, row: Row):
        self.rows.append(row)

    def write(self, f):
        f.write(self.mark_start + "\n")
        f.write(", ".join(self.cols) + "\n")
        for row in self.rows:
            row.write(f)
        f.write("\n")


@dataclass
class RunValues(Section):
    mark_start: str = "[Run Values]"
    cols: list[str] = field(default_factory=lambda: ["KeyName", "Value"])


@dataclass
class Settings(Section):
    mark_start: str = "[Settings]"
    cols: list[str] = field(default_factory=lambda: ["SettingName", "Value"])


@dataclass
class Samples(Section):
    mark_start: str = "[Samples]"
    cols: list[str] = field(
        default_factory=lambda: [
            "SampleName",
            "Index1",
            "Index2",
            "Lane",
            "Project",
            "ExternalID",
        ]
    )


@dataclass
class Manifest:
    runvalues: RunValues = RunValues()
    settings: Settings = Settings()
    samples: Samples = Samples()

    def write(self, file_path: str):
        with open(file_path, "w") as f:
            for section in [self.runvalues, self.settings, self.samples]:
                section.write(f)


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


def check_pair_distance(row, row_comp, dist_warning_threshold: int = 2):
    """Directionality-agnostic distance check between two index pairs."""

    def get_index_combos(row):
        return set(
            [
                row["Index1"] + row["Index2"],
                row["Index1"] + revcomp(row["Index2"]),
                revcomp(row["Index1"]) + row["Index2"],
                revcomp(row["Index1"]) + revcomp(row["Index2"]),
            ]
        )

    row_combos = get_index_combos(row)
    row_comp_combos = get_index_combos(row_comp)

    for row_combo in row_combos:
        for row_comp_combo in row_comp_combos:
            dist = distance(row_combo, row_comp_combo)

            if dist <= dist_warning_threshold:
                warning = "\n".join(
                    [
                        f"Edit distance between {row['SampleName']} and {row_comp['SampleName']} indices is {dist}.",
                        f" The warning threshold is {dist_warning_threshold}.",
                        "Supplied indexes:",
                        f" {row['SampleName']}: {row['Index1']}-{row['Index2']}",
                        f" {row_comp['SampleName']}: {row_comp['Index1']}-{row_comp['Index2']}",
                        "Comparison:",
                        f" {row['SampleName']}: {row_combo}",
                        f" {row_comp['SampleName']}: {row_comp_combo}",
                    ]
                )
                logging.warning(warning)
                if dist == 0:
                    raise AssertionError("Index collision detected.")


def check_pair_distance_new(row, row_comp, dist_warning_threshold: int = 2):
    """Directionality-agnostic distance check between two index pairs."""
    dists = []
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
                    dists.append(
                        (
                            distance(a1, b1) + distance(a2, b2),
                            f"{a1}-{a2} {b1}-{b2}",
                            f"{_a1}-{_a2} {_b1}-{_b2}",
                        )
                    )
    min_dist = min(dists, key=lambda x: x[0])

    if min_dist[0] <= dist_warning_threshold:
        print(f"{row['SampleName']} <--> {row_comp['SampleName']}")
        print(
            f"Given: {row['Index1']}-{row['Index2']} <--> {row_comp['Index1']}-{row_comp['Index2']}"
        )
        print(f"Distance: {min_dist[0]} when flipped to {min_dist[2]}")
        print_match(*min_dist[1].split())
        print()


def print_match(seq1, seq2):
    assert len(seq1) == len(seq2)

    m = ""
    for seq1_base, seq2_base in zip(seq1, seq2):
        if seq1_base == seq2_base:
            m += "|"
        else:
            m += "X"

    lines = "\n".join([seq1, m, seq2])
    print(lines)


def check_distances(rows: list[dict]) -> None:
    for i in range(len(rows)):
        row = rows[i]

        for row_comp in rows[i + 1 :]:
            check_pair_distance_new(row, row_comp)


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
