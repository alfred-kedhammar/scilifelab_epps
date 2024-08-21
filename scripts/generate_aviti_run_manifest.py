#!/usr/bin/env python

import logging
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, field
from datetime import datetime as dt

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims
from Levenshtein import distance

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
                index2 = None

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
        check_index_collision(lane_rows)
        all_rows.extend(lane_rows)

    df = pd.DataFrame(all_rows)

    samples_section = f"[Samples]\n{df.to_csv(index=None, header=True)}"

    return samples_section


def revcomp(seq: str) -> str:
    """Reverse-complement a DNA string."""
    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def check_index_collision(rows: list[dict], warning_dist: int = 3) -> None:
    """Directionality-agnostic index collision checker."""

    def idx_combinations(idx1: str, idx2: str | None) -> list[str]:
        """Given one or two indices, return all possible reverse-complement combinations."""
        if idx2 is None:
            return [idx1, revcomp(idx1)]
        else:
            return [
                idx1 + idx2,
                idx1 + revcomp(idx2),
                revcomp(idx1) + idx2,
                revcomp(idx1) + revcomp(idx2),
            ]

    for i in range(len(rows)):
        row = rows[i]
        idxs = idx_combinations(row["Index1"], row["Index2"])

        for row_comp in rows[i + 1 :]:
            idxs_comp = idx_combinations(row_comp["Index1"], row_comp["Index2"])

            for idx in idxs:
                for idx_comp in idxs_comp:
                    dist = distance(idx, idx_comp)
                    if dist <= warning_dist:
                        warning = "\n".join(
                            [
                                f"Edit distance between {row['SampleName']} and {row_comp['SampleName']} indices is {dist}.",
                                f" The warning threshold is {warning_dist}.",
                                "Supplied indexes:",
                                f" {row['SampleName']}: {row['Index1']}-{row['Index2']}",
                                f" {row_comp['SampleName']}: {row_comp['Index1']}-{row_comp['Index2']}",
                                "Comparison:",
                                f" {row['SampleName']}: {idx}",
                                f" {row_comp['SampleName']}: {idx_comp}",
                            ]
                        )
                        logging.warning(warning)
                        # TODO
                        print(warning)

            if any(idx in idxs_comp for idx in idxs):
                raise ValueError(
                    "Index collision detected between"
                    + f" {row['SampleName']} ({row['Index1']}-{row['Index2']}) and"
                    + f" {row_comp['SampleName']} ({row_comp['Index1']}-{row_comp['Index2']})."
                )


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
