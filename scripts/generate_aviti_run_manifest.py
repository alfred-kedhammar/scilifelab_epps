#!/usr/bin/env python

import logging
from argparse import ArgumentParser
from dataclasses import dataclass, field
from datetime import datetime as dt

import pandas as pd
from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

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


@epp_decorator(script_path=__file__, timestamp=TIMESTAMP)
def main(args):
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    process = Process(lims, id=args.pid)

    logging.info("Starting to build run manifest.")

    # Get the analytes placed into the flowcell
    arts_out = [op for op in process.all_outputs() if op.type == "Analyte"]

    # Iterate over pools
    rows = []
    for art_out in arts_out:
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

        assert len(labels.unique()) == len(
            labels
        ), "Detected non-unique reagent labels."

        # Iterate over samples

        for sample in samples:
            lims_label = sample2label[sample.name]

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

            rows.append(row)

    df = pd.DataFrame(rows)
    samples = f"[Samples]\n{df.to_csv(index=None, header=True)}"


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
