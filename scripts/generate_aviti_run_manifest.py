#!/usr/bin/env python

import logging
from argparse import ArgumentParser
from dataclasses import dataclass, field
from datetime import datetime as dt

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.wrapper import epp_decorator

DESC = """Script to generate Anglerfish samplesheet for ONT runs.
"""

TIMESTAMP = dt.now().strftime("%y%m%d_%H%M%S")


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


class Section:
    def __init__(self) -> None:
        self.rows: list[Row] = []

    def add(self, row: Row):
        self.rows.append(row)

    def write(self, f):
        f.write(f"{self.mark_start}\n")
        for row in self.rows:
            row.write(f)
        f.write("\n")


class RunValues(Section):
    def __init__(self) -> None:
        super().__init__()
        self.mark_start: str = "[Run Values]"
        self.cols: list[str] = ["KeyName", "Value"]


class Settings(Section):
    def __init__(self) -> None:
        super().__init__()
        self.mark_start: str = "[Settings]"
        self.cols: list[str] = ["SettingName", "Value"]


class Samples(Section):
    def __init__(self) -> None:
        super().__init__()
        self.mark_start: str = "[Samples]"
        self.cols: list[str] = [
            "SampleName",
            "Index1",
            "Index2",
            "Lane",
            "Project",
            "ExternalID",
        ]


class Manifest:
    def __init__(self) -> None:
        self.sections: list[Section] = [RunValues(), Settings(), Samples()]

    def write(self, file_path: str):
        with open(file_path, "w") as f:
            for section in self.sections:
                section.write(f)


@epp_decorator(script_path=__file__, timestamp=TIMESTAMP)
def main(args):
    lims = Lims(BASEURI, USERNAME, PASSWORD)
    process = Process(lims, id=args.pid)

    logging.info("Starting to build run manifest.")

    pass


if __name__ == "__main__":
    # Parse args
    parser = ArgumentParser(description=DESC)
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
