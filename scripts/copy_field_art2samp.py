#!/usr/bin/env python

DESC = """EPP script to copy user defined fields from analyte level to
submitted sample level in Clarity LIMS. Can be executed in the background
or triggered by a user pressing a "blue button".

This script can only be applied to processes where ANALYTES are modified
in the GUI. The script can output two different logs, where the
status_changelog contains notes with the technician, the date and changed
status for each copied status. The regular log file contains regular
execution information.

Error handling:
If the udf given is blank or not defined for any of the inputs,
the script will log this, and not perform any changes for that artifact.


Written by Johannes Alneberg, Science for Life Laboratory, Stockholm, Sweden
"""

import logging
import sys
import re
from argparse import ArgumentParser

from genologics.config import BASEURI, PASSWORD, USERNAME
from genologics.entities import Process
from genologics.lims import Lims

from scilifelab_epps.epp import CopyField, EppLogger


NGISAMPLE_PAT = re.compile("P[0-9]+_[0-9]+")

def main(lims, args, epp_logger):
    source_udfs = args.source_udf
    dest_udfs = args.dest_udf
    correct_artifacts = 0
    incorrect_artifacts = 0
    no_updated = 0
    p = Process(lims, id=args.pid)
    artifacts, inf = p.analytes()

    if args.status_changelog:
        epp_logger.prepend_old_log(args.status_changelog)

    if not dest_udfs:
        dest_udfs = source_udfs
    elif len(dest_udfs) != len(source_udfs):
        logging.error("source_udfs and dest_udfs lists of arguments are uneven.")
        sys.exit(-1)
    for i in range(len(source_udfs)):
        source_udf = source_udfs[i]
        dest_udf = dest_udfs[i]
        with open(args.status_changelog, "a") as changelog_f:
            for artifact in artifacts:
                if source_udf in artifact.udf:
                    correct_artifacts = correct_artifacts + 1
                    # Special case for copying values from Aggregate QC step
                    if args.aggregate:
                        # Only copy for NGI samples and skip controls
                        if NGISAMPLE_PAT.findall(artifact.samples[0].name):
                            copy_sesion = CopyField(
                                artifact, artifact.samples[0].artifact, source_udf, dest_udf
                            )
                            test = copy_sesion.copy_udf(changelog_f)
                        else:
                            test = ''
                    else:
                        copy_sesion = CopyField(
                            artifact, artifact.samples[0], source_udf, dest_udf
                        )
                        test = copy_sesion.copy_udf(changelog_f)
                    if test:
                        no_updated = no_updated + 1
                else:
                    incorrect_artifacts = incorrect_artifacts + 1
                    logging.warning(
                        (
                            "Found artifact for sample {} with {} "
                            "undefined/blank, exiting"
                        ).format(artifact.samples[0].name, source_udf)
                    )

    if incorrect_artifacts == 0:
        warning = "no artifacts"
    else:
        warning = f"WARNING: skipped {incorrect_artifacts} udfs(s)"
    d = {
        "ua": no_updated,
        "ca": correct_artifacts,
        "ia": incorrect_artifacts,
        "warning": warning,
    }

    abstract = (
        "Updated {ua} udf(s), out of {ca} in total, "
        "{warning} with incorrect udf info."
    ).format(**d)

    print(abstract, file=sys.stderr)  # stderr will be logged and printed in GUI


if __name__ == "__main__":
    parser = ArgumentParser(description=DESC)
    parser.add_argument("--pid", help="Lims id for current Process")
    parser.add_argument(
        "--log",
        help=(
            "File name for standard log file, " " for runtime information and problems."
        ),
    )
    parser.add_argument(
        "-s",
        "--source_udf",
        type=str,
        default=None,
        nargs="*",
        help=(
            "Name(s) of the source user defined field(s) "
            "that will be copied. One or many udf-names "
            "can be given."
        ),
    )
    parser.add_argument(
        "-d",
        "--dest_udf",
        type=str,
        default=None,
        nargs="*",
        help=(
            "Name(s) of the destination user defined "
            "field(s) that will be written to. This "
            "argument is optional, if left empty "
            "the source_udf argument is used instead. "
            "Zero or many udf-names can be given. If "
            "more than zero, the numer of udfs needs "
            "to be the same as number of source_udfs"
        ),
    )
    parser.add_argument(
        "-c",
        "--status_changelog",
        help=(
            "File name for status changelog file, "
            " for concise information on who, what and "
            " when for status change events. "
            "Prepends the old changelog file by default."
        ),
    )
    parser.add_argument(
        "--aggregate",
        dest="aggregate",
        action="store_true",
        help=(
            "Used for Aggregate QC step specially."
        ),
    )
    args = parser.parse_args()

    lims = Lims(BASEURI, USERNAME, PASSWORD)
    lims.check_version()

    with EppLogger(log_file=args.log, lims=lims, prepend=True) as epp_logger:
        main(lims, args, epp_logger)
